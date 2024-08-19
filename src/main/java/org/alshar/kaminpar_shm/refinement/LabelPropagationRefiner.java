package org.alshar.kaminpar_shm.refinement;
import com.sun.jdi.Value;
import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.Math.Random_shm;
import org.alshar.common.ParallelUtils.ParallelFor;
import org.alshar.common.context.*;
import org.alshar.common.datastructures.*;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.kaminpar_shm.coarsening.LPClustering;
import org.alshar.kaminpar_shm.coarsening.LabelPropagationConfig;

import java.security.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

public class LabelPropagationRefiner extends Refiner {
    private final LabelPropagationRefinerImpl impl;

    public LabelPropagationRefiner(Context ctx) {
        impl = new LabelPropagationRefinerImpl(ctx);
    }

    @Override
    public void initialize(PartitionedGraph pGraph) {
        impl.initialize(pGraph);
    }

    @Override
    public boolean refine(PartitionedGraph pGraph, PartitionContext pCtx) {
        return impl.refine(pGraph, pCtx);
    }

    private static class LabelPropagationRefinerImpl {
        private final RefinementContext rCtx;
        private Graph graph;
        private PartitionedGraph pGraph;
        private PartitionContext pCtx;
        // Equivalent to the _chunks and _buckets vectors in C++
        private List<LPClustering.Chunk> chunks;
        private List<LPClustering.Bucket> buckets;
        private int _initialNumClusters;
        private int numNodes;
        private int currentNumClusters;
        private NodeWeight maxClusterWeight;
        private AtomicIntegerArray clusters;
        private AtomicLongArray clusterWeights;
        private AtomicIntegerArray active;
        private long expectedTotalGain;
        private int maxDegree = Integer.MAX_VALUE;
        private int MaxNumNeighbors = Integer.MAX_VALUE;
        private AtomicIntegerArray favoredClusters;
        private int desiredNumClusters;
        private int _numClusters;
        private final Random_shm.RandomPermutations<Integer> randomPermutations;
        private ThreadLocal<RatingMap<Key, Value>> ratingMapEts;

        public LabelPropagationRefinerImpl(Context ctx) {
            rCtx = ctx.refinement;
            ratingMapEts = ThreadLocal.withInitial(() -> new RatingMap<>(ctx.partition.k.value));
            allocate(ctx.partition.n.value, ctx.partition.n.value, ctx.partition.k.value);
            setMaxDegree(rCtx.lp.largeDegreeThreshold);
            setMaxNumNeighbors(rCtx.lp.maxNumNeighbors);
            chunks = new ArrayList<>();
            buckets = new ArrayList<>();
            this.randomPermutations = new Random_shm.RandomPermutations<>(Random_shm.getInstance(), LabelPropagationConfig.kPermutationSize, LabelPropagationConfig.kNumberOfNodePermutations);

        }

        public void setMaxNumNeighbors(int maxNumNeighbors) {
            MaxNumNeighbors = maxNumNeighbors;
        }

        public void setMaxDegree(int maxDegree) {
            this.maxDegree = maxDegree;
        }

        private void allocate(int numNodes, int numActiveNodes, int numClusters) {
            // Check if the current number of nodes is less than the provided numNodes
            if (this.numNodes < numNodes) {
                if (LabelPropagationConfig.kUseLocalActiveSetStrategy) {
                    active = new AtomicIntegerArray(numNodes);  // Resize the active array
                }
                this.numNodes = numNodes;  // Update the number of nodes
            }

            // Check if the current number of active nodes is less than the provided numActiveNodes
            if (this.currentNumClusters < numActiveNodes) {
                if (LabelPropagationConfig.kUseActiveSetStrategy) {
                    active = new AtomicIntegerArray(numActiveNodes);  // Resize the active array
                }
                if (LabelPropagationConfig.kUseTwoHopClustering) {
                    favoredClusters = new AtomicIntegerArray(numActiveNodes);  // Resize the favored clusters array
                }
                this.currentNumClusters = numActiveNodes;  // Update the number of active nodes
            }

            // Check if the current number of clusters is less than the provided numClusters
            if (this._numClusters < numClusters) {
                // Adjust the rating map size for each thread-local rating map
                ratingMapEts.get().setMaxSize(numClusters);

                // Update the number of clusters
                this._numClusters = numClusters;
            }
        }


        public void initialize(PartitionedGraph pGraph) {
            this.graph = pGraph.getGraph();
        }

        public boolean refine(PartitionedGraph pGraph, PartitionContext pCtx) {
            this.pGraph = pGraph;
            this.pCtx = pCtx;
            assert this.graph == pGraph.getGraph();
            assert pGraph.k().value <= pCtx.k.value;

            initialize(graph, new NodeID(pCtx.k.value));

            int maxIterations = rCtx.lp.numIterations == 0 ? Integer.MAX_VALUE : rCtx.lp.numIterations;
            for (int iteration = 0; iteration < maxIterations; iteration++) {
                if (performIteration() == 0) {
                    return false;
                }
            }
            return true;
        }
        private int performIteration() {
            return performIteration(0, Integer.MAX_VALUE);
        }
        private int performIteration(int from, int to) {
            if (from != 0 || to != Integer.MAX_VALUE) {
                chunks.clear();
            }

            if (chunks.isEmpty()) {
                initChunks(from, to);
            }

            shuffleChunks();

            AtomicIntegerArray movedNodesCount = new AtomicIntegerArray(numNodes);
            AtomicIntegerArray removedClustersCount = new AtomicIntegerArray(1); // To track the number of removed clusters
            AtomicInteger nextChunk = new AtomicInteger(0);

            ParallelFor.parallelFor(0, chunks.size(), 1, (start, end) -> {
                for (int chunkIdx = start; chunkIdx < end; chunkIdx++) {
                    if (shouldStop()) {
                        return;
                    }

                    LPClustering.Chunk chunk = chunks.get(chunkIdx);
                    List<Integer> permutation = createPermutation(chunk);
                    int numSubChunks = (int) Math.ceil((chunk.end - chunk.start) / (double) LabelPropagationConfig.kPermutationSize);
                    List<Integer> subChunkPermutation = createSubChunkPermutation(numSubChunks);

                    LocalRatingMap localRatingMap = new LocalRatingMap(_numClusters);
                    Random_shm localRand = new Random_shm(); // Replace with appropriate Random instance

                    for (int subChunk = 0; subChunk < numSubChunks; subChunk++) {
                        for (int i = 0; i < LabelPropagationConfig.kPermutationSize; i++) {
                            int u = chunk.start + LabelPropagationConfig.kPermutationSize * subChunkPermutation.get(subChunk) + permutation.get(i % LabelPropagationConfig.kPermutationSize);
                            if (u < chunk.end && graph.degree(new NodeID(u)).value < maxDegree && (LabelPropagationConfig.kUseActiveSetStrategy && active.get(u) != 0)) {
                                boolean[] result = handleNode(u, localRand, localRatingMap); // Updated call to handleNode

                                if (result[0]) {
                                    movedNodesCount.incrementAndGet(u);
                                }
                                if (result[1]) {
                                    removedClustersCount.incrementAndGet(0);
                                }
                            }
                        }
                    }
                }
            });

            currentNumClusters -= removedClustersCount.get(0);

            // Sum the total moved nodes count from all threads
            int totalMovedNodes = 0;
            for (int i = 0; i < numNodes; i++) {
                totalMovedNodes += movedNodesCount.get(i);
            }

            return totalMovedNodes;
        }

        private void initialize(Graph graph, NodeID num_clusters) {
            // Ensure that the graph and node counts are initialized properly
            this.graph = graph;
            this.numNodes = graph.n().value;
            this.currentNumClusters = num_clusters.value;  // Set the current number of clusters to the number of nodes
            this._initialNumClusters = num_clusters.value; // Initialize the initial number of clusters

            // Clear any existing state
            clusters = new AtomicIntegerArray(graph.n().value);  // Reset clusters
            clusterWeights = new AtomicLongArray(graph.n().value);  // Reset cluster weights

            // Reset any internal state or counters
            resetState();

            // Clear chunks and buckets, similar to what is done in the C++ code
            chunks.clear();
            buckets.clear();
        }


        private BlockID initialCluster(NodeID u) {
            return pGraph.block(u);
        }

        private NodeWeight initialClusterWeight(NodeID cluster) {
            return graph.nodeWeight(cluster);
        }

        private BlockWeight clusterWeight(BlockID b) {
            return pGraph.blockWeight(b);
        }

        private boolean moveClusterWeight(BlockID oldBlock, BlockID newBlock, BlockWeight delta, BlockWeight maxWeight) {
            return pGraph.moveBlockWeight(oldBlock, newBlock, delta, maxWeight);
        }

        private void initChunks(int from, int to) {
            chunks.clear();
            buckets.clear();

            // Adjust 'to' to ensure it's within the graph's range
            to = Math.min(to, graph.n().value);

            // Calculate maximum bucket and chunk sizes
            int maxBucket = Math.min((int) Math.floor(Math.log(maxDegree) / Math.log(2)), graph.numberOfBuckets());
            long maxChunkSize = (long) Math.max(1024, Math.sqrt(graph.m().value)); // Config::kMinChunkSize = 1024
            int maxNodeChunkSize = (int) Math.max(1024, Math.sqrt(graph.n().value)); // Config::kMinChunkSize = 1024

            int position = 0;
            for (int bucket = 0; bucket < maxBucket; ++bucket) {
                if (position + graph.bucketSize(bucket).value < from || graph.bucketSize(bucket).value == 0) {
                    position += graph.bucketSize(bucket).value;
                    continue;
                }
                if (position >= to) {
                    break;
                }

                int remainingBucketSize = graph.bucketSize(bucket).value;
                if (from > graph.firstNodeInBucket(bucket).value) {
                    remainingBucketSize -= from - graph.firstNodeInBucket(bucket).value;
                }
                int bucketSize = Math.min(remainingBucketSize, to - position);

                AtomicInteger offset = new AtomicInteger(0);
                List<LPClustering.Chunk> localChunks = new ArrayList<>();
                int bucketStart = Math.max(graph.firstNodeInBucket(bucket).value, from);

                while (offset.get() < bucketSize) {
                    int begin = offset.getAndAdd(maxNodeChunkSize);
                    if (begin >= bucketSize) {
                        break;
                    }
                    int end = Math.min(begin + maxNodeChunkSize, bucketSize);

                    long currentChunkSize = 0;
                    int chunkStart = bucketStart + begin;

                    for (int i = begin; i < end; ++i) {
                        int u = bucketStart + i;
                        currentChunkSize += graph.degree(new NodeID(u)).value;
                        if (currentChunkSize >= maxChunkSize) {
                            localChunks.add(new LPClustering.Chunk(chunkStart, u + 1));
                            chunkStart = u + 1;
                            currentChunkSize = 0;
                        }
                    }

                    if (currentChunkSize > 0) {
                        localChunks.add(new LPClustering.Chunk(chunkStart, bucketStart + end));
                    }
                }

                int chunksStart = chunks.size();
                chunks.addAll(localChunks);
                buckets.add(new LPClustering.Bucket(chunksStart, chunks.size()));

                position += graph.bucketSize(bucket).value;
            }

            // Ensure that all nodes in the range [from, to) are covered
            validateChunks(from, to);
        }
        private void shuffleChunks() {
            ParallelFor.parallelFor(0, buckets.size(), 1, (start, end) -> {
                for (int i = start; i < end; i++) {
                    LPClustering.Bucket bucket = buckets.get(i);
                    Random random = new Random(); // You can use a better randomization method if needed
                    Collections.shuffle(chunks.subList(bucket.start, bucket.end), random);
                }
            });
        }
        private List<Integer> createPermutation(LPClustering.Chunk chunk) {
            return randomPermutations.get();
        }
        private void validateChunks(int from, int to) {
            boolean[] hit = new boolean[to - from];
            for (LPClustering.Chunk chunk : chunks) {
                for (int u = chunk.start; u < chunk.end; ++u) {
                    assert from <= u && u < to;
                    assert !hit[u - from];
                    hit[u - from] = true;
                }
            }

            for (int u = 0; u < to - from; ++u) {
                assert graph.degree(new NodeID(u)).value == 0 || hit[u];
            }
        }

        private BlockID cluster(NodeID u) {
            return pGraph.block(u);
        }

        private void moveNode(NodeID u, BlockID block) {
            pGraph.setBlock(u, block);
        }

        private BlockID numClusters() {
            return pGraph.k();
        }

        private BlockWeight maxClusterWeight(BlockID block) {
            return pCtx.blockWeights.max(block.value);
        }
        private long maxClusterWeight() {
            return maxClusterWeight.value;
        }

        private boolean acceptCluster(ClusterSelectionState state) {

            // Calculate the current maximum cluster weight
            long currentMaxWeight = maxClusterWeight(new BlockID(state.currentCluster)).value;

            // Calculate the overloads for the best and current clusters
            long bestOverload = state.bestClusterWeight - maxClusterWeight(new BlockID(state.bestCluster)).value;
            long currentOverload = state.currentClusterWeight - currentMaxWeight;
            long initialOverload = state.initialClusterWeight - maxClusterWeight(new BlockID(state.initialCluster)).value;

            // Determine whether to accept the current cluster
            return (state.currentGain > state.bestGain ||
                    (state.currentGain == state.bestGain &&
                            (currentOverload < bestOverload ||
                                    (currentOverload == bestOverload && state.localRand.randomBool())))) &&
                    (state.currentClusterWeight + state.uWeight.value < currentMaxWeight ||
                            currentOverload < initialOverload || state.currentCluster == state.initialCluster);
        }

        private void resetState() {
            // Parallel initialization using Java parallel streams or ForkJoinPool
            ParallelFor.parallelFor(0, numNodes, 1, (start, end) -> {
                for (int u = start; u < end; u++) {
                    if (LabelPropagationConfig.kUseActiveSetStrategy || LabelPropagationConfig.kUseLocalActiveSetStrategy) {
                        active.set(u, 1);
                    }

                    // Initialize cluster and optionally favored clusters
                    int initialCluster = initialCluster(new NodeID(u)).value;
                    clusters.set(u, initialCluster);

                    // If TwoHopClustering is enabled, set the favored cluster
                    if (LabelPropagationConfig.kUseTwoHopClustering) {
                        favoredClusters.set(u, initialCluster);
                    }

                }
            });

            // Initialize cluster weights
            ParallelFor.parallelFor(0, currentNumClusters, 1, (start, end) -> {
                for (int cluster = start; cluster < end; cluster++) {
                    long initialClusterWeight = initialClusterWeight(new NodeID(cluster)).value;
                    clusterWeights.set(cluster, initialClusterWeight);
                }
            });

            // Reset any additional statistics if needed
            expectedTotalGain = 0;
            currentNumClusters = _initialNumClusters;
        }
        private List<Integer> createSubChunkPermutation(int numSubChunks) {
            List<Integer> subChunkPermutation = new ArrayList<>(numSubChunks);
            for (int i = 0; i < numSubChunks; i++) {
                subChunkPermutation.add(i);
            }
            Random_shm.getInstance().shuffle(subChunkPermutation);
            return subChunkPermutation;
        }
        private boolean shouldStop() {
            // Assuming the equivalent of Config::kTrackClusterCount is always true
            if (LabelPropagationConfig.kTrackClusterCount) {
                return currentNumClusters <= desiredNumClusters;
            }
            return false;
        }
        private boolean[] handleNode(int u, Random_shm localRand, LocalRatingMap localRatingMap) {
            // Check if the node should be skipped
            if (derivedSkipNode(u)) {
                return new boolean[]{false, false};
            }

            // Get the node's weight and current cluster
            NodeWeight uWeight = graph.nodeWeight(new NodeID(u));
            int uCluster = derivedCluster(u);

            // Find the best cluster for the node
            Pair<Integer, Long> bestClusterResult = findBestCluster(u, uWeight, uCluster, localRand, localRatingMap);
            int newCluster = bestClusterResult.getKey();
            long newGain = bestClusterResult.getValue();

            // If the best cluster is different from the current cluster
            if (derivedCluster(u) != newCluster) {
                // Attempt to move the cluster weight
                if (derivedMoveClusterWeight(uCluster, newCluster, uWeight.value, derivedMaxClusterWeight(newCluster))) {
                    // Move the node to the new cluster
                    derivedMoveNode(u, newCluster);
                    activateNeighbors(u);

                    // If stats tracking is enabled, update the expected total gain
                    if (LabelPropagationConfig.kStatistics) {
                        expectedTotalGain += newGain;
                    }

                    // Check if the original cluster is now empty
                    boolean decrementClusterCount = LabelPropagationConfig.kTrackClusterCount && derivedClusterWeight(uCluster) == 0;
                    return new boolean[]{true, decrementClusterCount};
                }
            }

            // Return false if no movement occurred
            return new boolean[]{false, false};
        }
        private Pair<Integer, Long> findBestCluster(int u, NodeWeight uWeight, int uCluster, Random_shm localRand, LocalRatingMap localRatingMap) {
            return localRatingMap.execute(
                    Math.min(graph.degree(new NodeID(u)).value, _numClusters),
                    mapType -> {
                        long initialClusterWeight = derivedClusterWeight(uCluster);
                        ClusterSelectionState state = new ClusterSelectionState(localRand, u, uWeight, uCluster, initialClusterWeight);

                        boolean isInterfaceNode = false;

                        for (int e = graph.firstEdge(new NodeID(u)).value; e < graph.firstEdge(new NodeID(u)).value + graph.degree(new NodeID(u)).value; e++) {
                            int v = graph.edgeTarget(new EdgeID(e)).value;
                            if (derivedAcceptNeighbor(u, v)) {
                                int vCluster = derivedCluster(v);
                                long rating = graph.edgeWeight(new EdgeID(e)).value;


                                long currentRating = localRatingMap.get(mapType, vCluster);
                                localRatingMap.set(mapType, vCluster, currentRating + rating);

                                isInterfaceNode |= v >= _numClusters;
                            }
                        }

                        if (LabelPropagationConfig.kUseLocalActiveSetStrategy && !isInterfaceNode) {
                            active.set(u, 0);
                        } else if (LabelPropagationConfig.kUseActiveSetStrategy) {
                            active.set(u, 0);
                        }

                        int favoredCluster = uCluster;
                        boolean storeFavoredCluster = LabelPropagationConfig.kUseTwoHopClustering && uWeight.value == initialClusterWeight && initialClusterWeight <= derivedMaxClusterWeight(uCluster) / 2;

                        long gainDelta = LabelPropagationConfig.kUseActualGain ? localRatingMap.get(mapType, uCluster) : 0;

                        for (int cluster : localRatingMap.getEntries(mapType)) {
                            state.currentCluster = cluster;
                            state.currentGain = localRatingMap.get(mapType, cluster) - gainDelta;
                            state.currentClusterWeight = derivedClusterWeight(state.currentCluster);

                            if (storeFavoredCluster && state.currentGain > state.bestGain) {
                                favoredCluster = state.currentCluster;
                            }

                            if (derivedAcceptCluster(state)) {
                                state.bestCluster = state.currentCluster;
                                state.bestClusterWeight = state.currentClusterWeight;
                                state.bestGain = state.currentGain;
                            }
                        }

                        if (storeFavoredCluster && state.bestCluster == state.initialCluster) {
                            favoredClusters.set(u, favoredCluster);
                        }

                        long actualGain = state.bestGain - localRatingMap.get(mapType, state.initialCluster);
                        localRatingMap.clear(mapType);
                        return new Pair<>(state.bestCluster, actualGain);
                    }
            );
        }
        private boolean derivedSkipNode(int node) {
            return skipNode(node);
        }
        private long derivedClusterWeight(int b) {
            return pGraph.blockWeight(new BlockID(b)).value;
        }
        private int derivedCluster(int u) {
            return pGraph.block(new NodeID(u)).value;
        }
        private boolean skipNode(int node) {
            return false;  // Default implementation: do not skip any node
        }
        private boolean derivedAcceptNeighbor(int u, int v) {
            return true; // Implement according to your logic
        }
        // Nested class for ClusterSelectionState
        private boolean derivedAcceptCluster(ClusterSelectionState state) {

            return acceptCluster(state);
        }
        private long derivedMaxClusterWeight(int cluster) {
            return pCtx.blockWeights.max(cluster).value;
        }
        private boolean derivedMoveClusterWeight(int oldBlock, int newBlock, long delta, long maxWeight) {
            return pGraph.moveBlockWeight(new BlockID(oldBlock),new BlockID(newBlock), new BlockWeight(delta), new BlockWeight(maxWeight));
        }
        private void derivedMoveNode(int u, int block) {
            pGraph.setBlock(new NodeID(u), new BlockID(block));
        }
        private void activateNeighbors(int u) {
            for (NodeID v : graph.adjacentNodes(new NodeID(u))) {
                if (derivedActivateNeighbor(v.value)) {
                    if (LabelPropagationConfig.kUseActiveSetStrategy || LabelPropagationConfig.kUseLocalActiveSetStrategy) {
                        active.set(v.value, 1);
                    }
                }
            }
        }
        private boolean derivedActivateNeighbor(int u) {
            return activateNeighbor(u);
        }
        private boolean activateNeighbor(int node) {
            return true;
        }
        public static class ClusterSelectionState {
            Random_shm localRand;
            int u;
            public NodeWeight uWeight;
            public int initialCluster;
            long initialClusterWeight;
            public int bestCluster;
            public long bestGain;
            public long bestClusterWeight;
            public int currentCluster;
            public long currentGain;
            public long currentClusterWeight;

            public ClusterSelectionState(Random_shm localRand, int u, NodeWeight uWeight, int initialCluster, long initialClusterWeight) {
                this.localRand = localRand;
                this.u = u;
                this.uWeight = uWeight;
                this.initialCluster = initialCluster;
                this.initialClusterWeight = initialClusterWeight;
                this.bestCluster = initialCluster;
                this.bestGain = 0;
                this.bestClusterWeight = initialClusterWeight;
                this.currentCluster = 0;
                this.currentGain = 0;
                this.currentClusterWeight = 0;
            }
        }
    }
}