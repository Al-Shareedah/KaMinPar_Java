package org.alshar.kaminpar_shm.coarsening;
import org.alshar.common.Math.Random_shm;
import org.alshar.common.ParallelUtils.ParallelFor;
import org.alshar.common.context.*;
import org.alshar.Graph;
import org.alshar.common.datastructures.*;
import org.alshar.common.timer.Timer_km;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

import static org.alshar.common.ParallelUtils.ParallelFor.parallelFor;

public class LPClustering extends Clusterer {
    private final LPClusteringImpl core;

    public LPClustering(NodeID maxN, CoarseningContext cCtx) {
        this.core = new LPClusteringImpl(maxN, cCtx);
    }

    @Override
    public void setMaxClusterWeight(NodeWeight maxClusterWeight) {
        core.setMaxClusterWeight(maxClusterWeight);
    }

    @Override
    public void setDesiredClusterCount(NodeID count) {
        core.setDesiredNumClusters(count);
    }

    @Override
    public int[] computeClustering(Graph graph) {
        return core.computeClustering(graph);
    }

    private static class LPClusteringImpl extends Clusterer {
        private final CoarseningContext cCtx;
        private NodeWeight maxClusterWeight;
        private Graph graph;
        private int numNodes;
        private int currentNumClusters;
        private int desiredNumClusters;
        private int maxDegree = Integer.MAX_VALUE;
        private AtomicIntegerArray clusters;
        private AtomicLongArray clusterWeights;
        private AtomicIntegerArray active;
        private AtomicIntegerArray favoredClusters;
        private int _initialNumClusters;
        private long expectedTotalGain;
        private int _numClusters;


        private final List<Chunk> chunks = new ArrayList<>();
        private final List<Bucket> buckets = new ArrayList<>();

        private final Random_shm.RandomPermutations<Integer> randomPermutations;
        public LPClusteringImpl(NodeID maxN, CoarseningContext cCtx) {
            this.cCtx = cCtx;
            allocate(maxN.value, maxN.value);
            this.randomPermutations = new Random_shm.RandomPermutations<>(Random_shm.getInstance(), LabelPropagationConfig.kPermutationSize, LabelPropagationConfig.kNumberOfNodePermutations);
        }

        @Override
        public void setMaxClusterWeight(NodeWeight maxClusterWeight) {
            this.maxClusterWeight = maxClusterWeight;
        }

        public void setDesiredNumClusters(NodeID count) {
            this.desiredNumClusters = count.value;
        }

        @Override
        public int[] computeClustering(Graph graph) {
            this.graph = graph;
            this.numNodes = graph.n().value;
            initialize(graph, graph.n());

            for (int iteration = 0; iteration < cCtx.lp.numIterations; ++iteration) {
                // Start a scoped timer for each iteration
                try (var iterationTimer = Timer_km.global().startScopedTimer("Iteration", String.valueOf(iteration))) {
                    if (performIteration() == 0) {
                        break;
                    }
                }
            }

            // Add timers for clustering isolated and two-hop nodes
            try (var isolatedNodesTimer = Timer_km.global().startScopedTimer("Cluster Isolated Nodes")) {
                clusterIsolatedNodes();
            }

            try (var twoHopNodesTimer = Timer_km.global().startScopedTimer("Cluster Two Hop Nodes")) {
                clusterTwoHopNodes();
            }

            return clusters();
        }


        private void allocate(int maxN, int maxClusters) {
            clusters = new AtomicIntegerArray(maxN);
            clusterWeights = new AtomicLongArray(maxClusters);
            active = new AtomicIntegerArray(maxN);
            favoredClusters = new AtomicIntegerArray(maxN);
            _numClusters = maxClusters;
        }

        private void initialize(Graph graph, NodeID numNodes) {
            this.graph = graph;
            this.numNodes = numNodes.value;
            this.currentNumClusters = numNodes.value;
            this._initialNumClusters = numNodes.value;

            // Clear any existing state
            clusters = new AtomicIntegerArray(numNodes.value);
            clusterWeights = new AtomicLongArray(numNodes.value);
            active = new AtomicIntegerArray(numNodes.value);

            // Call reset state
            resetState();
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

            LongAdder movedNodesCount = new LongAdder(); // To track the number of moved nodes
            LongAdder removedClustersCount = new LongAdder(); // To track the number of removed clusters
            AtomicInteger nextChunk = new AtomicInteger(0);

            ParallelFor.parallelFor(0, chunks.size(), 1, (start, end) -> {
                for (int chunkIdx = start; chunkIdx < end; chunkIdx++) {
                    if (shouldStop()) {
                        return;
                    }

                    Chunk chunk = chunks.get(chunkIdx);
                    List<Integer> permutation = createPermutation(chunk);
                    int numSubChunks = (int) Math.ceil((chunk.end - chunk.start) / (double) LabelPropagationConfig.kPermutationSize);
                    List<Integer> subChunkPermutation = createSubChunkPermutation(numSubChunks);

                    LocalRatingMap localRatingMap = new LocalRatingMap(_numClusters);
                    Random_shm localRand = new Random_shm(); // Replace with appropriate Random instance

                    for (int subChunk = 0; subChunk < numSubChunks; subChunk++) {
                        for (int i = 0; i < LabelPropagationConfig.kPermutationSize; i++) {
                            int u = chunk.start + LabelPropagationConfig.kPermutationSize * subChunkPermutation.get(subChunk) + permutation.get(i % LabelPropagationConfig.kPermutationSize);
                            if (u < chunk.end && graph.degree(new NodeID(u)).value < maxDegree && (!LabelPropagationConfig.kUseActiveSetStrategy || active.get(u) != 0)) {
                                boolean[] result = handleNode(u, localRand, localRatingMap); // Updated call to handleNode

                                if (result[0]) {
                                    movedNodesCount.increment();
                                }
                                if (result[1]) {
                                    removedClustersCount.increment();
                                }
                            }
                        }
                    }
                }
            });

            currentNumClusters -= removedClustersCount.sum();

            return movedNodesCount.intValue();
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
        private List<Integer> createPermutation(Chunk chunk) {
            return randomPermutations.get();
        }
        private void shuffleChunks() {
            ParallelFor.parallelFor(0, buckets.size(), 1, (start, end) -> {
                for (int i = start; i < end; i++) {
                    Bucket bucket = buckets.get(i);
                    Random random = new Random(); // You can use a better randomization method if needed
                    Collections.shuffle(chunks.subList(bucket.start, bucket.end), random);
                }
            });
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
                List<Chunk> localChunks = new ArrayList<>();
                int bucketStart = Math.max(graph.firstNodeInBucket(bucket).value, from);

                // Parallelize the chunk creation within each bucket
                ParallelFor.parallelFor(0, bucketSize, 1, (start, end) -> {
                    while (offset.get() < bucketSize) {
                        int begin = offset.getAndAdd(maxNodeChunkSize);
                        if (begin >= bucketSize) {
                            break;
                        }
                        int endLocal = Math.min(begin + maxNodeChunkSize, bucketSize);

                        long currentChunkSize = 0;
                        int chunkStart = bucketStart + begin;

                        for (int i = begin; i < endLocal; ++i) {
                            int u = bucketStart + i;
                            currentChunkSize += graph.degree(new NodeID(u)).value;
                            if (currentChunkSize >= maxChunkSize) {
                                localChunks.add(new Chunk(chunkStart, u + 1));
                                chunkStart = u + 1;
                                currentChunkSize = 0;
                            }
                        }

                        if (currentChunkSize > 0) {
                            localChunks.add(new Chunk(chunkStart, bucketStart + endLocal));
                        }
                    }
                });

                int chunksStart = chunks.size();
                chunks.addAll(localChunks);
                buckets.add(new Bucket(chunksStart, chunks.size()));

                position += graph.bucketSize(bucket).value;
            }

            // Ensure that all nodes in the range [from, to) are covered
            //validateChunks(from, to);
        }

        private List<Integer> createSubChunkPermutation(int numSubChunks) {
            List<Integer> subChunkPermutation = new ArrayList<>(numSubChunks);
            for (int i = 0; i < numSubChunks; i++) {
                subChunkPermutation.add(i);
            }
            Random_shm.getInstance().shuffle(subChunkPermutation);
            return subChunkPermutation;
        }

        private void validateChunks(int from, int to) {
            boolean[] hit = new boolean[to - from];
            for (Chunk chunk : chunks) {
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



        private void clusterIsolatedNodes() {
           //This method is intentionally left blank
        }

        private void clusterTwoHopNodes() {
            ParallelFor.parallelFor(0, numNodes, 1, (start, end) -> {
                for (int u = start; u < end; u++) {
                    if (isConsideredForTwoHopClustering(u)) {
                        handleTwoHopNode(u);
                    }
                }
            });
        }


        private boolean isConsideredForTwoHopClustering(int u) {
            if (graph.degree(new NodeID(u)).value == 0) {
                return false; // Not considered: isolated node
            } else if (clusters.get(u) != u) {
                return false; // Not considered: joined another cluster
            } else {
                long currentWeight = clusterWeights.get(u);
                if (currentWeight > maxClusterWeight.value / 2 || currentWeight != graph.nodeWeight(new NodeID(u)).value) {
                    return false; // Not considered: not a singleton cluster; or its weight is too heavy
                }
            }
            return true;
        }
        private void handleTwoHopNode(int u) {
            int favoredCluster = clusters.get(u); // Initially set the favored cluster to the current cluster

            for (int e = graph.firstEdge(new NodeID(u)).value; e < graph.firstEdge(new NodeID(u)).value + graph.degree(new NodeID(u)).value; e++) {
                int v = graph.edgeTarget(new EdgeID(e)).value;
                int vCluster = clusters.get(v);
                long gain = graph.edgeWeight(new EdgeID(e)).value;

                if (vCluster != favoredCluster && gain > 0 && clusterWeights.get(vCluster) + graph.nodeWeight(new NodeID(u)).value <= maxClusterWeight.value) {
                    clusters.set(u, vCluster);
                    clusterWeights.addAndGet(vCluster, graph.nodeWeight(new NodeID(u)).value);
                    clusterWeights.addAndGet(favoredCluster, -graph.nodeWeight(new NodeID(u)).value);
                    break;
                }
            }
        }
        private int[] clusters() {
            int[] finalClusters = new int[numNodes];
            for (int i = 0; i < numNodes; i++) {
                finalClusters[i] = clusters.get(i);
            }
            return finalClusters;
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

                    resetNodeState(new NodeID(u));
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
        private NodeID initialCluster(NodeID u) {
            return u; // Initial cluster is the node itself
        }

        private NodeWeight initialClusterWeight(NodeID cluster) {
            return graph.nodeWeight(cluster);
        }


        private void resetNodeState(NodeID node) {
            // You can add additional logic here if needed
        }
        private boolean shouldStop() {
            // Assuming the equivalent of Config::kTrackClusterCount is always true
            if (LabelPropagationConfig.kTrackClusterCount) {
                return currentNumClusters <= desiredNumClusters;
            }
            return false;
        }

        private boolean derivedSkipNode(int node) {
            return skipNode(node);
        }

        private boolean skipNode(int node) {
            return false;  // Default implementation: do not skip any node
        }

        private int derivedCluster(int u) {
            return clusters.get(u);
        }

        private long derivedMaxClusterWeight(int cluster) {
            return maxClusterWeight();
        }

        private long maxClusterWeight() {
            return maxClusterWeight.value;
        }

        private boolean derivedMoveClusterWeight(int oldCluster, int newCluster, long delta, long maxWeight) {
            return moveClusterWeight(oldCluster, newCluster, delta, maxWeight);
        }

        private boolean moveClusterWeight(int oldCluster, int newCluster, long delta, long maxWeight) {
            if (clusterWeights.get(newCluster) + delta <= maxWeight) {
                clusterWeights.addAndGet(newCluster, delta);
                clusterWeights.addAndGet(oldCluster, -delta);
                return true;
            }
            return false;
        }

        private void derivedMoveNode(int u, int cluster) {
            clusters.set(u, cluster);
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

        private long derivedClusterWeight(int cluster) {
            return clusterWeights.get(cluster);
        }

        private boolean derivedAcceptNeighbor(int u, int v) {
            return true; // Implement according to your logic
        }

        private boolean derivedAcceptCluster(ClusterSelectionState state) {
            return state.currentClusterWeight + state.uWeight.value <= maxClusterWeight() && state.currentGain > state.bestGain;
        }
    }
    public static class Chunk {
        public int start;
        public int end;

        public Chunk(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    public static class Bucket {
        public int start;
        public int end;

        public Bucket(int start, int end) {
            this.start = start;
            this.end = end;
        }
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
