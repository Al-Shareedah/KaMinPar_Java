package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.Graph;
import org.alshar.common.datastructures.*;
import org.alshar.common.GraphUtils.Edge;
import org.alshar.common.Seq;
import org.alshar.common.context.*;
import org.alshar.kaminpar_shm.PartitionedGraph;

import org.alshar.common.Math.Random_shm;
import org.alshar.common.Math.Random_shm.RandomPermutations;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class InitialCoarsener {
    private static final int CHUNK_SIZE = 256;
    private static final int NUMBER_OF_NODE_PERMUTATIONS = 16;

    private static class Cluster {
        boolean locked;
        NodeWeight weight;
        NodeID leader;
    }

    public static class MemoryContext {
        List<Cluster> clustering = new ArrayList<>();
        List<NodeID> clusterSizes = new ArrayList<>();
        List<NodeID> leaderNodeMapping = new ArrayList<>();
        FastResetArray<EdgeWeight> ratingMap = new FastResetArray<>();
        FastResetArray<EdgeWeight> edgeWeightCollector = new FastResetArray<>();
        List<NodeID> clusterNodes = new ArrayList<>();

        public MemoryContext() {
            this.clustering = new ArrayList<>();
            this.clusterSizes = new ArrayList<>();
            this.leaderNodeMapping = new ArrayList<>();
            this.ratingMap = new FastResetArray<>();
            this.edgeWeightCollector = new FastResetArray<>();
            this.clusterNodes = new ArrayList<>();
        }

        // Constructor to initialize the lists with a specified size
        public MemoryContext(int size) {
            this.clustering = new ArrayList<>(size);
            this.clusterSizes = new ArrayList<>(size);
            this.leaderNodeMapping = new ArrayList<>(size);
            this.ratingMap = new FastResetArray<>(size);
            this.edgeWeightCollector = new FastResetArray<>(size);
            this.clusterNodes = new ArrayList<>(size);
        }

        public long memoryInKB() {
            return (clustering.size() * Long.BYTES +
                    clusterSizes.size() * Long.BYTES +
                    leaderNodeMapping.size() * Long.BYTES +
                    ratingMap.memoryInKB() +
                    edgeWeightCollector.memoryInKB() +
                    clusterNodes.size() * Long.BYTES) / 1000;
        }
    }

    private final Graph inputGraph;
    private Graph currentGraph;
    private final SequentialGraphHierarchy hierarchy;

    private final InitialCoarseningContext cCtx;

    private List<Cluster> clustering = new ArrayList<>();
    private FastResetArray<EdgeWeight> ratingMap = new FastResetArray<>();
    private List<NodeID> clusterSizes = new ArrayList<>();
    private List<NodeID> leaderNodeMapping = new ArrayList<>();
    private FastResetArray<EdgeWeight> edgeWeightCollector = new FastResetArray<>();
    private List<NodeID> clusterNodes = new ArrayList<>();

    private NodeID currentNumMoves = new NodeID(0);
    private boolean precomputedClustering = false;
    private NodeWeight interleavedMaxClusterWeight = new NodeWeight(0);
    private boolean interleavedLocked = false;
    private boolean isSet = false;
    private final Random_shm rand = Random_shm.getInstance();
    private final RandomPermutations<Integer> randomPermutations = new RandomPermutations<>(rand, CHUNK_SIZE, NUMBER_OF_NODE_PERMUTATIONS);

    public InitialCoarsener(Graph graph, InitialCoarseningContext cCtx, MemoryContext mCtx) {
        this.inputGraph = graph;
        this.currentGraph = graph;
        this.hierarchy = new SequentialGraphHierarchy(graph); // Pass the graph to the hierarchy
        this.cCtx = cCtx;
        this.clustering = mCtx.clustering;
        this.clusterSizes = mCtx.clusterSizes;
        this.leaderNodeMapping = mCtx.leaderNodeMapping;
        this.ratingMap = mCtx.ratingMap;
        this.edgeWeightCollector = mCtx.edgeWeightCollector;
        this.clusterNodes = mCtx.clusterNodes;

        // Ensure clustering is appropriately sized
        if (clustering.size() < inputGraph.n().value + 1) {
            for (int i = clustering.size(); i <= inputGraph.n().value; i++) {
                clustering.add(new Cluster());
            }
        }
    }


    public InitialCoarsener(Graph graph, InitialCoarseningContext cCtx) {
        this(graph, cCtx, new MemoryContext());
    }

    public int size() {
        return hierarchy.size();
    }

    public boolean empty() {
        return size() == 0;
    }

    public Graph coarsestGraph() {
        return hierarchy.coarsestGraph();
    }

    public Graph coarsen(Function<NodeID, NodeWeight> cbMaxClusterWeight) {
        NodeWeight maxClusterWeight = cbMaxClusterWeight.apply(currentGraph.n());

        if (/*!precomputedClustering*/ true) {
            performLabelPropagation(maxClusterWeight);
        }
        NodeID c_n = new NodeID(currentGraph.n().value - currentNumMoves.value);
        boolean converged = (1.0 - 1.0 * c_n.value / currentGraph.n().value) <= cCtx.convergenceThreshold;

        if (!converged) {
            interleavedMaxClusterWeight = cbMaxClusterWeight.apply(c_n);
            ContractionResult contractionResult = contractCurrentClustering();
            currentGraph = contractionResult.graph;
            leaderNodeMapping = contractionResult.leaderNodeMapping;

            hierarchy.takeCoarseGraph(currentGraph, leaderNodeMapping);
        }
        return currentGraph;
    }

    public PartitionedGraph uncoarsen(PartitionedGraph cPGraph) {
        // Perform the pop and project operation, retrieving the finer graph
        PartitionedGraph pGraph = hierarchy.popAndProject(cPGraph);

        // Update the current graph to the coarsest graph in the hierarchy
        this.currentGraph = hierarchy.coarsestGraph();

        return pGraph;
    }

    public MemoryContext free() {
        MemoryContext mCtx = new MemoryContext();
        mCtx.clustering = new ArrayList<>(this.clustering);
        mCtx.clusterSizes = new ArrayList<>(this.clusterSizes);
        mCtx.leaderNodeMapping = new ArrayList<>(this.leaderNodeMapping);
        mCtx.ratingMap = this.ratingMap;
        mCtx.edgeWeightCollector = this.edgeWeightCollector;
        mCtx.clusterNodes = new ArrayList<>(this.clusterNodes);

        // Clear the current context
        this.clustering = new ArrayList<>();
        this.clusterSizes = new ArrayList<>();
        this.leaderNodeMapping = new ArrayList<>();
        this.ratingMap = new FastResetArray<>();
        this.edgeWeightCollector = new FastResetArray<>();
        this.clusterNodes = new ArrayList<>();

        return mCtx;
    }


    private void resetCurrentClustering() {
        currentNumMoves = new NodeID(0);
        NodeID n = currentGraph.n();

        if (currentGraph.nodeWeighted()) {
            for (NodeID u = new NodeID(0); u.value < n.value; u = u.add(1)) {
                clustering.get(u.value).locked = false;
                clustering.get(u.value).leader = u;
                clustering.get(u.value).weight = currentGraph.nodeWeight(u);
            }
        } else {
            NodeWeight unitNodeWeight = new NodeWeight(currentGraph.totalNodeWeight().value / n.value);
            for (NodeID u = new NodeID(0); u.value < n.value; u = u.add(1)) {
                clustering.get(u.value).locked = false;
                clustering.get(u.value).leader = u;
                clustering.get(u.value).weight = unitNodeWeight;
            }
        }
    }
    private void resetCurrentClustering(NodeID cN, StaticArray<NodeWeight> cNodeWeights) {
        currentNumMoves = new NodeID(0);

        for (NodeID u = new NodeID(0); u.value < cN.value; u = u.add(1)) {
            clustering.get(u.value).locked = false;
            clustering.get(u.value).leader = u;
            clustering.get(u.value).weight = cNodeWeights.get(u.value);
        }
    }


    private ContractionResult contractCurrentClustering() {
        // Initialize the structures needed to create the coarser graph
        NodeID n = currentGraph.n();
        NodeID cN = new NodeID(n.value - currentNumMoves.value);

        StaticArray<NodeID> cNodes = new StaticArray<>(cN.value + 1);
        StaticArray<NodeWeight> cNodeWeights = new StaticArray<>(cN.value);  // Resize based on cN
        StaticArray<NodeID> cEdges = new StaticArray<>(currentGraph.m().value);  // Overestimated size
        StaticArray<EdgeWeight> cEdgeWeights = new StaticArray<>(currentGraph.m().value);  // Overestimated size

        List<NodeID> nodeMapping = new ArrayList<>(n.value);





        // Initialize mappings and clustering
        for (int i = 0; i < n.value; i++) {
            nodeMapping.add(new NodeID(0));
        }
        for (int i = 0; i < cN.value + 1; i++) {
            cNodes.set(i, new NodeID(0));
        }
        for (int i = 0; i < cN.value; i++) {
            cNodeWeights.set(i, new NodeWeight(0));
        }

        // Reset or initialize edgeWeightCollector mapping
        if (edgeWeightCollector.size() == 0) {
            // If size is zero, set the size to currentGraph.n().value
            edgeWeightCollector = new FastResetArray<>(n.value);
            for (int i = 0; i < n.value; i++) {
                edgeWeightCollector.set(i, new EdgeWeight(0));
            }
            edgeWeightCollector.clear();
        } else {
            // If size is not zero, reset every element to 0
            edgeWeightCollector.clear();
        }

        // Reset or initialize cluster sizes and leader node mapping
        if (clusterSizes.size() == 0) {
            // If size is zero, set the size to currentGraph.n().value
            clusterSizes = new ArrayList<>(n.value);
            for (int i = 0; i < n.value; i++) {
                clusterSizes.add(new NodeID(0));
            }
        } else {
            // If size is not zero, reset every element to 0
            for (int i = 0; i < clusterSizes.size(); i++) {
                clusterSizes.set(i, new NodeID(0));
            }
        }

        // Handle leaderNodeMapping initialization or resetting
        if (leaderNodeMapping.size() == 0) {
            leaderNodeMapping = new ArrayList<>(n.value);
            for (int i = 0; i < n.value; i++) {
                leaderNodeMapping.add(new NodeID(0));
            }
        } else {
            for (int i = 0; i < leaderNodeMapping.size(); i++) {
                leaderNodeMapping.set(i, new NodeID(0));
            }
        }

        if (clusterNodes.size() == 0) {
            clusterNodes = new ArrayList<>(n.value);
            for (int i = 0; i < n.value; i++) {
                clusterNodes.add(new NodeID(0));
            }
        }

        // Step 1: Build node mapping and cluster sizes
        NodeID currentNode = new NodeID(0);
        for (NodeID u = new NodeID(0); u.value < n.value; u = u.add(1)) {
            NodeID leader = clustering.get(u.value).leader;

            if (leaderNodeMapping.get(leader.value).value == 0) {
                cNodeWeights.set(currentNode.value, clustering.get(leader.value).weight);
                currentNode = currentNode.add(1);
                NodeID currentNodeValue = new NodeID(currentNode.value);
                leaderNodeMapping.set(leader.value, currentNodeValue);  // 1-based index

            }
            NodeID cluster = leaderNodeMapping.get(leader.value).subtract(1);  // Convert to 0-based index
            nodeMapping.set(u.value, cluster);
            clusterSizes.set(cluster.value, clusterSizes.get(cluster.value).add(1));
        }

        // Step 2: Update cluster nodes
        NodeID counter = new NodeID(0);
        for (int i = 0; i < clusterSizes.size(); i++) {
            NodeID temp = clusterSizes.get(i);
            clusterSizes.set(i, counter);
            counter = counter.add(temp.value);
        }

        // Build cluster nodes array
        for (NodeID u = new NodeID(0); u.value < n.value; u = u.add(1)) {
            NodeID cluster = nodeMapping.get(u.value);
            clusterNodes.set(clusterSizes.get(cluster.value).value++, u);
        }

        // Initialize clustering for the coarse graph
        resetCurrentClustering(cN, cNodeWeights);

        // Step 3: Build coarse graph nodes and edges
        NodeID cU = new NodeID(0);
        NodeID cM = new NodeID(0);
        cNodes.set(0, new NodeID(0));

        for (int i = 0; i < n.value; i++) { // Use clusterNodes instead of nodeMapping directly
            NodeID u = clusterNodes.get(i); // Get node from clusterNodes
            NodeID cluster = nodeMapping.get(u.value);

            if (!cluster.equals(cU)) {
                interleavedHandleNode(cU, cNodeWeights.get(cU.value));
                for (Integer cVIndex : edgeWeightCollector.usedEntryIds()) {
                    NodeID cV = new NodeID(cVIndex);
                    EdgeWeight weight = edgeWeightCollector.get(cV.value);
                    cEdges.set(cM.value, cV);
                    cEdgeWeights.set(cM.value++, weight);
                }
                edgeWeightCollector.clear();

                // Update cNodes before incrementing cU
                NodeID cMValue = new NodeID(cM.value);
                cNodes.set(cU.value + 1, cMValue);  // Correctly set the value of cM for the next index

                // Increment cU to move to the next coarse node
                cU = cU.add(1);  // Increment cU, similar to ++c_u in C++
            }

            // Add edges
            for (Edge edge : currentGraph.neighbors(u)) {
                NodeID v = currentGraph.edgeTarget(edge.getEdgeID());
                NodeID cV = nodeMapping.get(v.value);
                if (!cU.equals(cV)) {
                    EdgeWeight weight = currentGraph.edgeWeight(edge.getEdgeID());
                    edgeWeightCollector.set(cV.value, edgeWeightCollector.get(cV.value).add(weight));
                    interleavedVisitNeighbor(cU, cV, weight);
                }
            }
        }


        // Finish last cluster
        interleavedHandleNode(cU, cNodeWeights.get(cU.value));
        for (Integer cVIndex : edgeWeightCollector.usedEntryIds()) {
            NodeID cV = new NodeID(cVIndex);
            EdgeWeight weight = edgeWeightCollector.get(cV.value);
            cEdges.set(cM.value, cV);
            cEdgeWeights.set(cM.value++, weight);
        }
        NodeID cMValue = new NodeID(cM.value);
        cNodes.set(cU.value + 1, cMValue);
        edgeWeightCollector.clear();

        // Restrict cEdges and cEdgeWeights to actual size
        cEdges.resize(cM.value);
        cEdgeWeights.resize(cM.value);

        // Create the coarse graph
        Graph coarseGraph = new Graph(
                new Seq(),  // Use sequential initialization
                castToEdgeID(cNodes),
                cEdges,
                cNodeWeights,
                cEdgeWeights,
                false
        );

        return new ContractionResult(coarseGraph, nodeMapping);
    }
    private void interleavedHandleNode(NodeID cU, NodeWeight cUWeight) {
        if (!interleavedLocked) {
            NodeID bestCluster = pickClusterFromRatingMap(cU, cUWeight, interleavedMaxClusterWeight);
            if (!bestCluster.equals(cU)) {
                clustering.get(cU.value).leader = bestCluster;
                clustering.get(bestCluster.value).weight = clustering.get(bestCluster.value).weight.add(cUWeight);
                clustering.get(bestCluster.value).locked = true;
                currentNumMoves = currentNumMoves.add(1);
            }
        }
        interleavedLocked = clustering.get(cU.value + 1).locked;
    }

    private void interleavedVisitNeighbor(NodeID cU, NodeID cV, EdgeWeight weight) {
        if (!interleavedLocked) {
            ratingMap.set(cV.value, ratingMap.get(cV.value).add(weight));
        }
    }


    private void performLabelPropagation(NodeWeight maxClusterWeight) {
        resetCurrentClustering();

        int maxBucket = (int) Math.floor(Math.log(cCtx.largeDegreeThreshold) / Math.log(2));
        int bucketCount = Math.min(maxBucket, currentGraph.numberOfBuckets());

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            int bucketSize = currentGraph.bucketSize(bucket).value;
            if (bucketSize == 0) continue;

            int firstNode = currentGraph.firstNodeInBucket(bucket).value;
            int numChunks = bucketSize / CHUNK_SIZE;

            // Shuffle chunks
            List<Integer> chunks = new ArrayList<>();
            for (int i = 0; i < numChunks; i++) {
                chunks.add(firstNode + i * CHUNK_SIZE);
            }
            rand.shuffle(chunks);

            // Process chunks
            for (int chunkOffset : chunks) {
                List<Integer> permutation = randomPermutations.get();
                for (int localU : permutation) {
                    handleNode(new NodeID(chunkOffset + localU), maxClusterWeight);
                }
            }

            // Process last chunk
            int lastChunkSize = bucketSize % CHUNK_SIZE;
            int lastStart = firstNode + bucketSize - lastChunkSize;
            for (int localU = 0; localU < lastChunkSize; localU++) {
                handleNode(new NodeID(lastStart + localU), maxClusterWeight);
            }
        }

        precomputedClustering = true;
    }

    private void handleNode(NodeID u, NodeWeight maxClusterWeight) {
        Cluster clusterU = clustering.get(u.value);
        if (!clusterU.locked) {
            NodeWeight uWeight = currentGraph.nodeWeight(u);
            NodeID bestCluster = pickCluster(u, uWeight, maxClusterWeight);

            if (!bestCluster.equals(u)) {
                clusterU.leader = bestCluster;
                Cluster bestClusterObj = clustering.get(bestCluster.value);
                bestClusterObj.locked = true;
                bestClusterObj.weight = bestClusterObj.weight.add(uWeight);
                currentNumMoves = currentNumMoves.add(1);
            }
        }
    }
    private NodeID pickCluster(NodeID u, NodeWeight uWeight, NodeWeight maxClusterWeight) {
        // Resize the ratingMap to match the number of nodes in the current graph
        if(ratingMap.capacity() != currentGraph.n().value
                && !isSet){
            ratingMap.resize(currentGraph.n().value);
            isSet=true;
        }

        // Ensure the ratingMap is clear before starting
        ratingMap.clear();

        // Iterate over neighbors of u and update the rating map based on clustering
        for (Edge edge : currentGraph.neighbors(u)) {
            NodeID v = currentGraph.edgeTarget(edge.getEdgeID());
            NodeID leader = clustering.get(v.value).leader;
            // Accumulate edge weights for the cluster leader
            ratingMap.set(leader.value, ratingMap.get(leader.value).add(currentGraph.edgeWeight(edge.getEdgeID())));
        }

        // Pick the best cluster based on the accumulated rating map
        return pickClusterFromRatingMap(u, uWeight, maxClusterWeight);
    }

    private NodeID pickClusterFromRatingMap(NodeID u, NodeWeight uWeight, NodeWeight maxClusterWeight) {
        NodeID bestCluster = u;
        EdgeWeight bestClusterGain = new EdgeWeight(0);

        // Iterate over used entries in the rating map to determine the best cluster
        for (int i = 0; i < ratingMap.usedEntryIds().size(); i++) {
            Integer clusterId = ratingMap.usedEntryIds().get(i);
            NodeID cluster = new NodeID(clusterId);
            EdgeWeight gain = ratingMap.get(cluster.value);
            ratingMap.set(cluster.value, new EdgeWeight(0));  // Reset the gain in the rating map

            NodeWeight weight = clustering.get(cluster.value).weight;

            // Determine if the current cluster is better than the best found so far
            if ((gain.compareTo(bestClusterGain) > 0 ||
                    (gain.equals(bestClusterGain) && rand.randomBool())) &&
                    weight.add(uWeight).compareTo(maxClusterWeight) <= 0) {
                bestCluster = cluster;
                bestClusterGain = gain;
            }
        }

        // Clear the rating map after processing
        ratingMap.clear();

        return bestCluster;
    }


    public StaticArray<EdgeID> castToEdgeID(StaticArray<NodeID> nodeArray) {
        StaticArray<EdgeID> edgeIDArray = new StaticArray<>(nodeArray.size());
        for (int i = 0; i < nodeArray.size(); i++) {
            // Initialize with EdgeID(0) to avoid null values
            NodeID nodeID = nodeArray.get(i);
            if (nodeID != null) {
                edgeIDArray.set(i, new EdgeID(nodeID.value));
            } else {
                edgeIDArray.set(i, new EdgeID(0));
            }
        }
        return edgeIDArray;
    }

    private static class ContractionResult {
        final Graph graph;
        final List<NodeID> leaderNodeMapping;

        ContractionResult(Graph graph, List<NodeID> leaderNodeMapping) {
            this.graph = graph;
            this.leaderNodeMapping = leaderNodeMapping;
        }
    }
}
