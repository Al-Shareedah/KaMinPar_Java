package org.alshar.common.GraphUtils;
import org.alshar.Graph;
import org.alshar.common.Seq;
import org.alshar.common.datastructures.*;
import org.alshar.kaminpar_shm.PartitionedGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

import static org.alshar.kaminpar_shm.PartitionUtils.computeFinalK;

public class SubgraphExtractor {

    // Helper class for start positions of subgraph memory


    // Class to hold the result of subgraph extraction
    public static class SubgraphExtractionResult {
        public List<Graph> subgraphs;
        public AtomicIntegerArray nodeMapping;
        public List<SubgraphMemoryStartPosition> positions;

        public SubgraphExtractionResult(List<Graph> subgraphs, AtomicIntegerArray nodeMapping, List<SubgraphMemoryStartPosition> positions) {
            this.subgraphs = subgraphs;
            this.nodeMapping = nodeMapping;
            this.positions = positions;
        }
    }

    // Class to hold the result of sequential subgraph extraction
    public static class SequentialSubgraphExtractionResult {
        public List<Graph> subgraphs;
        public List<NodeID> nodeMapping = new ArrayList<>();
        public List<SubgraphMemoryStartPosition> positions;

        public SequentialSubgraphExtractionResult(List<Graph> subgraphs, List<SubgraphMemoryStartPosition> positions) {
            this.subgraphs = subgraphs;
            this.positions = positions;
        }
    }

    // Method to sequentially extract subgraphs
    public static SequentialSubgraphExtractionResult extractSubgraphsSequential(
            PartitionedGraph pGraph,
            int[] finalKs,
            SubgraphMemoryStartPosition memoryPosition,
            SubgraphMemory subgraphMemory,
            TemporarySubgraphMemory tmpSubgraphMemory) {

        assert pGraph.k().value == 2 : "Only suitable for bipartitions!";

        boolean isNodeWeighted = pGraph.getGraph().nodeWeighted();
        boolean isEdgeWeighted = pGraph.getGraph().edgeWeighted();

        int finalK = finalKs[0] + finalKs[1];
        tmpSubgraphMemory.ensureSizeNodes(new NodeID(pGraph.n().value + finalK), isNodeWeighted);

        List<EdgeID> nodes = tmpSubgraphMemory.getNodes();
        List<NodeID> edges = tmpSubgraphMemory.getEdges();
        List<NodeWeight> nodeWeights = tmpSubgraphMemory.getNodeWeights();
        List<EdgeWeight> edgeWeights = tmpSubgraphMemory.getEdgeWeights();
        List<NodeID> mapping = tmpSubgraphMemory.getMapping();

        int[] sN = new int[2];
        int[] sM = new int[2];

        // Find graph sizes
        for (int u = 0; u < pGraph.n().value; u++) {
            int b = pGraph.block(new NodeID(u)).value;
            mapping.set(u, new NodeID(sN[b]++));

            for (Edge edge : pGraph.neighbors(new NodeID(u))) {
                if (pGraph.block(edge.getTarget()).value == b) {
                    sM[b]++;
                }
            }
        }

        // Start position of subgraph[1] in common memory
        int n1 = sN[0] + finalKs[0];
        int m1 = sM[0];

        nodes.set(0, new EdgeID(0));
        nodes.set(n1, new EdgeID(0));
        tmpSubgraphMemory.ensureSizeEdges(new EdgeID(sM[0] + sM[1]), isEdgeWeighted);

        // Build and extract graphs in temporary memory buffer
        int[] nextEdgeId = new int[2];

        for (int u = 0; u < pGraph.n().value; u++) {
            int b = pGraph.block(new NodeID(u)).value;

            int n0 = b * n1;
            int m0 = b * m1; // either 0 or sM[0]

            for (Edge edge : pGraph.neighbors(new NodeID(u))) {
                if (pGraph.block(edge.getTarget()).value == b) {
                    edges.set(m0 + nextEdgeId[b], mapping.get(edge.getTarget().value));
                    if (isEdgeWeighted) {
                        edgeWeights.set(m0 + nextEdgeId[b], pGraph.edgeWeight(edge.getEdgeID()));
                    }
                    nextEdgeId[b]++;
                }
            }

            nodes.set(n0 + mapping.get(u).value + 1, new EdgeID(nextEdgeId[b]));
            if (isNodeWeighted) {
                nodeWeights.set(n0 + mapping.get(u).value, pGraph.nodeWeight(new NodeID(u)));
            }
        }

        // Copy graphs to subgraphMemory at memoryPosition
        System.arraycopy(nodes.toArray(), 0, subgraphMemory.getNodes().getArray(), (int) memoryPosition.nodesStartPos, pGraph.n().value + finalK);
        System.arraycopy(edges.toArray(), 0, subgraphMemory.getEdges().getArray(), (int) memoryPosition.edgesStartPos, sM[0] + sM[1]);
        if (isNodeWeighted) {
            System.arraycopy(nodeWeights.toArray(), 0, subgraphMemory.getNodeWeights().getArray(), (int) memoryPosition.nodesStartPos, pGraph.n().value + finalK);
        }
        if (isEdgeWeighted) {
            System.arraycopy(edgeWeights.toArray(), 0, subgraphMemory.getEdgeWeights().getArray(), (int) memoryPosition.edgesStartPos, sM[0] + sM[1]);
        }

        SubgraphMemoryStartPosition[] subgraphPositions = new SubgraphMemoryStartPosition[2];
        subgraphPositions[0] = new SubgraphMemoryStartPosition(memoryPosition.nodesStartPos, memoryPosition.edgesStartPos);
        subgraphPositions[1] = new SubgraphMemoryStartPosition(memoryPosition.nodesStartPos + n1, memoryPosition.edgesStartPos + m1);

        List<Graph> subgraphs = new ArrayList<>();
        subgraphs.add(createGraph(subgraphMemory, isNodeWeighted, isEdgeWeighted, memoryPosition.nodesStartPos, sN[0], memoryPosition.edgesStartPos, sM[0]));
        subgraphs.add(createGraph(subgraphMemory, isNodeWeighted, isEdgeWeighted, memoryPosition.nodesStartPos + n1, sN[1], memoryPosition.edgesStartPos + m1, sM[1]));

        return new SequentialSubgraphExtractionResult(subgraphs, Arrays.asList(subgraphPositions));
    }

    private static Graph createGraph(SubgraphMemory subgraphMemory, boolean isNodeWeighted, boolean isEdgeWeighted, long nodeStartPos, int nodeCount, long edgeStartPos, int edgeCount) {
        // Initialize StaticArray for nodes
        StaticArray<EdgeID> sNodes = new StaticArray<>(nodeCount + 1);
        for (int i = 0; i <= nodeCount; i++) {
            sNodes.set(i, subgraphMemory.getNodes().get((int) nodeStartPos + i));
        }

        // Initialize StaticArray for edges
        StaticArray<NodeID> sEdges = new StaticArray<>(edgeCount);
        for (int i = 0; i < edgeCount; i++) {
            sEdges.set(i, subgraphMemory.getEdges().get((int) edgeStartPos + i));
        }

        // Initialize StaticArray for node weights
        StaticArray<NodeWeight> sNodeWeights = isNodeWeighted ? new StaticArray<>(nodeCount) : new StaticArray<>(0);
        if (isNodeWeighted) {
            for (int i = 0; i < nodeCount; i++) {
                sNodeWeights.set(i, subgraphMemory.getNodeWeights().get((int) nodeStartPos + i));
            }
        }

        // Initialize StaticArray for edge weights
        StaticArray<EdgeWeight> sEdgeWeights = isEdgeWeighted ? new StaticArray<>(edgeCount) : new StaticArray<>(0);
        if (isEdgeWeighted) {
            for (int i = 0; i < edgeCount; i++) {
                sEdgeWeights.set(i, subgraphMemory.getEdgeWeights().get((int) edgeStartPos + i));
            }
        }

        // Create and return the new Graph instance
        return new Graph(sNodes, sEdges, sNodeWeights, sEdgeWeights, false);
    }


    // Method to extract subgraphs from a partitioned graph
    public static SubgraphExtractionResult extractSubgraphs(PartitionedGraph pGraph, BlockID inputK, SubgraphMemory subgraphMemory) {
        Graph graph = pGraph.getGraph();

        // Allocation
        int n = pGraph.n().value;
        AtomicIntegerArray mapping = new AtomicIntegerArray(n);
        List<SubgraphMemoryStartPosition> startPositions = new ArrayList<>(pGraph.k().value + 1);
        AtomicIntegerArray bucketIndex = new AtomicIntegerArray(pGraph.k().value);
        List<Graph> subgraphs = new ArrayList<>(Collections.nCopies(pGraph.k().value, null));

        // Initialize positions
        for (int i = 0; i < pGraph.k().value + 1; i++) {
            startPositions.add(new SubgraphMemoryStartPosition(0, 0)); // Provide initial values
        }

        // Count number of nodes and edges in each block
        AtomicIntegerArray numNodesInBlock = new AtomicIntegerArray(pGraph.k().value);
        AtomicLongArray numEdgesInBlock = new AtomicLongArray(pGraph.k().value);

        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                IntStream.range(0, n).parallel().forEach(u -> {
                    int uBlock = pGraph.block(new NodeID(u)).value;
                    numNodesInBlock.incrementAndGet(uBlock);
                    for (NodeID v : graph.adjacentNodes(new NodeID(u))) {
                        if (pGraph.block(v).value == uBlock) {
                            numEdgesInBlock.incrementAndGet(uBlock);
                        }
                    }
                });
            }
        });

        // Merge block sizes and compute final K
        for (int b = 0; b < pGraph.k().value; b++) {
            int padding = computeFinalK(b, pGraph.k().value, inputK.value); // Correctly compute padding
            startPositions.get(b + 1).nodesStartPos = numNodesInBlock.get(b) + padding;
            startPositions.get(b + 1).edgesStartPos = numEdgesInBlock.get(b);
        }

        // Apply prefix sum to start positions
        for (int b = 1; b <= pGraph.k().value; b++) {
            startPositions.get(b).nodesStartPos += startPositions.get(b - 1).nodesStartPos;
            startPositions.get(b).edgesStartPos += startPositions.get(b - 1).edgesStartPos;
        }

        // Build temporary bucket array in nodes array
        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                IntStream.range(0, n).parallel().forEach(u -> {
                    int b = pGraph.block(new NodeID(u)).value;
                    int posInSubgraph = bucketIndex.incrementAndGet(b);
                    long pos = startPositions.get(b).nodesStartPos + posInSubgraph;
                    subgraphMemory.getNodes().set((int) pos, new EdgeID(u));
                    mapping.set(u, posInSubgraph);
                });
            }
        });

        boolean isNodeWeighted = /* graph.nodeWeighted() */ false;
        boolean isEdgeWeighted = graph.edgeWeighted();

        // Build subgraph
        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                IntStream.range(0, pGraph.k().value).parallel().forEach(b -> {
                    long nodesStartPos = startPositions.get(b).nodesStartPos;
                    long e = 0;
                    for (int u = 0; u < bucketIndex.get(b); u++) {
                        long pos = nodesStartPos + u;
                        int uPrime = subgraphMemory.getNodes().get((int) pos).value;
                        subgraphMemory.getNodes().set((int) pos, new EdgeID((int) e));
                        if (isNodeWeighted) {
                            subgraphMemory.getNodeWeights().set((int) pos, new NodeWeight(graph.nodeWeight(new NodeID(uPrime)).value));
                        }

                        long e0 = startPositions.get(b).edgesStartPos;

                        for (Edge edge : graph.neighbors(new NodeID(uPrime))) {
                            if (pGraph.block(edge.getTarget()).value == b) {
                                if (isEdgeWeighted) {
                                    subgraphMemory.getEdgeWeights().set((int) (e0 + e), new EdgeWeight(graph.edgeWeight(edge.getEdgeID()).value));
                                }
                                subgraphMemory.getEdges().set((int) (e0 + e), new NodeID(mapping.get(edge.getTarget().value)));
                                e++;
                            }
                        }
                    }
                    subgraphMemory.getNodes().set((int) (nodesStartPos + bucketIndex.get(b)), new EdgeID((int) e));
                });
            }
        });

        // Create graph objects
        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                IntStream.range(0, pGraph.k().value).parallel().forEach(b -> {
                    long n0 = startPositions.get(b).nodesStartPos;
                    long m0 = startPositions.get(b).edgesStartPos;

                    long n = Math.abs(startPositions.get(b + 1).nodesStartPos - n0 - computeFinalK(b, pGraph.k().value, inputK.value));
                    long m = Math.abs(startPositions.get(b + 1).edgesStartPos - m0);

                    StaticArray<EdgeID> sNodes = new StaticArray<>((int) n0, (int) (n + 1), subgraphMemory.getNodes().getArray());
                    StaticArray<NodeID> sEdges = new StaticArray<>((int) m0, (int) m, subgraphMemory.getEdges().getArray());
                    StaticArray<NodeWeight> sNodeWeights = new StaticArray<>(isNodeWeighted ? (int) n0 : 0, isNodeWeighted ? (int) n : 0, subgraphMemory.getNodeWeights().getArray());
                    StaticArray<EdgeWeight> sEdgeWeights = new StaticArray<>(isEdgeWeighted ? (int) m0 : 0, isEdgeWeighted ? (int) m : 0, subgraphMemory.getEdgeWeights().getArray());

                    subgraphs.set(b, new Graph(new Seq(),sNodes, sEdges, sNodeWeights, sEdgeWeights, false));
                });
            }
        });


        return new SubgraphExtractionResult(subgraphs, mapping, startPositions);
    }

    // Utility method to copy subgraph partitions back into the original partitioned graph
    public static PartitionedGraph copySubgraphPartitions(
            PartitionedGraph pGraph,
            List<StaticArray<BlockID>> subgraphPartitions,
            BlockID kPrime,
            BlockID inputK,
            AtomicIntegerArray mapping) {

        // Compute block offsets
        int[] k0 = new int[pGraph.k().value + 1];
        Arrays.fill(k0, kPrime.value / pGraph.k().value);

        if (kPrime.equals(inputK)) {
            for (int b = 0; b < pGraph.k().value; b++) {
                k0[b + 1] = computeFinalK(b, pGraph.k().value, inputK.value);
            }
        }

        k0[0] = 0;
        Arrays.parallelPrefix(k0, Integer::sum);

        // Copy the subgraph partitions into the partitioned graph
        StaticArray<BlockID> partition = pGraph.takeRawPartition();

        IntStream.range(0, pGraph.n().value).parallel().forEach(u -> {
            int b = partition.get(u).value;
            int sU = mapping.get(u);
            partition.set(u, new BlockID(k0[b] + subgraphPartitions.get(b).get(sU).value));
        });

        PartitionedGraph newPGraph = new PartitionedGraph(pGraph.getGraph(), kPrime, partition);

        return newPGraph;
    }

}
