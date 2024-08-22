package org.alshar.kaminpar_shm.coarsening;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.alshar.Graph;
import org.alshar.common.GraphUtils.*;
import org.alshar.common.datastructures.*;
import org.alshar.common.timer.Timer_km;

public class Contraction {

    public static class MemoryContext {
        public StaticArray<Integer> buckets;
        public StaticArray<AtomicInteger> bucketsIndex;
        public StaticArray<AtomicInteger> leaderMapping;
        public StaticArray<NavigationMarker<Integer, Edge>> allBufferedNodes;

        // Constructor to initialize MemoryContext with the graph size
        public MemoryContext(int graphSize) {
            this.buckets = new StaticArray<>(graphSize);
            this.bucketsIndex = new StaticArray<>(graphSize);
            this.leaderMapping = new StaticArray<>(graphSize);
            this.allBufferedNodes = new StaticArray<>(graphSize);

            // Initialize elements in bucketsIndex and leaderMapping to AtomicInteger(0)
            for (int i = 0; i < graphSize; i++) {
                this.bucketsIndex.set(i, new AtomicInteger(0));
                this.leaderMapping.set(i, new AtomicInteger(0));
            }
        }

        // Constructor to initialize an empty MemoryContext
        public MemoryContext() {
            this.buckets = new StaticArray<>(0);
            this.bucketsIndex = new StaticArray<>(0);
            this.leaderMapping = new StaticArray<>(0);
            this.allBufferedNodes = new StaticArray<>(0);
        }
    }

    public static ContractResult contract(Graph graph, StaticArray<Integer> clustering, MemoryContext mCtx) {
        return contractGenericClustering(graph, clustering, mCtx);
    }

    private static ContractResult contractGenericClustering(Graph graph, StaticArray<Integer> clustering, MemoryContext mCtx) {
        StaticArray<AtomicInteger> bucketsIndex = mCtx.bucketsIndex;
        StaticArray<Integer> buckets = mCtx.buckets;
        StaticArray<AtomicInteger> leaderMapping = mCtx.leaderMapping;
        StaticArray<NavigationMarker<Integer, Edge>> allBufferedNodes = mCtx.allBufferedNodes;

        int n = graph.n().value;
        int c_n;
        // Allocate memory for mapping
        StaticArray<Integer> mapping = new StaticArray<>(n);

        try (var allocationTimer = Timer_km.global().startScopedTimer("Allocation")) {
            resizeIfNeeded(leaderMapping, n);
            resizeIfNeeded(buckets, n);
        }

        // Preprocessing phase
        try (var preprocessingTimer = Timer_km.global().startScopedTimer("Preprocessing")) {
            // Step 1: Set node_mapping[x] = 1 if there is a cluster with leader x
            for (int u = 0; u < n; u++) {
                leaderMapping.get(u).set(0);
            }
            for (int u = 0; u < n; u++) {
                leaderMapping.get(clustering.get(u)).set(1);
            }


            // Step 2: Compute prefix sum to get coarse node IDs
            prefixSum(leaderMapping, n);
            c_n = leaderMapping.get(n - 1).get();  // Number of nodes in the coarse graph

            // Step 3: Assign coarse node ID to all nodes
            for (int u = 0; u < n; u++) {
                mapping.set(u, leaderMapping.get(clustering.get(u)).get() - 1);
            }
        }

        // Allocation phase for buckets
        try (var allocationTimer = Timer_km.global().startScopedTimer("Allocation")) {
            // Prepare bucketsIndex
            for (int i = 0; i < c_n + 1; i++) {
                bucketsIndex.set(i, new AtomicInteger(0));
            }
            resizeIfNeeded(bucketsIndex, c_n + 1);
        }

        // Preprocessing phase for buckets
        try (var preprocessingTimer = Timer_km.global().startScopedTimer("Preprocessing")) {
            // Count the number of nodes in each bucket
            for (int u = 0; u < n; u++) {
                bucketsIndex.get(mapping.get(u)).incrementAndGet();
            }
            prefixSum(bucketsIndex, bucketsIndex.size());

            // Sort nodes into buckets
            for (int u = 0; u < n; u++) {
                int pos = bucketsIndex.get(mapping.get(u)).decrementAndGet();
                buckets.set(pos, u);
            }
        }

        // Build nodes array of the coarse graph
        StaticArray<EdgeID> cNodes = new StaticArray<>(c_n + 1);
        StaticArray<NodeWeight> cNodeWeights = new StaticArray<>(c_n);
        // Initialize NavigableLinkedList for each node
        List<NavigableLinkedList<Integer, Edge>> edgeBufferLists = new ArrayList<>(c_n);
        try (var allocationTimer = Timer_km.global().startScopedTimer("Allocation")) {
            for (int i = 0; i < cNodes.size(); i++) {
                cNodes.set(i, new EdgeID(0)); // Initialize each element to EdgeID(0)
            }
            for (int i = 0; i < cNodeWeights.size(); i++) {
                cNodeWeights.set(i, new NodeWeight(1)); // Initialize each element to a weight of 1
            }
        }

        // Construct coarse edges
        try (var constructEdgesTimer = Timer_km.global().startScopedTimer("Construct coarse edges")) {
            for (int i = 0; i < c_n; i++) {
                edgeBufferLists.add(new NavigableLinkedList<>());
            }
            buildCoarseGraphNodes(graph, cNodes, cNodeWeights, mapping, buckets, bucketsIndex, edgeBufferLists, c_n);

            // Combine all buffered nodes
            List<NavigationMarker<Integer, Edge>> combinedMarkers = TsNavigableList.combine(edgeBufferLists);
            mCtx.allBufferedNodes = new StaticArray<>(combinedMarkers.size());
            for (int i = 0; i < combinedMarkers.size(); i++) {
                mCtx.allBufferedNodes.set(i, combinedMarkers.get(i));
            }
        }

        // Construct coarse graph

        // Get the number of edges in the coarse graph from the last element of cNodes
        int numberOfEdges = cNodes.get(cNodes.size() - 1).value;
        // Initialize cEdges with the correct number of edges
        StaticArray<NodeID> cEdges = new StaticArray<>(numberOfEdges);
        StaticArray<EdgeWeight> cEdgeWeights = new StaticArray<>(numberOfEdges);
        try (var allocationTimer = Timer_km.global().startScopedTimer("Allocation")) {

            for (int i = 0; i < cEdges.size(); i++) {
                cEdges.set(i, new NodeID(0)); // Set each element to NodeID initialized with 0
            }

            for (int i = 0; i < cEdgeWeights.size(); i++) {
                cEdgeWeights.set(i, new EdgeWeight(1)); // Set each element to EdgeWeight initialized with 1
            }
        }

        // Construct the coarse graph
        try (var constructGraphTimer = Timer_km.global().startScopedTimer("Construct coarse graph")) {
            constructCoarseGraph(cEdges, cEdgeWeights, cNodes, mCtx.allBufferedNodes);
        }

        // Return the new coarse graph and the mapping
        Graph coarseGraph = new Graph(cNodes, cEdges, cNodeWeights, cEdgeWeights, false);
        return new ContractResult(coarseGraph, convertListToIntArray(StaticArray.release(mapping)), mCtx);
    }

    private static void buildCoarseGraphNodes(Graph graph, StaticArray<EdgeID> cNodes, StaticArray<NodeWeight> cNodeWeights,
                                              StaticArray<Integer> mapping, StaticArray<Integer> buckets, StaticArray<AtomicInteger> bucketsIndex,
                                              List<NavigableLinkedList<Integer, Edge>> edgeBufferLists, int c_n) {

        for (int c_u = 0; c_u < c_n; c_u++) {
            int first = bucketsIndex.get(c_u).get();
            int last = bucketsIndex.get(c_u + 1).get();
            long c_u_weight = 0;

            NavigableLinkedList<Integer, Edge> edgeBuffer = edgeBufferLists.get(c_u);

            // Mark the coarse node
            edgeBuffer.mark(c_u);

            for (int i = first; i < last; i++) {
                int u = buckets.get(i);
                c_u_weight += graph.nodeWeight(new NodeID(u)).value;

                for (Edge edge : graph.neighbors(new NodeID(u))) {
                    int c_v = mapping.get(edge.getTarget().value);
                    if (c_u != c_v) {
                        // Retrieve the weight of the edge using the edgeWeight method
                        EdgeWeight edgeWeight = graph.edgeWeight(edge.getEdgeID());
                        // Add the edge to the edge buffer with the correct weight
                        edgeBuffer.addElement(new Edge(edge.getEdgeID(), new NodeID(c_v), edgeWeight));
                    }
                }
            }

            // Set c_u_weight as NodeWeight
            cNodeWeights.set(c_u, new NodeWeight(c_u_weight));

            // Set the number of edges for this coarse node
            cNodes.set(c_u + 1, new EdgeID(edgeBuffer.position()));
        }

        // Now, perform a prefix sum on cNodes to compute cumulative edge counts
        // Initialize atomicCNodes with default AtomicInteger(0) values
        StaticArray<AtomicInteger> atomicCNodes = new StaticArray<>(cNodes.size());
        for (int i = 0; i < atomicCNodes.size(); i++) {
            atomicCNodes.set(i, new AtomicInteger(cNodes.get(i).value)); // Initialize with values from cNodes
        }

        // Compute prefix sum on atomicCNodes
        prefixSum(atomicCNodes, atomicCNodes.size());

        // Update cNodes with the cumulative sums from atomicCNodes
        for (int i = 0; i < cNodes.size(); i++) {
            cNodes.set(i, new EdgeID(atomicCNodes.get(i).get()));
        }
    }


    private static void constructCoarseGraph(StaticArray<NodeID> cEdges, StaticArray<EdgeWeight> cEdgeWeights,
                                             StaticArray<EdgeID> cNodes,
                                             StaticArray<NavigationMarker<Integer, Edge>> allBufferedNodes) {
        for (int c_u = 0; c_u < cNodes.size() - 1; c_u++) {
            int firstTargetIndex = cNodes.get(c_u).value;
            int c_u_degree = cNodes.get(c_u + 1).value - firstTargetIndex;

            List<Edge> edges = allBufferedNodes.get(c_u).getEdges(c_u_degree);
            for (int i = 0; i < c_u_degree; i++) {
                Edge edge = edges.get(i);
                cEdges.set(firstTargetIndex + i, edge.getTarget());
                cEdgeWeights.set(firstTargetIndex + i, edge.getWeight());
            }
        }
    }

    private static void prefixSum(StaticArray<AtomicInteger> array, int size) {
        int sum = 0;
        for (int i = 0; i < size; i++) {
            sum += array.get(i).get();
            array.get(i).set(sum);
        }
    }

    private static void resizeIfNeeded(StaticArray<?> array, int newSize) {
        if (array.size() < newSize) {
            array.resize(newSize);
        }
    }

    public static int[] convertListToIntArray(List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }
}
