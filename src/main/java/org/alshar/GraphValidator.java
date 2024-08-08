package org.alshar;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import org.alshar.common.StaticArray;
import org.alshar.kaminpar_shm.kaminpar.*;
public class GraphValidator {
    public static void validateUndirectedGraph(StaticArray<EdgeID> nodes, StaticArray<NodeID> edges, StaticArray<NodeWeight> nodeWeights, StaticArray<EdgeWeight> edgeWeights) {
        int n = nodes.size() - 1;
        int m = edges.size();

        // Create a copy of edges and edge weights for sorting
        List<Tuple<NodeID, EdgeWeight>> edgesWithWeights = new ArrayList<>(m);
        for (int e = 0; e < m; e++) {
            EdgeWeight weight = edgeWeights.isEmpty() ? new EdgeWeight(1) : edgeWeights.get(e);
            edgesWithWeights.add(new Tuple<>(edges.get(e), weight));
        }

        // Sort outgoing edges of each node
        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                for (int u = 0; u < n; u++) {
                    Collections.sort(
                            edgesWithWeights.subList(nodes.get(u).value, nodes.get(u + 1).value),
                            Comparator.comparingInt(o -> o.first.value)
                    );

                    // Check for multi edges
                    for (int e = nodes.get(u).value + 1; e < nodes.get(u + 1).value; e++) {
                        if (edges.get(e - 1).value == edges.get(e).value) {
                            System.err.println("node " + (u + 1) + " has multiple edges to neighbor " + edges.get(e).value);
                            System.exit(1);
                        }
                    }
                }
            }
        });

        // Check for reverse edges
        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                for (int u = 0; u < n; u++) {
                    for (int e = nodes.get(u).value; e < nodes.get(u + 1).value; e++) {
                        NodeID v = edgesWithWeights.get(e).first;
                        EdgeWeight weight = edgesWithWeights.get(e).second;

                        List<Tuple<NodeID, EdgeWeight>> vEdges = edgesWithWeights.subList(nodes.get(v.value).value, nodes.get(v.value + 1).value);
                        if (Collections.binarySearch(vEdges, new Tuple<>(new NodeID(u), weight), Comparator.comparingInt(o -> o.first.value)) < 0) {
                            System.err.println("missing reverse edge: of edge " + (u + 1) + " --> " + (v.value + 1)
                                    + " (the reverse edge might exist but with an inconsistent weight)");
                            System.exit(1);
                        }
                    }
                }
            }
        });
    }

    // Utility class to hold pairs of values
    public static class Tuple<A, B> {
        public final A first;
        public final B second;

        public Tuple(A first, B second) {
            this.first = first;
            this.second = second;
        }
    }
}
