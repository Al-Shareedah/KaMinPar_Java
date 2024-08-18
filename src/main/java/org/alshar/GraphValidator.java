package org.alshar;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import org.alshar.common.datastructures.*;

public class GraphValidator {

    public static void validateUndirectedGraph(StaticArray<EdgeID> nodes, StaticArray<NodeID> edges, StaticArray<NodeWeight> nodeWeights, StaticArray<EdgeWeight> edgeWeights) {
        int n = nodes.size() - 1;
        int m = edges.size();

        // Create a copy of edges and edge weights for sorting
        List<Tuple<NodeID, EdgeWeight>> edgesWithWeights = new ArrayList<>(m);
        for (int e = 0; e < m; e++) {
            EdgeWeight weight = edgeWeights.isEmpty() ? new EdgeWeight(1) : edgeWeights.get(e);
            NodeID nodeId = edges.get(e);

            // Null checks
            if (nodeId == null) {
                throw new NullPointerException("NodeID at index " + e + " is null.");
            }

            edgesWithWeights.add(new Tuple<>(nodeId, weight));
        }

        // Sort outgoing edges of each node
        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                for (int u = 0; u < n; u++) {
                    int start = nodes.get(u).value;
                    int end = nodes.get(u + 1).value;

                    // Null check for node indices
                    if (start >= edgesWithWeights.size() || end > edgesWithWeights.size()) {
                        throw new IndexOutOfBoundsException("Invalid node index range: " + start + " to " + end);
                    }

                    Collections.sort(
                            edgesWithWeights.subList(start, end),
                            Comparator.comparingInt(o -> o.first.value)
                    );

                    // Check for multi edges
                    for (int e = start + 1; e < end; e++) {
                        if (edgesWithWeights.get(e - 1).first.value == edgesWithWeights.get(e).first.value) {
                            System.err.println("Node " + (u + 1) + " has multiple edges to neighbor " + edgesWithWeights.get(e).first.value);
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
                    final int currentU = u;  // Make u effectively final by assigning it to another final variable
                    int start = nodes.get(currentU).value;
                    int end = nodes.get(currentU + 1).value;

                    for (int e = start; e < end; e++) {
                        NodeID v = edgesWithWeights.get(e).first;
                        EdgeWeight weight = edgesWithWeights.get(e).second;

                        // Null check
                        if (v == null) {
                            throw new NullPointerException("NodeID is null at edge index " + e);
                        }

                        int vStart = nodes.get(v.value).value;
                        int vEnd = nodes.get(v.value + 1).value;

                        List<Tuple<NodeID, EdgeWeight>> vEdges = edgesWithWeights.subList(vStart, vEnd);

                        // Check for reverse edge
                        if (vEdges.stream().noneMatch(edge -> edge.first.value == currentU && edge.second.equals(weight))) {
                            System.err.println("Missing reverse edge: of edge " + (currentU + 1) + " --> " + (v.value + 1)
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
