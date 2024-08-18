package org.alshar.kaminpar_shm.io;

import org.alshar.common.datastructures.*;
import org.alshar.common.datastructures.StaticArray;

import java.io.IOException;

public class MetisReader {
    public static void read(
            String filename,
            StaticArray<EdgeID> nodes,
            StaticArray<NodeID> edges,
            StaticArray<NodeWeight> nodeWeights,
            StaticArray<EdgeWeight> edgeWeights,
            boolean checked
    ) throws IOException {
        // Flags for storing weights
        final boolean[] storeNodeWeights = {false};
        final boolean[] storeEdgeWeights = {false};
        final long[] totalNodeWeight = {0};
        final long[] totalEdgeWeight = {0};

        final int[] u = {0};
        final int[] e = {0};

        MetisParser.parse(filename,
                format -> {
                    if (checked) {
                        if (format.numberOfNodes >= Integer.MAX_VALUE) {
                            throw new RuntimeException("Number of nodes is too large for the node ID type");
                        }
                        if (format.numberOfEdges >= Integer.MAX_VALUE) {
                            throw new RuntimeException("Number of edges is too large for the edge ID type");
                        }
                        if (format.numberOfEdges > (format.numberOfNodes * (format.numberOfNodes - 1)) / 2) {
                            throw new RuntimeException("Specified number of edges is impossibly large");
                        }
                    }

                    storeNodeWeights[0] = format.hasNodeWeights;
                    storeEdgeWeights[0] = format.hasEdgeWeights;
                    nodes.resize((int) format.numberOfNodes + 1);
                    edges.resize((int) format.numberOfEdges * 2);
                    if (storeNodeWeights[0]) {
                        nodeWeights.resize((int) format.numberOfNodes);
                    }
                    if (storeEdgeWeights[0]) {
                        edgeWeights.resize((int) format.numberOfEdges * 2);
                    }
                },
                weight -> {
                    if (checked) {
                        if (weight > Integer.MAX_VALUE) {
                            throw new RuntimeException("Node weight is too large for the node weight type");
                        }
                        if (weight <= 0) {
                            throw new RuntimeException("Zero node weights are not supported");
                        }
                    }

                    if (storeNodeWeights[0]) {
                        nodeWeights.set(u[0], new NodeWeight((int) Math.min(weight, Integer.MAX_VALUE)));
                    }
                    nodes.set(u[0], new EdgeID(e[0]));
                    totalNodeWeight[0] += weight;
                    u[0]++;
                },
                (weight, v) -> {
                    if (checked) {
                        if (weight > Integer.MAX_VALUE) {
                            throw new RuntimeException("Edge weight is too large for the edge weight type");
                        }
                        if (weight <= 0) {
                            throw new RuntimeException("Zero edge weights are not supported");
                        }
                        if (v + 1 >= nodes.size()) {
                            throw new RuntimeException("Neighbor " + (v + 1) + " of node " + (u[0] + 1) + " is out of bounds");
                        }
                        if (v + 1 == u[0]) {
                            throw new RuntimeException("Detected self-loop on node " + (v + 1) + ", which is not allowed");
                        }
                    }

                    if (storeEdgeWeights[0]) {
                        edgeWeights.set(e[0], new EdgeWeight((int) Math.min(weight, Integer.MAX_VALUE)));
                    }
                    edges.set(e[0], new NodeID(Math.toIntExact(v)));
                    totalEdgeWeight[0] += weight;
                    e[0]++;
                }
        );
        nodes.set(u[0], new EdgeID(e[0]));

        if (checked) {
            if (totalNodeWeight[0] > Integer.MAX_VALUE) {
                throw new RuntimeException("Total node weight does not fit into the node weight type");
            }
            if (totalEdgeWeight[0] > Integer.MAX_VALUE) {
                throw new RuntimeException("Total edge weight does not fit into the edge weight type");
            }
        }

        boolean unitNodeWeights = totalNodeWeight[0] + 1 == nodes.size();
        if (unitNodeWeights) {
            nodeWeights.free();
        }

        boolean unitEdgeWeights = totalEdgeWeight[0] == edges.size();
        if (unitEdgeWeights) {
            edgeWeights.free();
        }
    }
}
