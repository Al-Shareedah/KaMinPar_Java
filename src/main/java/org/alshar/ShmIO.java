package org.alshar;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.alshar.common.StaticArray;
import org.alshar.kaminpar_shm.kaminpar.*;

public class ShmIO {

    public static class Metis {
        public static void read(String filename, StaticArray<EdgeID> nodes, StaticArray<NodeID> edges,
                                StaticArray<NodeWeight> nodeWeights, StaticArray<EdgeWeight> edgeWeights,
                                boolean checked) throws IOException {
            List<String> lines = Files.readAllLines(Paths.get(filename));
            boolean storeNodeWeights = false;
            boolean storeEdgeWeights = false;
            long totalNodeWeight = 0;
            long totalEdgeWeight = 0;

            int u = 0;
            int e = 0;

            for (String line : lines) {
                // parse the line accordingly to METIS format
                // the first line would be the format specification
                if (line.startsWith("%")) continue;
                if (line.isEmpty()) continue;

                String[] tokens = line.split("\\s+");
                if (tokens.length == 1) {
                    // This should be a node weight or edge weight line
                    if (storeNodeWeights) {
                        long weight = Long.parseLong(tokens[0]);
                        if (checked && weight > Integer.MAX_VALUE) {
                            throw new IOException("Node weight is too large for the node weight type");
                        }
                        nodeWeights.set(u, new NodeWeight((int) weight));
                        totalNodeWeight += weight;
                        u++;
                    } else if (storeEdgeWeights) {
                        long weight = Long.parseLong(tokens[0]);
                        if (checked && weight > Integer.MAX_VALUE) {
                            throw new IOException("Edge weight is too large for the edge weight type");
                        }
                        edgeWeights.set(e, new EdgeWeight((int) weight));
                        totalEdgeWeight += weight;
                        e++;
                    }
                } else {
                    // This should be an edge line
                    for (int i = 0; i < tokens.length; i++) {
                        int v = Integer.parseInt(tokens[i]) - 1; // 1-based index in METIS
                        if (checked && v >= nodes.size()) {
                            throw new IOException("Neighbor out of bounds");
                        }
                        edges.set(e, new NodeID(v));
                        e++;
                    }
                    nodes.set(u, new EdgeID(e));
                    u++;
                }
            }

            nodes.set(u, new EdgeID(e));

            if (totalNodeWeight <= nodes.size()) {
                nodeWeights.free();
            }

            if (totalEdgeWeight <= edges.size()) {
                edgeWeights.free();
            }
        }
    }

    public static class Partition {
        public static void write(String filename, List<Integer> partition) throws IOException {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                for (int block : partition) {
                    writer.write(block + "\n");
                }
            }
        }

        public static List<Integer> read(String filename) throws IOException {
            List<Integer> partition = new ArrayList<>();
            List<String> lines = Files.readAllLines(Paths.get(filename));

            for (String line : lines) {
                partition.add(Integer.parseInt(line.trim()));
            }

            return partition;
        }
    }
}

