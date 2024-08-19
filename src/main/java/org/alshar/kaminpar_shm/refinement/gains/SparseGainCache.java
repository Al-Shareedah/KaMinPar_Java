package org.alshar.kaminpar_shm.refinement.gains;

import org.alshar.common.GraphUtils.Edge;
import org.alshar.common.datastructures.NodeID;
import org.alshar.kaminpar_shm.PartitionedGraph;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SparseGainCache {
    private final int n;
    private final int k;
    private final AtomicIntegerArray gainCache;
    private final AtomicIntegerArray weightedDegrees;

    public SparseGainCache(int n, int k) {
        this.n = n;
        this.k = k;
        this.gainCache = new AtomicIntegerArray(n * k);
        this.weightedDegrees = new AtomicIntegerArray(n);
    }

    public void initialize(PartitionedGraph pGraph) {
        reset();
        recomputeAll(pGraph);
    }

    public void reset() {
        for (int i = 0; i < gainCache.length(); i++) {
            gainCache.set(i, 0);
        }
    }

    public void recomputeAll(PartitionedGraph pGraph) {
        for (int u = 0; u < n; u++) {
            recomputeNode(pGraph, u);
        }
    }

    public void recomputeNode(PartitionedGraph pGraph, int u) {
        int blockU = pGraph.block(new NodeID(u)).value;
        weightedDegrees.set(u, 0);

        for (Edge edge : pGraph.neighbors(new NodeID(u))) {
            int v = edge.getTarget().value;
            int blockV = pGraph.block(new NodeID(v)).value;
            int weight = (int) pGraph.edgeWeight(edge.getEdgeID()).value;

            gainCache.addAndGet(index(u, blockV), weight);
            weightedDegrees.addAndGet(u, weight);
        }
    }

    private int index(int node, int block) {
        return node * k + block;
    }

    public int gain(int node, int blockFrom, int blockTo) {
        return weightedDegreeTo(node, blockTo) - weightedDegreeTo(node, blockFrom);
    }

    public int conn(int node, int block) {
        return weightedDegreeTo(node, block);
    }

    private int weightedDegreeTo(int node, int block) {
        return gainCache.get(index(node, block));
    }

    public void move(PartitionedGraph pGraph, int node, int blockFrom, int blockTo) {
        for (Edge edge : pGraph.neighbors(new NodeID(node))) {
            int v = edge.getTarget().value;
            int weight = (int) pGraph.edgeWeight(edge.getEdgeID()).value;

            gainCache.addAndGet(index(v, blockFrom), -weight);
            gainCache.addAndGet(index(v, blockTo), weight);
        }
    }
}

