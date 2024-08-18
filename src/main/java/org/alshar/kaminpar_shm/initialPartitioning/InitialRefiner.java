package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.Graph;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.kaminpar_shm.refinement.Marker;
import java.util.ArrayList;
import java.util.List;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BinaryMinHeap;

import org.alshar.common.datastructures.EdgeWeight;

import java.util.ArrayList;
import java.util.List;

public abstract class InitialRefiner {
    public static class MemoryContext {
        BinaryMinHeap<EdgeWeight>[] queues = new BinaryMinHeap[2];
        Marker marker = new Marker(0, 1);
        List<EdgeWeight> weightedDegrees = new ArrayList<>();

        public MemoryContext() {
            queues[0] = new BinaryMinHeap<>(0, new EdgeWeight(Long.MIN_VALUE));
            queues[1] = new BinaryMinHeap<>(0, new EdgeWeight(Long.MIN_VALUE));
        }

        public void resize(int n) {
            if (queues[0].capacity() < n) {
                queues[0].resize(n);
            }
            if (queues[1].capacity() < n) {
                queues[1].resize(n);
            }
            if (marker.size() < n) {
                marker.resize(n);
            }
            if (weightedDegrees.size() < n) {
                for (int i = weightedDegrees.size(); i < n; i++) {
                    weightedDegrees.add(new EdgeWeight(0));
                }
            }
        }

        public long memoryInKB() {
            return marker.memoryInKB() +
                    weightedDegrees.size() * Long.BYTES / 1000 +
                    queues[0].memoryInKB() + queues[1].memoryInKB();
        }
    }

    public abstract void initialize(Graph graph);

    public abstract boolean refine(PartitionedGraph pGraph, PartitionContext pCtx);

    public abstract MemoryContext free();
}


