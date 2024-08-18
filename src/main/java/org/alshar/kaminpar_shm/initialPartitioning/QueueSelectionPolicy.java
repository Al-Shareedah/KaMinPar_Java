package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.common.Math.Random_shm;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BinaryMinHeap;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.kaminpar_shm.PartitionedGraph;

public interface QueueSelectionPolicy {
    int select(PartitionedGraph pGraph, PartitionContext pCtx, BinaryMinHeap<EdgeWeight>[] queues, Random_shm rand);
}

