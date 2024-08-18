package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.common.Math.Random_shm;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BinaryMinHeap;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.kaminpar_shm.PartitionedGraph;

public class MaxGainSelectionPolicy implements QueueSelectionPolicy {
    @Override
    public int select(PartitionedGraph pGraph, PartitionContext pCtx, BinaryMinHeap<EdgeWeight>[] queues, Random_shm rand) {
        EdgeWeight loss0 = queues[0].isEmpty() ? new EdgeWeight(Long.MAX_VALUE) : queues[0].peekKey();
        EdgeWeight loss1 = queues[1].isEmpty() ? new EdgeWeight(Long.MAX_VALUE) : queues[1].peekKey();

        if (loss0.equals(loss1)) {
            return (pGraph.blockWeight(new BlockID(1)).compareTo(pGraph.blockWeight(new BlockID(0))) > 0) ? 1 : 0;
        }
        return (loss1.compareTo(loss0) < 0) ? 1 : 0;
    }
}
