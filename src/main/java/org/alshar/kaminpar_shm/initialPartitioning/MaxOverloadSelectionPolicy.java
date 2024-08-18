package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.common.Math.Random_shm;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BinaryMinHeap;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.kaminpar_shm.PartitionedGraph;

public class MaxOverloadSelectionPolicy implements QueueSelectionPolicy {
    @Override
    public int select(PartitionedGraph pGraph, PartitionContext pCtx, BinaryMinHeap<EdgeWeight>[] queues, Random_shm rand) {
        NodeWeight overload0 = NodeWeight.max(new NodeWeight(0), pGraph.blockWeight(new BlockID(0)).subtract(pCtx.blockWeights.max(0)));
        NodeWeight overload1 = NodeWeight.max(new NodeWeight(0), pGraph.blockWeight(new BlockID(1)).subtract(pCtx.blockWeights.max(1)));

        if (overload0.equals(new NodeWeight(0)) && overload1.equals(new NodeWeight(0))) {
            return new MaxGainSelectionPolicy().select(pGraph, pCtx, queues, rand);
        }
        return (overload1.compareTo(overload0) > 0 || (overload1.equals(overload0) && rand.randomBool())) ? 1 : 0;
    }
}
