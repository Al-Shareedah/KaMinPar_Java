package org.alshar.common.datastructures;

import org.alshar.common.context.PartitionContext;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.kaminpar_shm.initialPartitioning.CutAcceptancePolicy;

public class BalancedMinCutAcceptancePolicy implements CutAcceptancePolicy {
    @Override
    public boolean accept(PartitionedGraph pGraph, PartitionContext pCtx, NodeWeight acceptedOverload, NodeWeight currentOverload, EdgeWeight acceptedDelta, EdgeWeight currentDelta) {
        return currentOverload.compareTo(acceptedOverload) <= 0 && currentDelta.compareTo(acceptedDelta) < 0;
    }
}
