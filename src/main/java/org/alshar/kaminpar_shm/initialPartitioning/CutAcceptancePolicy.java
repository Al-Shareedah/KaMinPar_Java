package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.kaminpar_shm.PartitionedGraph;

public interface CutAcceptancePolicy {
    boolean accept(PartitionedGraph pGraph, PartitionContext pCtx, NodeWeight acceptedOverload, NodeWeight currentOverload, EdgeWeight acceptedDelta, EdgeWeight currentDelta);
}

