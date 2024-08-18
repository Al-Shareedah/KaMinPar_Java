package org.alshar.kaminpar_shm.refinement;

import org.alshar.common.context.PartitionContext;
import org.alshar.kaminpar_shm.PartitionedGraph;

public class NoopRefiner extends Refiner {

    @Override
    public void initialize(PartitionedGraph pGraph) {
        // No initialization needed for NoopRefiner
    }

    @Override
    public boolean refine(PartitionedGraph pGraph, PartitionContext pCtx) {
        // No refinement occurs in NoopRefiner, always return false
        return false;
    }
}
