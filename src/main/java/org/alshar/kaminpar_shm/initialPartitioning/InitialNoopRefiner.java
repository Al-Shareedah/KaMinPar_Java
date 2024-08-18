package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.context.PartitionContext;
import org.alshar.kaminpar_shm.PartitionedGraph;

public class InitialNoopRefiner extends InitialRefiner {
    private MemoryContext mCtx;

    public InitialNoopRefiner(MemoryContext mCtx) {
        this.mCtx = mCtx;
    }

    @Override
    public void initialize(Graph graph) {
        // No initialization needed
    }

    @Override
    public boolean refine(PartitionedGraph pGraph, PartitionContext pCtx) {
        return false;
    }

    @Override
    public MemoryContext free() {
        return mCtx;
    }
}