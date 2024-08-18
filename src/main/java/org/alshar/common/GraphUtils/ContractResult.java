package org.alshar.common.GraphUtils;

import org.alshar.Graph;
import org.alshar.kaminpar_shm.coarsening.Contraction;

public class ContractResult {
    public Graph coarseGraph;
    public int[] mapping;
    public Contraction.MemoryContext mCtx;

    public ContractResult(Graph coarseGraph, int[] mapping, Contraction.MemoryContext mCtx) {
        this.coarseGraph = coarseGraph;
        this.mapping = mapping;
        this.mCtx = mCtx;
    }

    public Graph getCoarseGraph() {
        return coarseGraph;
    }
}