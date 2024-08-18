package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.context.InitialRefinementContext;
import org.alshar.common.datastructures.EdgeWeight;

public interface StoppingPolicy {
    void init(Graph graph);

    boolean shouldStop(InitialRefinementContext fmCtx);

    void reset();

    void update(EdgeWeight gain);
}

