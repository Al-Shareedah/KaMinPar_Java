package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.context.InitialRefinementContext;
import org.alshar.common.datastructures.EdgeWeight;


public class SimpleStoppingPolicy implements StoppingPolicy {
    private int numSteps;

    @Override
    public void init(Graph graph) {
        // No initialization needed for SimpleStoppingPolicy
    }

    @Override
    public boolean shouldStop(InitialRefinementContext fmCtx) {
        return numSteps > fmCtx.numFruitlessMoves;
    }

    @Override
    public void reset() {
        numSteps = 0;
    }

    @Override
    public void update(EdgeWeight gain) {
        numSteps++;
    }
}

