package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.context.InitialRefinementContext;
import org.alshar.common.datastructures.EdgeWeight;

public class AdaptiveStoppingPolicy implements StoppingPolicy {
    private double beta;
    private int numSteps;
    private double variance;
    private double Mk;
    private double MkMinus1;
    private double Sk;
    private double SkMinus1;

    @Override
    public void init(Graph graph) {
        this.beta = Math.round(Math.sqrt(graph.n().value));
    }

    @Override
    public boolean shouldStop(InitialRefinementContext fmCtx) {
        double factor = (fmCtx.alpha / 2.0) - 0.25;
        return (numSteps > beta) && ((Mk == 0) || (numSteps >= (variance / (Mk * Mk)) * factor));
    }

    @Override
    public void reset() {
        numSteps = 0;
        variance = 0.0;
        Mk = 0.0;
        MkMinus1 = 0.0;
        Sk = 0.0;
        SkMinus1 = 0.0;
    }

    @Override
    public void update(EdgeWeight gain) {
        numSteps++;
        if (numSteps == 1) {
            MkMinus1 = gain.value;
            Mk = MkMinus1;
            SkMinus1 = 0.0;
        } else {
            Mk = MkMinus1 + (gain.value - MkMinus1) / numSteps;
            Sk = SkMinus1 + (gain.value - MkMinus1) * (gain.value - Mk);
            variance = Sk / (numSteps - 1.0);

            MkMinus1 = Mk;
            SkMinus1 = Sk;
        }
    }
}