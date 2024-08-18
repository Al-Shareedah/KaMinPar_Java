package org.alshar.common.context;

import org.alshar.common.enums.FMStoppingRule;

public class InitialRefinementContext {
    public boolean disabled;
    public FMStoppingRule stoppingRule;
    public int numFruitlessMoves;
    public double alpha;
    public int numIterations;
    public double improvementAbortionThreshold;
}
