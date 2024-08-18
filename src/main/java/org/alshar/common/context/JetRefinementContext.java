package org.alshar.common.context;

import org.alshar.common.enums.RefinementAlgorithm;

public class JetRefinementContext {
    public int numIterations;
    public int numFruitlessIterations;
    public double fruitlessThreshold;
    public double fineNegativeGainFactor;
    public double coarseNegativeGainFactor;
    public RefinementAlgorithm balancingAlgorithm;
}
