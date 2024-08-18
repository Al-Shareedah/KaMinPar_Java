package org.alshar.common.context;

public class InitialPartitioningContext {
    public InitialCoarseningContext coarsening;
    public InitialRefinementContext refinement;
    public double repetitionMultiplier;
    public int minNumRepetitions;
    public int minNumNonAdaptiveRepetitions;
    public int maxNumRepetitions;
    public int numSeedIterations;
    public boolean useAdaptiveBipartitionerSelection;
}
