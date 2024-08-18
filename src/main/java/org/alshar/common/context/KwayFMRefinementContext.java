package org.alshar.common.context;

import org.alshar.common.enums.GainCacheStrategy;

public class KwayFMRefinementContext {
    public int numSeedNodes;
    public double alpha;
    public int numIterations;
    public boolean unlockLocallyMovedNodes;
    public boolean unlockSeedNodes;
    public boolean useExactAbortionThreshold;
    public double abortionThreshold;
    public GainCacheStrategy gainCacheStrategy;
    public int constantHighDegreeThreshold;
    public double kBasedHighDegreeThreshold;
    public boolean dbgComputeBatchStats;
}
