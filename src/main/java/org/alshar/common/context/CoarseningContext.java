package org.alshar.common.context;

import org.alshar.common.enums.ClusterWeightLimit;
import org.alshar.common.enums.ClusteringAlgorithm;

public class CoarseningContext {
    public ClusteringAlgorithm algorithm;
    public LabelPropagationCoarseningContext lp;
    public int contractionLimit;
    public boolean enforceContractionLimit;
    public double convergenceThreshold;
    public ClusterWeightLimit clusterWeightLimit;
    public double clusterWeightMultiplier;

    public boolean coarseningShouldConverge(int oldN, int newN) {
        return (1.0 - 1.0 * newN / oldN) <= convergenceThreshold;
    }
}