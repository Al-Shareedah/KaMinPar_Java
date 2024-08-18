package org.alshar.common.context;

import org.alshar.common.enums.IsolatedNodesClusteringStrategy;
import org.alshar.common.enums.TwoHopStrategy;

public class LabelPropagationCoarseningContext {
    public int numIterations;
    public int largeDegreeThreshold;
    public int maxNumNeighbors;
    public TwoHopStrategy twoHopStrategy;
    public double twoHopThreshold;
    public IsolatedNodesClusteringStrategy isolatedNodesStrategy;
    public boolean useActiveSetStrategy = true; // default to true
    public boolean useLocalActiveSetStrategy = false; // default to false
    public boolean useTwoHopClustering = true; // default to false
    public boolean trackClusterCount = true;
}