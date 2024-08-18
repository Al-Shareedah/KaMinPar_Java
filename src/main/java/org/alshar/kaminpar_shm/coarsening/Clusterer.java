package org.alshar.kaminpar_shm.coarsening;

import org.alshar.Graph;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;


public abstract class Clusterer {
    public Clusterer() {
        // Default constructor
    }

    public Clusterer(Clusterer other) {
        // Deleted copy constructor equivalent
        throw new UnsupportedOperationException("Copy constructor is not supported.");
    }

    public Clusterer(Clusterer other, boolean isMoveConstructor) {
        // Deleted move constructor equivalent
        throw new UnsupportedOperationException("Move constructor is not supported.");
    }

    // Optional options
    public void setMaxClusterWeight(NodeWeight weight) {
        // Default implementation
    }

    public void setDesiredClusterCount(NodeID count) {
        // Default implementation
    }

    // Clustering function
    public abstract int[] computeClustering(Graph graph);
}
