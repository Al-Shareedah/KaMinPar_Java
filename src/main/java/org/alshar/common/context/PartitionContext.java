package org.alshar.common.context;

import org.alshar.Graph;
import org.alshar.common.datastructures.*;
import org.alshar.kaminpar_shm.kaminpar;

public class PartitionContext {
    public double epsilon;
    public BlockID k;
    public NodeID n = new NodeID(kaminpar.kInvalidNodeID);
    public EdgeID m = new EdgeID(kaminpar.kInvalidEdgeID);
    public NodeWeight totalNodeWeight = kaminpar.kInvalidNodeWeight;
    public EdgeWeight totalEdgeWeight = kaminpar.kInvalidEdgeWeight;
    public NodeWeight maxNodeWeight = kaminpar.kInvalidNodeWeight;

    public BlockWeightsContext blockWeights = new BlockWeightsContext();

    void setupBlockWeights() {
        blockWeights.setup(this);
    }

    public void setup(Graph graph) {
        n = new NodeID(graph.n().value);
        m = new EdgeID(graph.m().value);
        totalNodeWeight = new NodeWeight(graph.totalNodeWeight().value);
        totalEdgeWeight = new EdgeWeight(graph.totalEdgeWeight().value);
        maxNodeWeight = new NodeWeight(graph.maxNodeWeight().value);
        setupBlockWeights();
    }
    public PartitionContext getPartition() {
        return this;
    }

}
