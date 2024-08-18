package org.alshar.common.GraphUtils;

import org.alshar.common.datastructures.EdgeID;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.datastructures.NodeID;

public class Edge {
    public EdgeID e;
    public NodeID v;
    public EdgeWeight weight;
    public Edge(EdgeID e, NodeID v) {
        this.e = e;
        this.v = v;
    }
    public Edge(EdgeID e, NodeID v, EdgeWeight weight) {
        this.e = e;
        this.v = v;
        this.weight = weight; // Set the edge weight
    }

    public EdgeID getEdgeID() {
        return e;
    }

    public NodeID getTarget() {
        return v;
    }
    public EdgeWeight getWeight() { // Add this method
        return weight;
    }
}

