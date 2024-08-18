package org.alshar.kaminpar_shm;

import org.alshar.*;
import org.alshar.common.GraphUtils.Edge;
import org.alshar.common.datastructures.EdgeID;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;

import java.util.Iterator;

public class GraphDelegate {
    protected Graph graph;

    public GraphDelegate(Graph graph) {
        this.graph = graph;
    }

    public boolean initialized() {
        return graph != null;
    }

    public Graph getGraph() {
        return graph;
    }

    // Node weights
    public boolean nodeWeighted() {
        return graph.nodeWeighted();
    }

    public NodeWeight totalNodeWeight() {
        return graph.totalNodeWeight();
    }

    public NodeWeight maxNodeWeight() {
        return graph.maxNodeWeight();
    }

    public NodeWeight nodeWeight(NodeID u) {
        return graph.nodeWeight(u);
    }

    // Edge weights
    public boolean edgeWeighted() {
        return graph.edgeWeighted();
    }

    public EdgeWeight totalEdgeWeight() {
        return graph.totalEdgeWeight();
    }

    public EdgeWeight edgeWeight(EdgeID e) {
        return graph.edgeWeight(e);
    }

    // Graph properties
    public NodeID n() {
        return graph.n();
    }

    public EdgeID m() {
        return graph.m();
    }

    // Low-level graph structure
    public NodeID edgeTarget(EdgeID e) {
        return graph.edgeTarget(e);
    }

    public NodeID degree(NodeID u) {
        return graph.degree(u);
    }

    public EdgeID firstEdge(NodeID u) {
        return graph.firstEdge(u);
    }

    public EdgeID firstInvalidEdge(NodeID u) {
        return graph.firstInvalidEdge(u);
    }


    public Iterable<NodeID> nodes() {
        return () -> new Iterator<NodeID>() {
            private int current = 0;

            @Override
            public boolean hasNext() {
                return current < graph.n().value;
            }

            @Override
            public NodeID next() {
                return new NodeID(current++);
            }
        };
    }

    public Iterable<EdgeID> edges() {
        return () -> new Iterator<EdgeID>() {
            private int current = 0;

            @Override
            public boolean hasNext() {
                return current < graph.m().value;
            }

            @Override
            public EdgeID next() {
                return new EdgeID(current++);
            }
        };
    }
    public Iterable<EdgeID> incidentEdges(NodeID u) {
        return () -> new Iterator<EdgeID>() {
            private int current = graph.firstEdge(u).value;
            private final int last = graph.firstInvalidEdge(u).value;

            @Override
            public boolean hasNext() {
                return current < last;
            }

            @Override
            public EdgeID next() {
                return new EdgeID(current++);
            }
        };
    }
    public Iterable<NodeID> adjacentNodes(NodeID u) {
        return () -> new Iterator<NodeID>() {
            private int current = graph.firstEdge(u).value;
            private final int last = graph.firstInvalidEdge(u).value;

            @Override
            public boolean hasNext() {
                return current < last;
            }

            @Override
            public NodeID next() {
                return graph.edgeTarget(new EdgeID(current++));
            }
        };
    }

    // Degree buckets
    public NodeID bucketSize(int bucket) {
        return graph.bucketSize(bucket);
    }

    public NodeID firstNodeInBucket(int bucket) {
        return graph.firstNodeInBucket(bucket);
    }

    public NodeID firstInvalidNodeInBucket(int bucket) {
        return graph.firstInvalidNodeInBucket(bucket);
    }

    public int numberOfBuckets() {
        return graph.numberOfBuckets();
    }

    public boolean sorted() {
        return graph.sorted();
    }
    public Iterable<Edge> neighbors(NodeID u) {
        return graph.neighbors(u);
    }
}
