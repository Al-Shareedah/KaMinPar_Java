package org.alshar.common.GraphUtils;

import org.alshar.common.datastructures.StaticArray;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.EdgeID;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.common.datastructures.EdgeWeight;

public class SubgraphMemory {
    private StaticArray<EdgeID> nodes;
    private StaticArray<NodeID> edges;
    private StaticArray<NodeWeight> nodeWeights;
    private StaticArray<EdgeWeight> edgeWeights;

    public SubgraphMemory() {
        int initialSize = 1; // Adjust initial size as needed
        nodes = new StaticArray<>(initialSize);
        edges = new StaticArray<>(initialSize);
        nodeWeights = new StaticArray<>(initialSize);
        edgeWeights = new StaticArray<>(initialSize);

        // Initialize elements with default values
        for (int i = 0; i < initialSize; i++) {
            nodes.set(i, new EdgeID(0)); // Replace 0 with appropriate default value
            edges.set(i, new NodeID(0));
            nodeWeights.set(i, new NodeWeight(0));
            edgeWeights.set(i, new EdgeWeight(0));
        }
    }

    public SubgraphMemory(NodeID n, BlockID k, EdgeID m, boolean isNodeWeighted, boolean isEdgeWeighted) {
        resize(n, k, m, isNodeWeighted, isEdgeWeighted);
    }

    public SubgraphMemory(PartitionedGraph pGraph) {
        resize(pGraph);
    }

    public void resize(PartitionedGraph pGraph) {
        resize(pGraph.n(), pGraph.k(), pGraph.m(), pGraph.nodeWeighted(), pGraph.edgeWeighted());
    }

    public void resize(NodeID n, BlockID k, EdgeID m, boolean isNodeWeighted, boolean isEdgeWeighted) {
        nodes = new StaticArray<>(n.value + k.value);
        edges = new StaticArray<>(m.value);
        nodeWeights = new StaticArray<>(isNodeWeighted ? n.value + k.value : 0);
        edgeWeights = new StaticArray<>(isEdgeWeighted ? m.value : 0);
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    // Getters for the arrays
    public StaticArray<EdgeID> getNodes() {
        return nodes;
    }

    public StaticArray<NodeID> getEdges() {
        return edges;
    }

    public StaticArray<NodeWeight> getNodeWeights() {
        return nodeWeights;
    }

    public StaticArray<EdgeWeight> getEdgeWeights() {
        return edgeWeights;
    }
}

