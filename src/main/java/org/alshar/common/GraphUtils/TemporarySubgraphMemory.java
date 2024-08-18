package org.alshar.common.GraphUtils;

import org.alshar.common.datastructures.EdgeID;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;

import java.util.ArrayList;
import java.util.List;

public class TemporarySubgraphMemory {
    private static final double OVERALLOCATION_FACTOR = 1.05;

    private List<EdgeID> nodes = new ArrayList<>();
    private List<NodeID> edges = new ArrayList<>();
    private List<NodeWeight> nodeWeights = new ArrayList<>();
    private List<EdgeWeight> edgeWeights = new ArrayList<>();
    private List<NodeID> mapping = new ArrayList<>();

    private long numNodeReallocs = 0;
    private long numEdgeReallocs = 0;

    public void ensureSizeNodes(NodeID n, boolean isNodeWeighted) {
        if (nodes.size() < n.value + 1) {
            nodes = resizeList(nodes, (int) (n.value * OVERALLOCATION_FACTOR + 1));
            numNodeReallocs++;
        }
        if (isNodeWeighted && nodeWeights.size() < n.value) {
            nodeWeights = resizeList(nodeWeights, (int) (n.value * OVERALLOCATION_FACTOR));
        }
        if (mapping.size() < n.value) {
            mapping = resizeList(mapping, (int) (n.value * OVERALLOCATION_FACTOR));
        }
    }

    public void ensureSizeEdges(EdgeID m, boolean isEdgeWeighted) {
        if (edges.size() < m.value) {
            edges = resizeList(edges, (int) (m.value * OVERALLOCATION_FACTOR));
            numEdgeReallocs++;
        }
        if (isEdgeWeighted && edgeWeights.size() < m.value) {
            edgeWeights = resizeList(edgeWeights, (int) (m.value * OVERALLOCATION_FACTOR));
        }
    }

    private <T> List<T> resizeList(List<T> list, int newSize) {
        while (list.size() < newSize) {
            list.add(null);
        }
        return list;
    }

    public long getNumNodeReallocs() {
        return numNodeReallocs;
    }

    public long getNumEdgeReallocs() {
        return numEdgeReallocs;
    }

    public long memoryInKB() {
        return (nodes.size() * Long.BYTES + edges.size() * Long.BYTES +
                nodeWeights.size() * Long.BYTES + edgeWeights.size() * Long.BYTES +
                mapping.size() * Long.BYTES) / 1000;
    }

    // Getters for the temporary memory lists
    public List<EdgeID> getNodes() {
        return nodes;
    }

    public List<NodeID> getEdges() {
        return edges;
    }

    public List<NodeWeight> getNodeWeights() {
        return nodeWeights;
    }

    public List<EdgeWeight> getEdgeWeights() {
        return edgeWeights;
    }

    public List<NodeID> getMapping() {
        return mapping;
    }
}
