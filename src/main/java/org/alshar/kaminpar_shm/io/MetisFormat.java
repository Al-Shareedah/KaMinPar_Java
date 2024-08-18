package org.alshar.kaminpar_shm.io;

public class MetisFormat {
    public long numberOfNodes;
    public long numberOfEdges;
    public boolean hasNodeWeights;
    public boolean hasEdgeWeights;

    public MetisFormat(long numberOfNodes, long numberOfEdges, boolean hasNodeWeights, boolean hasEdgeWeights) {
        this.numberOfNodes = numberOfNodes;
        this.numberOfEdges = numberOfEdges;
        this.hasNodeWeights = hasNodeWeights;
        this.hasEdgeWeights = hasEdgeWeights;
    }
}
