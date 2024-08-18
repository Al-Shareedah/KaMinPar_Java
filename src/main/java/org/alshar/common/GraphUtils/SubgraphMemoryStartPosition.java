package org.alshar.common.GraphUtils;

public class SubgraphMemoryStartPosition {
    public long nodesStartPos = 0;
    public long edgesStartPos = 0;

    public SubgraphMemoryStartPosition add(SubgraphMemoryStartPosition other) {
        this.nodesStartPos += other.nodesStartPos;
        this.edgesStartPos += other.edgesStartPos;
        return this;
    }

    public SubgraphMemoryStartPosition plus(SubgraphMemoryStartPosition other) {
        return new SubgraphMemoryStartPosition(this.nodesStartPos + other.nodesStartPos, this.edgesStartPos + other.edgesStartPos);
    }

    public SubgraphMemoryStartPosition(long nodesStartPos, long edgesStartPos) {
        this.nodesStartPos = nodesStartPos;
        this.edgesStartPos = edgesStartPos;
    }
}
