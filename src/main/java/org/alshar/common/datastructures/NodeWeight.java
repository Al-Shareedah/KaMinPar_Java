package org.alshar.common.datastructures;

public class NodeWeight implements Comparable<NodeWeight> {
    public long value;

    public NodeWeight(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
    // Static max method
    public static NodeWeight max(NodeWeight a, NodeWeight b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public NodeWeight add(NodeWeight other) {
        return new NodeWeight(this.value + other.value);
    }

    public int compareTo(NodeWeight other) {
        return Long.compare(this.value, other.value);
    }
    public NodeWeight subtract(NodeWeight other) {
        return new NodeWeight(this.value - other.value);
    }
}

