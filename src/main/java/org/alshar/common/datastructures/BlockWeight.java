package org.alshar.common.datastructures;

public class BlockWeight extends NodeWeight {
    public BlockWeight(long value) {
        super(value);
    }
    @Override
    public BlockWeight add(NodeWeight other) {
        return new BlockWeight(this.value + other.value);
    }
    public BlockWeight subtract(NodeWeight other) {
        return new BlockWeight(this.value - other.value);
    }
}
