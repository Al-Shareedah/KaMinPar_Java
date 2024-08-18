package org.alshar.common.datastructures;

import org.alshar.kaminpar_shm.kaminpar;

import java.util.Objects;

public class BlockID {
    public int value;
    public static final BlockID kInvalidBlockID = new BlockID(-1);
    public BlockID(int value) {
        this.value = value;
    }

    public boolean isInvalid() {
        return this.value == kaminpar.kInvalidBlockID.value;
    }

    public int getValue() {
        return value;
    }
    // Add method for adding another BlockID
    public BlockID add(BlockID other) {
        return new BlockID(this.value + other.value);
    }


    // Add method for incrementing the BlockID value
    public BlockID add(int increment) {
        return new BlockID(this.value + increment);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BlockID blockID = (BlockID) obj;
        return value == blockID.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}

