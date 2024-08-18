package org.alshar.common.datastructures;

public class NodeID {
    public int value;

    public NodeID(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    // Method to add an integer to the NodeID's value and return a new NodeID
    public NodeID add(int increment) {
        return new NodeID(this.value + increment);
    }

    // Optional: Override equals and hashCode for proper comparisons
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NodeID nodeID = (NodeID) obj;
        return value == nodeID.value;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public String toString() {
        return "NodeID{" +
                "value=" + value +
                '}';
    }
    // Method to subtract an integer from the NodeID's value and return a new NodeID
    public NodeID subtract(int decrement) {
        return new NodeID(this.value - decrement);
    }


}
