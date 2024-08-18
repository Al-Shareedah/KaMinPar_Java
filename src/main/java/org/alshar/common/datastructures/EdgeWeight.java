package org.alshar.common.datastructures;

public class EdgeWeight implements Comparable<EdgeWeight> {
    public long value;

    public EdgeWeight(long value) {
        this.value = value;
    }

    public EdgeWeight add(EdgeWeight other) {
        return new EdgeWeight(this.value + other.value);
    }

    public EdgeWeight subtract(EdgeWeight other) {
        return new EdgeWeight(this.value - other.value);
    }

    @Override
    public int compareTo(EdgeWeight other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EdgeWeight that = (EdgeWeight) obj;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    public EdgeWeight negate() {
        return new EdgeWeight(-this.value);
    }
}
