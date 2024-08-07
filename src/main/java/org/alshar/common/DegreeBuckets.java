package org.alshar.common;

public class DegreeBuckets {

    public static final int kNumberOfDegreeBuckets = Long.SIZE + 1;

    public static int lowestDegreeInBucket(int bucket) {
        return (1 << bucket) >> 1;
    }

    public static int degreeBucket(long degree) {
        return (degree == 0) ? 0 : floorLog2(degree) + 1;
    }

    private static int floorLog2(long value) {
        return 63 - Long.numberOfLeadingZeros(value);
    }

    public static void main(String[] args) {
        // Test the utility functions
        System.out.println("Number of degree buckets: " + kNumberOfDegreeBuckets);
        System.out.println("Lowest degree in bucket 5: " + lowestDegreeInBucket(5));
        System.out.println("Degree bucket for degree 32: " + degreeBucket(32));
    }
}

