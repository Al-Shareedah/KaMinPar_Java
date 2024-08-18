package org.alshar.common.Math;
import org.alshar.common.datastructures.BlockID;

import java.util.*;

public class MathUtils {

    public static boolean isSquare(int value) {
        int sqrt = (int) Math.sqrt(value);
        return sqrt * sqrt == value;
    }

    public static boolean isPowerOf2(int arg) {
        return arg > 0 && (arg & (arg - 1)) == 0;
    }

    public static int floorLog2(int arg) {
        return Integer.SIZE - Integer.numberOfLeadingZeros(arg) - 1;
    }

    public static int floor2(int arg) {
        return 1 << floorLog2(arg);
    }

    public static int ceilLog2(int arg) {
        return floorLog2(arg) + 1 - ((arg & (arg - 1)) == 0 ? 1 : 0);
    }

    public static int ceil2(int arg) {
        return 1 << ceilLog2(arg);
    }

    public static <E extends Number & Comparable<E>> double percentile(List<E> sortedSequence, double percentile) {
        Objects.requireNonNull(sortedSequence);
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Percentile must be between 0 and 1.");
        }

        int index = (int) Math.ceil(percentile * sortedSequence.size()) - 1;
        return sortedSequence.get(index).doubleValue();
    }

    public static int[] splitIntegral(int value, double ratio) {
        int ceilPart = (int) Math.ceil(value * ratio);
        int floorPart = (int) Math.floor(value * (1.0 - ratio));
        return new int[]{ceilPart, floorPart};
    }
    public static BlockID[] splitIntegral(BlockID blockID) {
        int value = blockID.value;  // Extract the integer value from BlockID
        int[] split = splitIntegral(value, 0.5);
        return new BlockID[]{new BlockID(split[0]), new BlockID(split[1])};  // Wrap the result back into BlockID objects
    }


    public static int[] computeLocalRange(int n, int size, int rank) {
        int chunk = n / size;
        int remainder = n % size;
        int from = rank * chunk + Math.min(rank, remainder);
        int to = Math.min(from + ((rank < remainder) ? chunk + 1 : chunk), n);
        return new int[]{from, to};
    }

    public static int computeLocalRangeRank(int n, int size, int element) {
        if (n <= size) {
            return element;
        }

        int c = n / size;
        int rem = n % size;
        int r0 = (element - rem) / c;
        return (element < rem || r0 < rem) ? element / (c + 1) : r0;
    }

    public static int findInDistribution(int value, List<Integer> distribution) {
        int index = Collections.binarySearch(distribution, value);
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }

    public static int distributeRoundRobin(int n, int size, int element) {
        int[] localRange = computeLocalRange(n, size, computeLocalRangeRank(n, size, element));
        int local = element - localRange[0];
        int owner = computeLocalRangeRank(n, size, element);
        return computeLocalRange(n, size, local % size)[0] + (local / size) * size + owner;
    }

    public static int[] decodeGridPosition(int pos, int numColumns) {
        int i = pos / numColumns;
        int j = pos % numColumns;
        return new int[]{i, j};
    }

    public static int encodeGridPosition(int row, int column, int numColumns) {
        return row * numColumns + column;
    }

    public static <E extends Number & Comparable<E>> E findMin(Collection<E> container) {
        return Collections.min(container);
    }

    public static <E extends Number & Comparable<E>> E findMax(Collection<E> container) {
        return Collections.max(container);
    }

    public static double findMean(Collection<? extends Number> container) {
        return container.stream().mapToDouble(Number::doubleValue).average().orElse(0);
    }

    public static <E extends Number & Comparable<E>> Triple<E, Double, E> findMinMeanMax(Collection<E> container) {
        E min = findMin(container);
        E max = findMax(container);
        double mean = findMean(container);
        return new Triple<>(min, mean, max);
    }

    public static int createMask(int numBits) {
        return (1 << numBits) - 1;
    }

    // Utility class to store triple values
    public static class Triple<L, M, R> {
        public final L left;
        public final M middle;
        public final R right;

        public Triple(L left, M middle, R right) {
            this.left = left;
            this.middle = middle;
            this.right = right;
        }
    }
}