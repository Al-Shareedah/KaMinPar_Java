package org.alshar.common.datastructures;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Function;
import java.util.ArrayList;
import java.util.List;

public class LocalRatingMap {
    private final int maxSize;
    private final MapType selectedMap;
    private final FixedSizeSparseMap<Integer, Long> smallMap;
    private final FastResetArray<Long> superSmallMap;
    private final FastResetArray<Long> largeMap;

    public enum MapType {
        SUPER_SMALL, SMALL, LARGE
    }

    public LocalRatingMap(int maxSize) {
        this.maxSize = maxSize;
        this.superSmallMap = new FastResetArray<>(128);
        this.smallMap = new FixedSizeSparseMap<>(0L, 32768); // Initialize with a default value of 0L
        this.largeMap = new FastResetArray<>(maxSize);
        this.selectedMap = selectMap(maxSize);
    }

    private MapType selectMap(int size) {
        if (size < 128) {
            return MapType.SUPER_SMALL;
        } else if (size < 32768) {
            return MapType.SMALL;
        } else {
            return MapType.LARGE;
        }
    }

    public Pair<Integer, Long> execute(int upperBound, Function<MapType, Pair<Integer, Long>> function) {
        switch (this.selectedMap) {
            case SUPER_SMALL:
                return function.apply(MapType.SUPER_SMALL);
            case SMALL:
                return function.apply(MapType.SMALL);
            case LARGE:
                return function.apply(MapType.LARGE);
            default:
                throw new IllegalStateException("Unexpected value: " + this.selectedMap);
        }
    }

    public void set(MapType mapType, int key, long value) {
        switch (mapType) {
            case SUPER_SMALL:
                superSmallMap.set(key, value);
                break;
            case SMALL:
                smallMap.put(key, value); // Updates the value in the map
                break;
            case LARGE:
                largeMap.set(key, value);
                break;
        }
    }

    public long get(MapType mapType, int key) {
        switch (mapType) {
            case SUPER_SMALL:
                return superSmallMap.exists(key) ? superSmallMap.get(key) : 0L;
            case SMALL:
                return smallMap.contains(key) ? smallMap.get(key) : 0L;
            case LARGE:
                return largeMap.exists(key) ? largeMap.get(key) : 0L;
            default:
                throw new IllegalStateException("Unexpected value: " + mapType);
        }
    }

    public List<Integer> getEntries(MapType mapType) {
        switch (mapType) {
            case SUPER_SMALL:
                return superSmallMap.usedEntryIds();
            case SMALL:
                return smallMap.getEntries(); // Use the newly added method
            case LARGE:
                return largeMap.usedEntryIds();
            default:
                throw new IllegalStateException("Unexpected value: " + mapType);
        }
    }

    public void clear(MapType mapType) {
        switch (mapType) {
            case SUPER_SMALL:
                superSmallMap.clear();
                break;
            case SMALL:
                smallMap.clear();
                break;
            case LARGE:
                largeMap.clear();
                break;
        }
    }
}
