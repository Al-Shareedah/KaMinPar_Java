package org.alshar.common.datastructures;
import java.util.*;
import java.util.function.Consumer;

public class RatingMap<Key, Value> {
    private enum MapType {
        SUPER_SMALL, SMALL, LARGE
    }

    private static final int SUPER_SMALL_MAP_SIZE = 128;
    private static final int SMALL_MAP_SIZE = 256;

    private MapType selectedMap = MapType.SMALL;
    private int maxSize;

    private Map<Key, Value> superSmallMap = new HashMap<>(SUPER_SMALL_MAP_SIZE);
    private Map<Key, Value> smallMap = new HashMap<>(SMALL_MAP_SIZE);
    private Map<Key, Value> largeMap = new HashMap<>();

    private int superSmallMapCounter = 0;
    private int smallMapCounter = 0;
    private int largeMapCounter = 0;

    public RatingMap(int maxSize) {
        this.maxSize = maxSize;
    }

    public MapType updateUpperBound(int upperBoundSize) {
        selectMap(upperBoundSize);
        return selectedMap;
    }

    public void execute(int upperBound, Consumer<Map<Key, Value>> lambda) {
        updateUpperBound(upperBound);

        switch (selectedMap) {
            case SUPER_SMALL:
                lambda.accept(superSmallMap);
                break;
            case SMALL:
                lambda.accept(smallMap);
                break;
            case LARGE:
                lambda.accept(largeMap);
                break;
        }
    }

    public int getSuperSmallMapCounter() {
        return superSmallMapCounter;
    }

    public int getSmallMapCounter() {
        return smallMapCounter;
    }

    public int getLargeMapCounter() {
        return largeMapCounter;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    private void selectMap(int upperBoundSize) {
        if (upperBoundSize < SUPER_SMALL_MAP_SIZE) {
            selectedMap = MapType.SUPER_SMALL;
            superSmallMapCounter++;
        } else if (maxSize < SMALL_MAP_SIZE || upperBoundSize > SMALL_MAP_SIZE / 3) {
            selectedMap = MapType.LARGE;
            largeMapCounter++;
        } else {
            selectedMap = MapType.SMALL;
            smallMapCounter++;
        }
    }

    public void clear() {
        superSmallMap.clear();
        smallMap.clear();
        largeMap.clear();
    }
}
