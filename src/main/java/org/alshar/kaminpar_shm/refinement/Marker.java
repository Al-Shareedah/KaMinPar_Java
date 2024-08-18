package org.alshar.kaminpar_shm.refinement;

import org.alshar.common.datastructures.NodeID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Marker {
    private List<Integer> data;
    private int markerId;
    private int[] firstUnmarkedElement;

    public Marker(int capacity, int numConcurrentMarkers) {
        this.data = new ArrayList<>(Arrays.asList(new Integer[capacity]));
        this.markerId = 0;
        this.firstUnmarkedElement = new int[numConcurrentMarkers];
        Arrays.fill(firstUnmarkedElement, 0);
    }

    public void set(int element, int marker, boolean trackFirstUnmarkedElement) {
        if (marker >= firstUnmarkedElement.length || element >= data.size()) {
            throw new IllegalArgumentException("Marker or element out of bounds");
        }

        int currentMarker = data.get(element) == null ? 0 : data.get(element);
        if ((currentMarker & ~((1 << firstUnmarkedElement.length) - 1)) == markerId) {
            currentMarker |= (1 << marker);
        } else {
            currentMarker = markerId | (1 << marker);
        }
        data.set(element, currentMarker);

        if (trackFirstUnmarkedElement) {
            while (firstUnmarkedElement[marker] < data.size() && get(firstUnmarkedElement[marker], marker)) {
                firstUnmarkedElement[marker]++;
            }
        }
    }

    public int firstUnmarkedElement(int marker) {
        if (marker >= firstUnmarkedElement.length) {
            throw new IllegalArgumentException("Marker out of bounds");
        }
        return firstUnmarkedElement[marker];
    }

    public boolean get(int element, int marker) {
        if (marker >= firstUnmarkedElement.length || element >= data.size()) {
            throw new IllegalArgumentException("Marker or element out of bounds");
        }
        int currentMarker = data.get(element) == null ? 0 : data.get(element);
        return ((currentMarker & ~((1 << firstUnmarkedElement.length) - 1)) == markerId) &&
                ((currentMarker & (1 << marker)) != 0);
    }
    public boolean get(NodeID element) {
        return get(element.value, 0);
    }

    public int size() {
        return data.size();
    }

    public int capacity() {
        return size();
    }

    public void reset() {
        markerId |= (1 << firstUnmarkedElement.length) - 1;
        markerId += 1;
        Arrays.fill(firstUnmarkedElement, 0);

        if ((markerId | ((1 << firstUnmarkedElement.length) - 1)) == Integer.MAX_VALUE) {
            markerId = 0;
            int capacity = data.size();
            data.clear();
            for (int i = 0; i < capacity; i++) {
                data.add(0);
            }
        }
    }

    public void resize(int capacity) {
        data = new ArrayList<>(Arrays.asList(new Integer[capacity]));
    }

    public int memoryInKB() {
        return data.size() * Integer.BYTES / 1000;
    }
}
