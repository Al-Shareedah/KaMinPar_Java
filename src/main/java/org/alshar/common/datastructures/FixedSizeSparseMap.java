package org.alshar.common.datastructures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FixedSizeSparseMap<Key, Value> {
    private static final int MAP_SIZE = 32768;
    private static final int EMPTY_TIMESTAMP = 0;

    private int mapSize;
    private Value initialValue;
    private Object[] dense;
    private Object[] sparse;
    private int size;
    private int timestamp;

    // Represents an element in the sparse map
    private class Element {
        Key key;
        Value value;

        Element(Key key, Value value) {
            this.key = key;
            this.value = value;
        }
    }

    // Represents an entry in the sparse array used for collision resolution
    private class SparseElement {
        Element element;
        int timestamp;

        SparseElement() {
            this.element = null;
            this.timestamp = EMPTY_TIMESTAMP;
        }
    }

    public FixedSizeSparseMap(Value initialValue) {
        this(initialValue, MAP_SIZE);
    }

    public FixedSizeSparseMap(Value initialValue, int customSize) {
        this.mapSize = alignToNextPowerOfTwo(customSize);
        this.initialValue = initialValue;
        this.dense = new Object[mapSize];
        this.sparse = new Object[mapSize];
        for (int i = 0; i < mapSize; i++) {
            sparse[i] = new SparseElement();
        }
        this.size = 0;
        this.timestamp = 1;
    }

    public int capacity() {
        return mapSize;
    }

    public int size() {
        return size;
    }

    public boolean contains(Key key) {
        SparseElement s = find(key);
        return containsValidElement(key, s);
    }

    public Value get(Key key) {
        SparseElement s = find(key);
        if (containsValidElement(key, s)) {
            return s.element.value;
        }
        return initialValue; // Return the initial value if the key is not found
    }

    public void clear() {
        size = 0;
        timestamp++;
    }

    public void setMaxSize(int newSize) {
        if (newSize > mapSize) {
            freeInternalData();
            allocate(newSize);
        }
    }

    public Value put(Key key, Value value) {
        SparseElement s = find(key);
        if (containsValidElement(key, s)) {
            s.element.value = value;
            return s.element.value;
        } else {
            return addElement(key, value, s);
        }
    }

    public void freeInternalData() {
        size = 0;
        timestamp = 0;
        dense = null;
        sparse = null;
    }

    private SparseElement find(Key key) {
        int hash = key.hashCode() & (mapSize - 1);
        while (((SparseElement) sparse[hash]).timestamp == timestamp) {
            if (((SparseElement) sparse[hash]).element.key.equals(key)) {
                return (SparseElement) sparse[hash];
            }
            hash = (hash + 1) & (mapSize - 1);
        }
        return (SparseElement) sparse[hash];
    }

    private boolean containsValidElement(Key key, SparseElement s) {
        return s.timestamp == timestamp && s.element != null && s.element.key.equals(key);
    }

    private Value addElement(Key key, Value value, SparseElement s) {
        dense[size] = new Element(key, value);
        s.element = (Element) dense[size];
        s.timestamp = timestamp;
        size++;
        return value;
    }

    private void allocate(int size) {
        mapSize = alignToNextPowerOfTwo(size);
        dense = new Object[mapSize];
        sparse = new Object[mapSize];
        for (int i = 0; i < mapSize; i++) {
            sparse[i] = new SparseElement();
        }
        size = 0;
        timestamp = 1;
    }

    private int alignToNextPowerOfTwo(int size) {
        return (int) Math.pow(2, Math.ceil(Math.log(size) / Math.log(2)));
    }

    public List<Key> getEntries() {
        List<Key> keys = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Element element = (Element) dense[i];
            if (element != null) {
                keys.add(element.key);
            }
        }
        return keys;
    }

    public List<Element> getKeyValuePairs() {
        List<Element> entries = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Element element = (Element) dense[i];
            if (element != null) {
                entries.add(element);
            }
        }
        return entries;
    }
}
