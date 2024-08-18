package org.alshar.common.datastructures;

import java.util.ArrayList;
import java.util.List;

public class FastResetArray<T> {
    private List<T> data;
    private List<Integer> usedEntries;
    // Default capacity
    private static final int DEFAULT_CAPACITY = 10;

    // Default constructor with default capacity
    public FastResetArray() {
        this(DEFAULT_CAPACITY);
    }

    // Constructor to initialize with a specified capacity
    public FastResetArray(int capacity) {
        this.data = new ArrayList<>(capacity);
        this.usedEntries = new ArrayList<>();
        // Initialize the array with null values
        for (int i = 0; i < capacity; i++) {
            data.add(null);
        }
    }

    // Get element by position (non-const equivalent in C++)
    public T get(int pos) {
        return data.get(pos);
    }

    // Set element by position and mark it as used
    public void set(int pos, T newValue) {
        if (data.get(pos) == null || (data.get(pos) instanceof EdgeWeight && ((EdgeWeight) data.get(pos)).value == 0)) {
            usedEntries.add(pos);
        }
        data.set(pos, newValue);
    }

    // Check if an element exists at a given position
    public boolean exists(int pos) {
        return data.get(pos) != null;
    }

    // Get the list of used entry IDs
    public List<Integer> usedEntryIds() {
        return usedEntries;
    }

    // Get the values of used entries
    public List<T> usedEntryValues() {
        List<T> values = new ArrayList<>();
        for (int pos : usedEntries) {
            values.add(data.get(pos));
        }
        return values;
    }

    // Get a list of pairs (index, value) for used entries
    public List<Entry<T>> entries() {
        List<Entry<T>> entries = new ArrayList<>();
        for (int pos : usedEntries) {
            entries.add(new Entry<>(pos, data.get(pos)));
        }
        return entries;
    }

    // Clear the used entries, reset them to 0 instead of null
    @SuppressWarnings("unchecked")
    public void clear() {
        for (int i = 0; i < data.size(); i++) {
            // Set each element to zero
            data.set(i, (T) new EdgeWeight(0));  // Cast to Object first to avoid type issues
        }
        usedEntries.clear();
    }



    // Check if there are any used entries
    public boolean isEmpty() {
        return usedEntries.isEmpty();
    }

    // Get the number of used entries
    public int size() {
        return usedEntries.size();
    }

    // Get the capacity of the array
    public int capacity() {
        return data.size();
    }

    // Resize the array to a new capacity
    public void resize(int capacity) {
        int oldSize = data.size();
        if (capacity > oldSize) {
            for (int i = oldSize; i < capacity; i++) {
                data.add(null);
            }
        } else if (capacity < oldSize) {
            for (int i = oldSize - 1; i >= capacity; i--) {
                data.remove(i);
            }
        }
    }

    // Calculate the memory usage in kilobytes
    public long memoryInKB() {
        return data.size() * Long.BYTES / 1000;
    }

    // Inner class to represent an entry of (index, value)
    public static class Entry<T> {
        public final int index;
        public final T value;

        public Entry(int index, T value) {
            this.index = index;
            this.value = value;
        }
    }
}