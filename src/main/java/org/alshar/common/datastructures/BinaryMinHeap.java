package org.alshar.common.datastructures;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
public class BinaryMinHeap<Key extends Comparable<Key>> {
    private static final int DEFAULT_CAPACITY = 16;
    private final List<HeapElement<Key>> heap;
    private int[] idPos;
    private int size;
    private final Key minValue; // Minimum possible value for Key type

    public static final int INVALID_ID = -1;

    public BinaryMinHeap(int capacity, Key minValue) {
        this.heap = new ArrayList<>(capacity);
        this.idPos = new int[capacity];
        this.size = 0;
        this.minValue = minValue;
        for (int i = 0; i < capacity; i++) {
            idPos[i] = INVALID_ID;
        }
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }
    public int capacity() {
        return heap.size();
    }

    public boolean contains(int id) {
        return idPos[id] != INVALID_ID;
    }

    public Key key(int id) {
        if (!contains(id)) {
            throw new IllegalArgumentException("ID does not exist in the heap.");
        }
        return heap.get(idPos[id]).key;
    }

    public int peekId() {
        if (isEmpty()) {
            throw new IllegalStateException("Heap is empty.");
        }
        return heap.get(0).id;
    }

    public Key peekKey() {
        if (isEmpty()) {
            throw new IllegalStateException("Heap is empty.");
        }
        return heap.get(0).key;
    }

    public void remove(int id) {
        if (!contains(id)) {
            throw new IllegalArgumentException("ID does not exist in the heap.");
        }
        decreasePriority(id, minValue);
        pop();
        if (contains(id)) {
            throw new IllegalStateException("Failed to remove ID from the heap.");
        }
    }


    public void pop() {
        if (isEmpty()) {
            throw new IllegalStateException("Heap is empty.");
        }
        swap(0, size - 1);
        idPos[heap.get(size - 1).id] = INVALID_ID;
        size--;
        siftDown(0);
    }


    public void push(int id, Key key) {
        if (contains(id)) {
            throw new IllegalArgumentException("ID already exists in the heap.");
        }
        if (size >= heap.size()) {
            heap.add(new HeapElement<>(id, key));
        } else {
            heap.set(size, new HeapElement<>(id, key));
        }
        idPos[id] = size;
        size++;
        siftUp(size - 1);
    }

    public void pushOrChangePriority(int id, Key newKey) {
        if (contains(id)) {
            changePriority(id, newKey);
        } else {
            push(id, newKey);
        }
    }

    public void changePriority(int id, Key newKey) {
        if (!contains(id)) {
            throw new IllegalArgumentException("ID does not exist in the heap.");
        }
        Key oldKey = key(id);
        if (newKey.compareTo(oldKey) < 0) {
            decreasePriority(id, newKey);
        } else if (newKey.compareTo(oldKey) > 0) {
            increasePriority(id, newKey);
        }
    }

    public void clear() {
        for (int i = 0; i < size; i++) {
            idPos[heap.get(i).id] = INVALID_ID;
        }
        size = 0;
    }

    public void decreasePriority(int id, Key newKey) {
        if (!contains(id)) {
            throw new IllegalArgumentException("ID does not exist in the heap.");
        }
        if (newKey.compareTo(key(id)) > 0) {
            throw new IllegalArgumentException("New key is greater than the current key.");
        }
        heap.get(idPos[id]).key = newKey;
        siftUp(idPos[id]);
    }


    public void increasePriority(int id, Key newKey) {
        if (!contains(id)) {
            throw new IllegalArgumentException("ID does not exist in the heap.");
        }
        if (newKey.compareTo(key(id)) < 0) {
            throw new IllegalArgumentException("New key is less than the current key.");
        }
        heap.get(idPos[id]).key = newKey;
        siftDown(idPos[id]);
    }

    public void resize(int capacity) {
        if (!isEmpty()) {
            throw new IllegalStateException("Heap should be empty when resizing.");
        }
        int[] newIdPos = new int[capacity];
        System.arraycopy(idPos, 0, newIdPos, 0, Math.min(idPos.length, capacity));
        for (int i = idPos.length; i < capacity; i++) {
            newIdPos[i] = INVALID_ID;
        }
        idPos = newIdPos;
        // Ensure the heap list has the correct size
        while (heap.size() < capacity) {
            heap.add(new HeapElement<>(INVALID_ID, minValue));
        }
        // Resize to the exact capacity (if needed)
        if (heap.size() > capacity) {
            for (int i= heap.size() - 1; i >= capacity; i--) {
                heap.remove(i);
            }
        }
    }

    public long memoryInKB() {
        return (idPos.length * Integer.BYTES + heap.size() * HeapElement.sizeInBytes()) / 1000;
    }

    private void siftUp(int pos) {
        while (pos != 0) {
            int parent= (pos - 1) / 4; // Assuming kTreeArity is 4
            if (heap.get(parent).key.compareTo(heap.get(pos).key) > 0) {
                // Swap elements in the heap
                swap(pos, parent);

            }
            pos = parent; // Update pos to parent for the next iteration
        }
    }


    private void siftDown(int pos) {
        while (true) {
            int firstChild = pos * 4 + 1;
            if (firstChild >= size) {
                break;
            }
            int smallestChild = firstChild;
            for (int i = 1; i < 4 && firstChild + i < size; i++) {
                if (heap.get(firstChild + i).key.compareTo(heap.get(smallestChild).key) < 0) {
                    smallestChild = firstChild + i;
                }
            }
            if (heap.get(smallestChild).key.compareTo(heap.get(pos).key) < 0) {
                swap(pos, smallestChild);
                pos = smallestChild;
            } else {
                break;
            }
        }
    }


    private void swap(int pos1, int pos2) {
        HeapElement<Key> temp = heap.get(pos1);
        heap.set(pos1, heap.get(pos2));
        heap.set(pos2, temp);
        idPos[heap.get(pos1).id] = pos1;
        idPos[heap.get(pos2).id] = pos2;
    }

    private static class HeapElement<Key> {
        int id;
        Key key;

        HeapElement(int id, Key key) {
            this.id = id;
            this.key = key;
        }

        static long sizeInBytes() {
            return Integer.BYTES + Long.BYTES;
        }
    }
}