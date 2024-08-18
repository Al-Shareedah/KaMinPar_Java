package org.alshar.kaminpar_shm.refinement;

import java.util.ArrayList;
import java.util.List;

public class DynamicBinaryMinMaxForest<ID, Key extends Comparable<Key>> {

    private final DynamicBinaryMaxForest<ID, Key> maxForest;
    private final DynamicBinaryMinForest<ID, Key> minForest;

    public DynamicBinaryMinMaxForest(int capacity, int heaps) {
        this.maxForest = new DynamicBinaryMaxForest<>(capacity, heaps);
        this.minForest = new DynamicBinaryMinForest<>(capacity, heaps);
    }

    public int capacity() {
        return maxForest.capacity();
    }

    public int size() {
        return maxForest.size();
    }

    public int size(int heap) {
        return maxForest.size(heap);
    }

    public void push(int heap, ID id, Key key) {
        maxForest.push(heap, id, key);
        minForest.push(heap, id, key);
    }

    public void changePriority(int heap, ID id, Key key) {
        maxForest.changePriority(heap, id, key);
        minForest.changePriority(heap, id, key);
    }

    public Key key(int heap, ID id) {
        if (maxForest.contains(id) && minForest.contains(id)) {
            if (maxForest.key(heap, id).equals(minForest.key(heap, id))) {
                return maxForest.key(heap, id);
            } else {
                throw new IllegalStateException("Inconsistent key values in min and max forests.");
            }
        } else {
            throw new IllegalStateException("ID not found in either min or max forest.");
        }
    }

    public boolean contains(ID id) {
        return maxForest.contains(id) && minForest.contains(id);
    }

    public boolean empty(int heap) {
        return maxForest.empty(heap) && minForest.empty(heap);
    }

    public ID peekMinId(int heap) {
        return minForest.peekId(heap);
    }

    public ID peekMaxId(int heap) {
        return maxForest.peekId(heap);
    }

    public Key peekMinKey(int heap) {
        return minForest.peekKey(heap);
    }

    public Key peekMaxKey(int heap) {
        return maxForest.peekKey(heap);
    }

    public void remove(int heap, ID id) {
        maxForest.remove(heap, id);
        minForest.remove(heap, id);
    }

    public void popMin(int heap) {
        ID minId = minForest.peekId(heap);
        maxForest.remove(heap, minId);
        minForest.pop(heap);
    }

    public void popMax(int heap) {
        ID maxId = maxForest.peekId(heap);
        minForest.remove(heap, maxId);
        maxForest.pop(heap);
    }

    public void clear() {
        minForest.clearAllHeaps();
        maxForest.clearAllHeaps();
    }


    public void clear(int heap) {
        minForest.clear(heap);
        maxForest.clear(heap);
    }

    public List<HeapElement<ID, Key>> elements(int heap) {
        return maxForest.elements(heap);
    }

    // Additional helper classes (DynamicBinaryMaxForest and DynamicBinaryMinForest)
    // should be implemented similarly as nested classes or separately.
}

class DynamicBinaryMaxForest<ID, Key extends Comparable<Key>> {

    private final List<List<HeapElement<ID, Key>>> heaps;
    private final List<Integer> idPos;

    public DynamicBinaryMaxForest(int capacity, int numHeaps) {
        this.heaps = new ArrayList<>(numHeaps);
        this.idPos = new ArrayList<>(capacity);
        for (int i = 0; i < numHeaps; i++) {
            heaps.add(new ArrayList<>());
        }
        for (int i = 0; i < capacity; i++) {
            idPos.add(-1); // Initialize with invalid ID position
        }
    }

    public int capacity() {
        return idPos.size();
    }

    public int size() {
        return heaps.stream().mapToInt(List::size).sum();
    }

    public int size(int heap) {
        return heaps.get(heap).size();
    }

    public void push(int heap, ID id, Key key) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        heapList.add(new HeapElement<>(id, key));
        idPos.set((Integer) id, heapList.size() - 1);
        siftUp(heap, heapList.size() - 1);
    }

    public void changePriority(int heap, ID id, Key key) {
        int pos = idPos.get((Integer) id);
        Key currentKey = heaps.get(heap).get(pos).key;
        if (key.compareTo(currentKey) > 0) {
            increasePriority(heap, id, key);
        } else if (key.compareTo(currentKey) < 0) {
            decreasePriority(heap, id, key);
        }
    }

    public Key key(int heap, ID id) {
        int pos = idPos.get((Integer) id);
        return heaps.get(heap).get(pos).key;
    }

    public boolean contains(ID id) {
        return idPos.get((Integer) id) != -1;
    }

    public boolean empty(int heap) {
        return heaps.get(heap).isEmpty();
    }

    public ID peekId(int heap) {
        return heaps.get(heap).get(0).id;
    }

    public Key peekKey(int heap) {
        return heaps.get(heap).get(0).key;
    }

    public void remove(int heap, ID id) {
        decreasePriority(heap, id, heaps.get(heap).get(0).key);
        pop(heap);
    }

    public void pop(int heap) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        if (!heapList.isEmpty()) {
            HeapElement<ID, Key> lastElement = heapList.get(heapList.size() - 1);
            idPos.set((Integer) lastElement.id, 0);
            idPos.set((Integer) heapList.get(0).id, -1);
            heapList.set(0, lastElement);
            heapList.remove(heapList.size() - 1);
            siftDown(heap, 0);
        }
    }

    public void clear(int heap) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        for (HeapElement<ID, Key> element : heapList) {
            idPos.set((Integer) element.id, -1);
        }
        heapList.clear();
    }
    public void clearAllHeaps() {
        for (int i = 0; i < heaps.size(); i++) {
            clear(i);
        }
    }

    public List<HeapElement<ID, Key>> elements(int heap) {
        return heaps.get(heap);
    }

    private void siftUp(int heap, int pos) {
        while (pos > 0) {
            int parent = (pos - 1) / 2;
            if (heaps.get(heap).get(pos).key.compareTo(heaps.get(heap).get(parent).key) > 0) {
                swap(heap, pos, parent);
                pos = parent;
            } else {
                break;
            }
        }
    }

    private void siftDown(int heap, int pos) {
        int size = heaps.get(heap).size();
        while (pos < size / 2) {
            int leftChild = 2 * pos + 1;
            int rightChild = 2 * pos + 2;
            int largest = leftChild;
            if (rightChild < size && heaps.get(heap).get(rightChild).key.compareTo(heaps.get(heap).get(leftChild).key) > 0) {
                largest = rightChild;
            }
            if (heaps.get(heap).get(pos).key.compareTo(heaps.get(heap).get(largest).key) >= 0) {
                break;
            }
            swap(heap, pos, largest);
            pos = largest;
        }
    }

    private void increasePriority(int heap, ID id, Key newKey) {
        int pos = idPos.get((Integer) id);
        heaps.get(heap).get(pos).key = newKey;
        siftUp(heap, pos);
    }

    private void decreasePriority(int heap, ID id, Key newKey) {
        int pos = idPos.get((Integer) id);
        heaps.get(heap).get(pos).key = newKey;
        siftDown(heap, pos);
    }

    private void swap(int heap, int pos1, int pos2) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        HeapElement<ID, Key> temp = heapList.get(pos1);
        heapList.set(pos1, heapList.get(pos2));
        heapList.set(pos2, temp);
        idPos.set((Integer) heapList.get(pos1).id, pos1);
        idPos.set((Integer) heapList.get(pos2).id, pos2);
    }
}

class DynamicBinaryMinForest<ID, Key extends Comparable<Key>> {

    private final List<List<HeapElement<ID, Key>>> heaps;
    private final List<Integer> idPos;

    public DynamicBinaryMinForest(int capacity, int numHeaps) {
        this.heaps = new ArrayList<>(numHeaps);
        this.idPos = new ArrayList<>(capacity);
        for (int i = 0; i < numHeaps; i++) {
            heaps.add(new ArrayList<>());
        }
        for (int i = 0; i < capacity; i++) {
            idPos.add(-1); // Initialize with invalid ID position
        }
    }

    public int capacity() {
        return idPos.size();
    }

    public int size() {
        return heaps.stream().mapToInt(List::size).sum();
    }

    public int size(int heap) {
        return heaps.get(heap).size();
    }

    public void push(int heap, ID id, Key key) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        heapList.add(new HeapElement<>(id, key));
        idPos.set((Integer) id, heapList.size() - 1);
        siftUp(heap, heapList.size() - 1);
    }

    public void changePriority(int heap, ID id, Key key) {
        int pos = idPos.get((Integer) id);
        Key currentKey = heaps.get(heap).get(pos).key;
        if (key.compareTo(currentKey) < 0) {
            decreasePriority(heap, id, key);
        } else if (key.compareTo(currentKey) > 0) {
            increasePriority(heap, id, key);
        }
    }

    public Key key(int heap, ID id) {
        int pos = idPos.get((Integer) id);
        return heaps.get(heap).get(pos).key;
    }

    public boolean contains(ID id) {
        return idPos.get((Integer) id) != -1;
    }

    public boolean empty(int heap) {
        return heaps.get(heap).isEmpty();
    }

    public ID peekId(int heap) {
        return heaps.get(heap).get(0).id;
    }

    public Key peekKey(int heap) {
        return heaps.get(heap).get(0).key;
    }

    public void remove(int heap, ID id) {
        decreasePriority(heap, id, heaps.get(heap).get(0).key);
        pop(heap);
    }

    public void pop(int heap) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        if (!heapList.isEmpty()) {
            HeapElement<ID, Key> lastElement = heapList.get(heapList.size() - 1);
            idPos.set((Integer) lastElement.id, 0);
            idPos.set((Integer) heapList.get(0).id, -1);
            heapList.set(0, lastElement);
            heapList.remove(heapList.size() - 1);
            siftDown(heap, 0);
        }
    }

    public void clear(int heap) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        for (HeapElement<ID, Key> element : heapList) {
            idPos.set((Integer) element.id, -1);
        }
        heapList.clear();
    }

    public void clearAllHeaps() {
        for (int i = 0; i < heaps.size(); i++) {
            clear(i);
        }
    }

    public List<HeapElement<ID, Key>> elements(int heap) {
        return heaps.get(heap);
    }

    private void siftUp(int heap, int pos) {
        while (pos > 0) {
            int parent = (pos - 1) / 2;
            if (heaps.get(heap).get(pos).key.compareTo(heaps.get(heap).get(parent).key) < 0) {
                swap(heap, pos, parent);
                pos = parent;
            } else {
                break;
            }
        }
    }

    private void siftDown(int heap, int pos) {
        int size = heaps.get(heap).size();
        while (pos < size / 2) {
            int leftChild = 2 * pos + 1;
            int rightChild = 2 * pos + 2;
            int smallest = leftChild;
            if (rightChild < size && heaps.get(heap).get(rightChild).key.compareTo(heaps.get(heap).get(leftChild).key) < 0) {
                smallest = rightChild;
            }
            if (heaps.get(heap).get(pos).key.compareTo(heaps.get(heap).get(smallest).key) <= 0) {
                break;
            }
            swap(heap, pos, smallest);
            pos = smallest;
        }
    }

    private void increasePriority(int heap, ID id, Key newKey) {
        int pos = idPos.get((Integer) id);
        heaps.get(heap).get(pos).key = newKey;
        siftDown(heap, pos);
    }

    private void decreasePriority(int heap, ID id, Key newKey) {
        int pos = idPos.get((Integer) id);
        heaps.get(heap).get(pos).key = newKey;
        siftUp(heap, pos);
    }

    private void swap(int heap, int pos1, int pos2) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        HeapElement<ID, Key> temp = heapList.get(pos1);
        heapList.set(pos1, heapList.get(pos2));
        heapList.set(pos2, temp);
        idPos.set((Integer) heapList.get(pos1).id, pos1);
        idPos.set((Integer) heapList.get(pos2).id, pos2);
    }
}

class HeapElement<ID, Key extends Comparable<Key>> {
    ID id;
    Key key;

    HeapElement(ID id, Key key) {
        this.id = id;
        this.key = key;
    }
}

