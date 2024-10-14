package org.alshar.kaminpar_shm.refinement;

import org.alshar.common.datastructures.NodeID;

import java.util.*;

public class DynamicBinaryMinMaxForest<ID, Key extends Comparable<Key>> {

    private final DynamicBinaryMaxForest<ID, Key> maxForest;
    private final DynamicBinaryMinForest<ID, Key> minForest;

    public DynamicBinaryMinMaxForest(int capacity, int heaps) {
        this.maxForest = new DynamicBinaryMaxForest<>(capacity, heaps);
        this.minForest = new DynamicBinaryMinForest<>(capacity, heaps);
    }

    public void checkInconsistencies() {
        // Get the actual number of heaps from the size of the heaps list
        int heapCount = Math.min(maxForest.heapsCount(), minForest.heapsCount());  // Safeguard if maxForest and minForest sizes differ

        for (int heap = 0; heap < heapCount; heap++) {
            // Get the elements for the current heap in maxForest and minForest
            List<HeapElement<ID, Key>> maxElements = maxForest.elements(heap);
            List<HeapElement<ID, Key>> minElements = minForest.elements(heap);

            // Create sets for IDs in each forest for easy comparison
            Set<ID> maxForestIDs = new HashSet<>();
            Set<ID> minForestIDs = new HashSet<>();

            for (HeapElement<ID, Key> element : maxElements) {
                maxForestIDs.add(element.id);
            }

            for (HeapElement<ID, Key> element : minElements) {
                minForestIDs.add(element.id);
            }

            // Compare the two sets to find missing IDs in either forest
            Set<ID> missingInMax = new HashSet<>(minForestIDs);
            missingInMax.removeAll(maxForestIDs);  // IDs in minForest but not in maxForest

            Set<ID> missingInMin = new HashSet<>(maxForestIDs);
            missingInMin.removeAll(minForestIDs);  // IDs in maxForest but not in minForest

            if (!missingInMax.isEmpty() || !missingInMin.isEmpty()) {
                System.out.println("Inconsistencies found in heap " + heap + ":");
                if (!missingInMax.isEmpty()) {
                    System.out.println("  Missing in maxForest: " + missingInMax);
                }
                if (!missingInMin.isEmpty()) {
                    System.out.println("  Missing in minForest: " + missingInMin);
                }
            }

            // Now check if the keys for matching IDs are consistent between the two forests
            for (ID id : maxForestIDs) {
                if (minForestIDs.contains(id)) {
                    Key maxKey = maxForest.key(heap, id);
                    Key minKey = minForest.key(heap, id);

                    if (!maxKey.equals(minKey)) {
                        System.out.println("Inconsistent key for ID " + id + " in heap " + heap + ":");
                        System.out.println("  maxForest key: " + maxKey);
                        System.out.println("  minForest key: " + minKey);
                    }
                }
            }
        }
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

    public void push(int heap, NodeID id, Key key) {
        maxForest.push(heap, (ID) id, key);
        minForest.push(heap, (ID) id, key);
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
    // Change from List<Integer> to Map<ID, Integer>
    private final Map<ID, Integer> idPos;  // Map to store the position of each ID in the heap

    public DynamicBinaryMaxForest(int capacity, int numHeaps) {
        this.heaps = new ArrayList<>(numHeaps);
        this.idPos = new HashMap<>(capacity);  // Initialize as a HashMap
        for (int i = 0; i < numHeaps; i++) {
            heaps.add(new ArrayList<>());
        }
    }

    public int capacity() {
        return idPos.size();
    }
    public int heapsCount() {
        return heaps.size();
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
        idPos.put(id, heapList.size() - 1);  // Use the map to store the position
        siftUp(heap, heapList.size() - 1);
    }

    public void changePriority(int heap, ID id, Key key) {
        int pos = idPos.get(id);  // Get the position using the map
        Key currentKey = heaps.get(heap).get(pos).key;
        if (key.compareTo(currentKey) > 0) {
            increasePriority(heap, id, key);
        } else if (key.compareTo(currentKey) < 0) {
            decreasePriority(heap, id, key);
        }
    }

    public Key key(int heap, ID id) {
        int pos = idPos.get(id);  // Use the map to get the position
        return heaps.get(heap).get(pos).key;
    }

    public boolean contains(ID id) {
        return idPos.containsKey(id);  // Use the map to check existence
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
            idPos.put(lastElement.id, 0);  // Update the position in the map
            idPos.remove(heapList.get(0).id);  // Remove the old position from the map
            heapList.set(0, lastElement);
            heapList.remove(heapList.size() - 1);
            siftDown(heap, 0);
        }
    }

    public void clear(int heap) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        for (HeapElement<ID, Key> element : heapList) {
            idPos.remove(element.id);  // Remove the ID from the map
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
        int pos = idPos.get(id);  // Use the map to get the position
        heaps.get(heap).get(pos).key = newKey;
        siftUp(heap, pos);
    }

    private void decreasePriority(int heap, ID id, Key newKey) {
        int pos = idPos.get(id);  // Use the map to get the position
        heaps.get(heap).get(pos).key = newKey;
        siftDown(heap, pos);
    }

    private void swap(int heap, int pos1, int pos2) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        HeapElement<ID, Key> temp = heapList.get(pos1);
        heapList.set(pos1, heapList.get(pos2));
        heapList.set(pos2, temp);

        // Update positions in the map
        idPos.put(heapList.get(pos1).id, pos1);
        idPos.put(heapList.get(pos2).id, pos2);
    }
}


class DynamicBinaryMinForest<ID, Key extends Comparable<Key>> {

    private final List<List<HeapElement<ID, Key>>> heaps;
    // Change from List<Integer> to Map<ID, Integer>
    private final Map<ID, Integer> idPos;  // Map to store the position of each ID in the heap

    public DynamicBinaryMinForest(int capacity, int numHeaps) {
        this.heaps = new ArrayList<>(numHeaps);
        this.idPos = new HashMap<>(capacity);  // Initialize as a HashMap
        for (int i = 0; i < numHeaps; i++) {
            heaps.add(new ArrayList<>());
        }
    }

    public int capacity() {
        return idPos.size();
    }
    public int heapsCount() {
        return heaps.size();
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
        idPos.put(id, heapList.size() - 1);  // Use the map to store the position
        siftUp(heap, heapList.size() - 1);
    }

    public void changePriority(int heap, ID id, Key key) {
        int pos = idPos.get(id);  // Get the position using the map
        Key currentKey = heaps.get(heap).get(pos).key;
        if (key.compareTo(currentKey) < 0) {
            decreasePriority(heap, id, key);
        } else if (key.compareTo(currentKey) > 0) {
            increasePriority(heap, id, key);
        }
    }

    public Key key(int heap, ID id) {
        int pos = idPos.get(id);  // Use the map to get the position
        return heaps.get(heap).get(pos).key;
    }

    public boolean contains(ID id) {
        return idPos.containsKey(id);  // Use the map to check existence
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
            // Step 1: Set the position of the back element to 0
            HeapElement<ID, Key> backElement = heapList.get(heapList.size() - 1);
            idPos.put(backElement.id, 0);  // Set the position of the back element to 0

            // Step 2: Set the position of the front element to an invalid value (e.g., -1 or null)
            HeapElement<ID, Key> frontElement = heapList.get(0);
            idPos.put(frontElement.id, -1);  // Mark the front element as invalid (use null or a special value)

            // Step 3: Swap the front element with the back element
            heapList.set(0, backElement);
            heapList.set(heapList.size() - 1, frontElement);  // Optional, but makes the next step clear

            // Step 4: Remove the back element
            heapList.remove(heapList.size() - 1);

            // Step 5: Sift down from the front to restore the heap property
            siftDown(heap, 0);
        }
    }

    public void clear(int heap) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        for (HeapElement<ID, Key> element : heapList) {
            idPos.remove(element.id);  // Remove the ID from the map
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
        int pos = idPos.get(id);  // Use the map to get the position
        heaps.get(heap).get(pos).key = newKey;
        siftDown(heap, pos);
    }

    private void decreasePriority(int heap, ID id, Key newKey) {
        int pos = idPos.get(id);  // Use the map to get the position
        heaps.get(heap).get(pos).key = newKey;
        siftUp(heap, pos);
    }

    private void swap(int heap, int pos1, int pos2) {
        List<HeapElement<ID, Key>> heapList = heaps.get(heap);
        HeapElement<ID, Key> temp = heapList.get(pos1);
        heapList.set(pos1, heapList.get(pos2));
        heapList.set(pos2, temp);

        // Update positions in the map
        idPos.put(heapList.get(pos1).id, pos1);
        idPos.put(heapList.get(pos2).id, pos2);
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

