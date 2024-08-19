package org.alshar.common.datastructures;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
public class DynamicBinaryHeap<ID, Key extends Comparable<Key>> {
    private static final int kTreeArity = 4;

    private final List<HeapElement<ID, Key>> heap = new ArrayList<>();
    private final Comparator<Key> comparator;

    public DynamicBinaryHeap(Comparator<Key> comparator) {
        this.comparator = comparator;
    }

    public void push(ID id, Key key) {
        heap.add(new HeapElement<>(id, key));
        siftUp(heap.size() - 1);
    }

    public ID peekId() {
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

    public void pop() {
        if (isEmpty()) {
            throw new IllegalStateException("Heap is empty.");
        }
        swap(0, heap.size() - 1);
        heap.remove(heap.size() - 1);
        siftDown(0);
    }

    public void clear() {
        heap.clear();
    }

    public int size() {
        return heap.size();
    }

    public boolean isEmpty() {
        return heap.isEmpty();
    }

    public List<HeapElement<ID, Key>> getElements() {
        return heap;
    }

    private void siftUp(int pos) {
        while (pos != 0) {
            int parent = (pos - 1) / kTreeArity;
            if (comparator.compare(heap.get(parent).key, heap.get(pos).key) > 0) {
                swap(pos, parent);
            }
            pos = parent;
        }
    }

    private void siftDown(int pos) {
        while (true) {
            int firstChild = kTreeArity * pos + 1;
            if (firstChild >= heap.size()) {
                break;
            }

            int smallestChild = firstChild;
            for (int i = 1; i < kTreeArity && firstChild + i < heap.size(); i++) {
                if (comparator.compare(heap.get(firstChild + i).key, heap.get(smallestChild).key) < 0) {
                    smallestChild = firstChild + i;
                }
            }

            if (comparator.compare(heap.get(smallestChild).key, heap.get(pos).key) >= 0) {
                break;
            }

            swap(pos, smallestChild);
            pos = smallestChild;
        }
    }

    private void swap(int pos1, int pos2) {
        HeapElement<ID, Key> temp = heap.get(pos1);
        heap.set(pos1, heap.get(pos2));
        heap.set(pos2, temp);
    }

    public static class HeapElement<ID, Key> {
        public final ID id;
        public final Key key;

        public HeapElement(ID id, Key key) {
            this.id = id;
            this.key = key;
        }
    }
}
