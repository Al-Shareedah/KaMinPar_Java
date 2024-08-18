package org.alshar.common.datastructures;
import java.util.ArrayList;
import java.util.List;
public class Queue<T> {
    private List<T> data;
    private int head;
    private int tail;

    public Queue(int capacity) {
        this.data = new ArrayList<>(capacity);
        this.head = 0;
        this.tail = 0;
    }

    // Access operators
    public T tail() {
        if (empty()) {
            throw new IllegalStateException("Queue is empty.");
        }
        return data.get(tail - 1);
    }

    public T head() {
        if (empty()) {
            throw new IllegalStateException("Queue is empty.");
        }
        return data.get(head);
    }

    public T get(int pos) {
        if (head + pos >= data.size()) {
            throw new IndexOutOfBoundsException("Index out of bounds.");
        }
        return data.get(head + pos);
    }

    public void set(int pos, T element) {
        if (head + pos >= data.size()) {
            throw new IndexOutOfBoundsException("Index out of bounds.");
        }
        data.set(head + pos, element);
    }

    // Iterators
    public List<T> begin() {
        return data.subList(head, tail);
    }

    public List<T> end() {
        return data.subList(tail, data.size());
    }

    // Capacity
    public boolean empty() {
        return head == tail;
    }

    public int size() {
        return tail - head;
    }

    public int capacity() {
        return data.size();
    }

    public void resize(int capacity) {
        List<T> newData = new ArrayList<>(capacity);
        for (int i = head; i < tail; i++) {
            newData.add(data.get(i));
        }
        data = newData;
        head = 0;
        tail = newData.size();
    }

    // Modification
    public void popHead() {
        if (head >= tail) {
            throw new IllegalStateException("Queue is empty.");
        }
        head++;
    }

    public void pushHead(T element) {
        if (head > 0) {
            data.set(--head, element);
        } else {
            throw new IllegalStateException("No space at the head of the queue.");
        }
    }

    public void pushTail(T element) {
        if (tail < data.size()) {
            data.set(tail++, element);
        } else {
            data.add(element);
            tail++;
        }
    }

    public void popTail() {
        if (head >= tail) {
            throw new IllegalStateException("Queue is empty.");
        }
        tail--;
    }

    public void clear() {
        head = 0;
        tail = 0;
    }

    public long memoryInKB() {
        return (long) data.size() * (Long.BYTES) / 1000;
    }
}
