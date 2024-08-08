package org.alshar.common;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class StaticArray<T> implements Iterable<T> {
    private Object[] array;
    private int size;

    public StaticArray(int size) {
        this.array = new Object[size];
        this.size = size;
    }

    public StaticArray(T[] initialArray) {
        this.array = Arrays.copyOf(initialArray, initialArray.length);
        this.size = initialArray.length;
    }
    public StaticArray(StaticArray<T> other) {
        this.array = Arrays.copyOf(other.array, other.size);
        this.size = other.size;
    }

    public T get(int index) {
        checkIndex(index);
        return (T) array[index];
    }

    public void set(int index, T value) {
        checkIndex(index);
        array[index] = value;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public Object[] getArray() {
        return array;
    }

    public void resize(int newSize) {
        array = Arrays.copyOf(array, newSize);
        size = newSize;
    }

    public void resize(int newSize, T initValue) {
        int oldSize = size;
        resize(newSize);
        if (newSize > oldSize) {
            Arrays.fill(array, oldSize, newSize, initValue);
        }
    }

    public void free() {
        array = new Object[0];
        size = 0;
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new StaticArrayIterator();
    }

    private class StaticArrayIterator implements Iterator<T> {
        private int currentIndex = 0;

        @Override
        public boolean hasNext() {
            return currentIndex < size;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return (T) array[currentIndex++];
        }
    }

    public static <T> StaticArray<T> copy(StaticArray<T> original) {
        StaticArray<T> copy = new StaticArray<>(original.size());
        for (int i = 0; i < original.size(); i++) {
            copy.set(i, original.get(i));
        }
        return copy;
    }

    public static <T> StaticArray<T> createFrom(java.util.List<T> list) {
        StaticArray<T> array = new StaticArray<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            array.set(i, list.get(i));
        }
        return array;
    }

    public static <T> StaticArray<AtomicInteger> createAtomicFrom(java.util.List<Integer> list) {
        StaticArray<AtomicInteger> array = new StaticArray<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            array.set(i, new AtomicInteger(list.get(i)));
        }
        return array;
    }

    public static <T> java.util.List<T> release(StaticArray<T> array) {
        java.util.List<T> list = new java.util.ArrayList<>(array.size());
        for (T element : array) {
            list.add(element);
        }
        return list;
    }

    public static java.util.List<Integer> releaseNonAtomic(StaticArray<AtomicInteger> array) {
        java.util.List<Integer> list = new java.util.ArrayList<>(array.size());
        for (AtomicInteger element : array) {
            list.add(element.get());
        }
        return list;
    }
}
