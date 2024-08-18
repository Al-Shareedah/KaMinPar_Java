package org.alshar.common.GraphUtils;

import org.alshar.common.datastructures.StaticArray;

import java.util.concurrent.atomic.AtomicInteger;


public class MemoryContext {
    public StaticArray<Integer> buckets;
    public StaticArray<AtomicInteger> bucketsIndex;
    public StaticArray<AtomicInteger> leaderMapping;
    public StaticArray<NavigationMarker<Integer, Edge>> allBufferedNodes;

    public MemoryContext(int size) {
        this.buckets = new StaticArray<>(size);
        this.bucketsIndex = new StaticArray<>(size);
        this.leaderMapping = new StaticArray<>(size);
        this.allBufferedNodes = new StaticArray<>(size);

        // Initialize each element in bucketsIndex and leaderMapping with a new AtomicInteger
        for (int i = 0; i < size; i++) {
            this.bucketsIndex.set(i, new AtomicInteger(0));
            this.leaderMapping.set(i, new AtomicInteger(0));
        }
    }
}

