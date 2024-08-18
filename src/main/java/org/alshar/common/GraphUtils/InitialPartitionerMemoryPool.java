package org.alshar.common.GraphUtils;

import org.alshar.kaminpar_shm.initialPartitioning.InitialPartitioner;

import java.util.ArrayList;
import java.util.List;

public class InitialPartitionerMemoryPool {
    private final List<InitialPartitioner.MemoryContext> pool = new ArrayList<>();

    public InitialPartitioner.MemoryContext get() {
        if (!pool.isEmpty()) {
            return pool.remove(pool.size() - 1); // Get and remove the last element
        }
        return new InitialPartitioner.MemoryContext(); // Or return a new context if empty
    }

    public void put(InitialPartitioner.MemoryContext mCtx) {
        pool.add(mCtx);
    }

    public long memoryInKB() {
        long memory = 0;
        for (InitialPartitioner.MemoryContext obj : pool) {
            memory += obj.memoryInKB();
        }
        return memory;
    }
    public int size() {
        return pool.size();
    }
}
