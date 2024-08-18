package org.alshar.common.GraphUtils;

import java.util.ArrayList;
import java.util.List;

public class GlobalInitialPartitionerMemoryPool {
    private static final ThreadLocal<InitialPartitionerMemoryPool> threadLocalPool = ThreadLocal.withInitial(InitialPartitionerMemoryPool::new);
    private static final List<InitialPartitionerMemoryPool> allPools = new ArrayList<>();
    public static InitialPartitionerMemoryPool local() {
        return threadLocalPool.get();
    }
    public static List<InitialPartitionerMemoryPool> all() {
        synchronized (allPools) {
            return new ArrayList<>(allPools);
        }
    }
}

