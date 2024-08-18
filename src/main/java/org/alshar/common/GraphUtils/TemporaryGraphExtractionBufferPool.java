package org.alshar.common.GraphUtils;

import java.util.ArrayList;
import java.util.List;

public class TemporaryGraphExtractionBufferPool {
    private static final ThreadLocal<TemporarySubgraphMemory> threadLocalPool = ThreadLocal.withInitial(TemporarySubgraphMemory::new);
    private static final List<TemporarySubgraphMemory> allPools = new ArrayList<>();

    public static TemporarySubgraphMemory local() {
        TemporarySubgraphMemory pool = threadLocalPool.get();
        synchronized (allPools) {
            if (!allPools.contains(pool)) {
                allPools.add(pool);
            }
        }
        return pool;
    }

    public static List<TemporarySubgraphMemory> all() {
        synchronized (allPools) {
            return new ArrayList<>(allPools);
        }
    }
}
