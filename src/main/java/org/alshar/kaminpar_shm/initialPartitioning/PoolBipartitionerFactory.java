package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.context.InitialPartitioningContext;
import org.alshar.common.context.PartitionContext;
import org.alshar.kaminpar_shm.initialPartitioning.AlternatingBlockSelectionStrategy;
public class PoolBipartitionerFactory {

    private PoolBipartitioner currentPoolBipartitioner;

    public PoolBipartitioner create(Graph graph, PartitionContext pCtx, InitialPartitioningContext iCtx, PoolBipartitioner.MemoryContext mCtx) {
        PoolBipartitioner pool = new PoolBipartitioner(graph, pCtx, iCtx, mCtx);

        pool.registerBipartitioner("greedy_graph_growing", new GreedyGraphGrowingBipartitioner(graph, pCtx, iCtx, mCtx.gggMCtx));
        pool.registerBipartitioner("bfs_alternating", new BfsBipartitioner(graph, pCtx, iCtx, mCtx.bfsMCtx, new AlternatingBlockSelectionStrategy()));
        pool.registerBipartitioner("bfs_lighter_block", new BfsBipartitioner(graph, pCtx, iCtx, mCtx.bfsMCtx, new LighterBlockSelectionStrategy()));
        pool.registerBipartitioner("bfs_longer_queue", new BfsBipartitioner(graph, pCtx, iCtx, mCtx.bfsMCtx, new LongerQueueBlockSelectionStrategy()));
        pool.registerBipartitioner("bfs_shorter_queue", new BfsBipartitioner(graph, pCtx, iCtx, mCtx.bfsMCtx, new ShorterQueueBlockSelectionStrategy()));
        pool.registerBipartitioner("bfs_sequential", new BfsBipartitioner(graph, pCtx, iCtx, mCtx.bfsMCtx, new SequentialBlockSelectionStrategy()));
        pool.registerBipartitioner("random", new RandomBipartitioner(graph, pCtx, iCtx, mCtx.randMCtx));

        return pool;
    }
}
