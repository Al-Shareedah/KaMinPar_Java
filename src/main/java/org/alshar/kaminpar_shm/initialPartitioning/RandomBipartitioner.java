package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.context.InitialPartitioningContext;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.common.Math.Random_shm;

public class RandomBipartitioner extends Bipartitioner {

    public static class MemoryContext {
        public int memoryInKB() {
            return 0;
        }
    }

    private final Random_shm rand = Random_shm.getInstance();

    public RandomBipartitioner(Graph graph, PartitionContext pCtx, InitialPartitioningContext iCtx, MemoryContext mCtx) {
        super(graph, pCtx, iCtx);
    }

    @Override
    protected void bipartitionImpl() {
        for (NodeID u : graph.nodes()) {
            int block = rand.randomIndex(0, 2);
            if (blockWeights.get(block).add(graph.nodeWeight(u)).compareTo(pCtx.blockWeights.perfectlyBalanced(block)) < 0) {
                setBlock(u, new BlockID(block));
            } else {
                addToSmallerBlock(u);
            }
        }
    }
}
