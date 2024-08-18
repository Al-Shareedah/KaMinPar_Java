package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.Graph;
import org.alshar.common.context.InitialPartitioningContext;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.*;
import org.alshar.kaminpar_shm.refinement.Marker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public abstract class BfsBipartitionerBase extends Bipartitioner {
    public static class MemoryContext {
        public List<Queue<NodeID>> queues;
        public Marker marker;

        public MemoryContext(int graphSize) {
            queues = new ArrayList<>(Arrays.asList(new Queue<>(graphSize), new Queue<>(graphSize)));
            marker = new Marker(graphSize, 3);
        }
        public MemoryContext() {
            this.queues = new ArrayList<>(Arrays.asList(new Queue<>(0), new Queue<>(0)));
            this.marker = new Marker(0, 3);
        }
        public long memoryInKB() {
            return queues.get(0).memoryInKB() + queues.get(1).memoryInKB() + marker.memoryInKB();
        }
    }

    public BfsBipartitionerBase(Graph graph, PartitionContext pCtx, InitialPartitioningContext iCtx) {
        super(graph, pCtx, iCtx);
    }
}

// Selection strategies
interface BlockSelectionStrategy {
    BlockID select(BlockID activeBlock, Bipartitioner.BlockWeights blockWeights, PartitionContext pCtx, List<Queue<NodeID>> queues);
}

class AlternatingBlockSelectionStrategy implements BlockSelectionStrategy {
    @Override
    public BlockID select(BlockID activeBlock, Bipartitioner.BlockWeights blockWeights, PartitionContext pCtx, List<Queue<NodeID>> queues) {
        return new BlockID(1 - activeBlock.getValue());
    }
}

class LighterBlockSelectionStrategy implements BlockSelectionStrategy {
    @Override
    public BlockID select(BlockID activeBlock, Bipartitioner.BlockWeights blockWeights, PartitionContext pCtx, List<Queue<NodeID>> queues) {
        return (blockWeights.get(0).compareTo(blockWeights.get(1)) < 0) ? new BlockID(0) : new BlockID(1);
    }
}

class SequentialBlockSelectionStrategy implements BlockSelectionStrategy {
    @Override
    public BlockID select(BlockID activeBlock, Bipartitioner.BlockWeights blockWeights, PartitionContext pCtx, List<Queue<NodeID>> queues) {
        return (blockWeights.get(0).compareTo(pCtx.blockWeights.perfectlyBalanced(0)) < 0) ? new BlockID(0) : new BlockID(1);
    }
}

class LongerQueueBlockSelectionStrategy implements BlockSelectionStrategy {
    @Override
    public BlockID select(BlockID activeBlock, Bipartitioner.BlockWeights blockWeights, PartitionContext pCtx, List<Queue<NodeID>> queues) {
        return (queues.get(0).size() < queues.get(1).size()) ? new BlockID(1) : new BlockID(0);
    }
}

class ShorterQueueBlockSelectionStrategy implements BlockSelectionStrategy {
    @Override
    public BlockID select(BlockID activeBlock, Bipartitioner.BlockWeights blockWeights, PartitionContext pCtx, List<Queue<NodeID>> queues) {
        return (queues.get(0).size() < queues.get(1).size()) ? new BlockID(0) : new BlockID(1);
    }
}

