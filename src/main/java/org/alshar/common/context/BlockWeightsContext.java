package org.alshar.common.context;

import org.alshar.common.datastructures.BlockWeight;
import org.alshar.kaminpar_shm.PartitionUtils;
import org.alshar.kaminpar_shm.kaminpar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

public class BlockWeightsContext {
    private List<BlockWeight> perfectlyBalancedBlockWeights;
    private List<BlockWeight> maxBlockWeights;
    public BlockWeightsContext() {
        // Default constructor
    }

    // Copy constructor
    public BlockWeightsContext(BlockWeightsContext other) {
        // Deep copy the perfectlyBalancedBlockWeights list
        this.perfectlyBalancedBlockWeights = new ArrayList<>();
        for (BlockWeight weight : other.perfectlyBalancedBlockWeights) {
            this.perfectlyBalancedBlockWeights.add(new BlockWeight(weight.value));
        }

        // Deep copy the maxBlockWeights list
        this.maxBlockWeights = new ArrayList<>();
        for (BlockWeight weight : other.maxBlockWeights) {
            this.maxBlockWeights.add(new BlockWeight(weight.value));
        }
    }

    public void setup(PartitionContext pCtx) {
        if (pCtx.k.value == 0) {
            throw new IllegalStateException("PartitionContext::k not initialized");
        }
        if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
        }
        if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
        }

        long perfectlyBalancedBlockWeight = (long) Math.ceil(1.0 * pCtx.totalNodeWeight.value / pCtx.k.value);
        long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * perfectlyBalancedBlockWeight);

        maxBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));
        perfectlyBalancedBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));

        ForkJoinPool forkJoinPool = new ForkJoinPool();

        try {
            forkJoinPool.invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int b = 0; b < pCtx.k.value; b++) {
                        perfectlyBalancedBlockWeights.set(b, new BlockWeight(perfectlyBalancedBlockWeight));
                        if (pCtx.maxNodeWeight.value == 1) {
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                        } else {
                            maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight, perfectlyBalancedBlockWeight + pCtx.maxNodeWeight.value)));
                        }
                    }
                }
            });
        } finally {
            forkJoinPool.shutdown();
            try {
                if (!forkJoinPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    forkJoinPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                forkJoinPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void setup(PartitionContext pCtx, int inputK) {
        if (pCtx.k.value == 0) {
            throw new IllegalStateException("PartitionContext::k not initialized");
        }
        if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
        }
        if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
        }

        double blockWeight = 1.0 * pCtx.totalNodeWeight.value / inputK;

        maxBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));
        perfectlyBalancedBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));

        ForkJoinPool forkJoinPool = new ForkJoinPool();

        try {
            forkJoinPool.invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int b = 0; b < pCtx.k.value; b++) {
                        int finalK = PartitionUtils.computeFinalK(b, pCtx.k.value, inputK);
                        perfectlyBalancedBlockWeights.set(b, new BlockWeight((long) Math.ceil(finalK * blockWeight)));
                        long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * perfectlyBalancedBlockWeights.get(b).value);
                        if (pCtx.maxNodeWeight.value == 1) {
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                        } else {
                            maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight, perfectlyBalancedBlockWeights.get(b).value + pCtx.maxNodeWeight.value)));
                        }
                    }
                }
            });
        } finally {
            forkJoinPool.shutdown();
            try {
                if (!forkJoinPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    forkJoinPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                forkJoinPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public BlockWeight max(int b) {
        return maxBlockWeights.get(b);
    }

    public List<BlockWeight> allMax() {
        return maxBlockWeights;
    }

    public BlockWeight perfectlyBalanced(int b) {
        return perfectlyBalancedBlockWeights.get(b);
    }

    public List<BlockWeight> allPerfectlyBalanced() {
        return perfectlyBalancedBlockWeights;
    }
}
