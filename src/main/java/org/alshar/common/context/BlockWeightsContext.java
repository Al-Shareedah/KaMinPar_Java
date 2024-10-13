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

    public void setup(PartitionContext pCtx, long [] blockWeightsConstraint) {
        if (pCtx.k.value == 0) {
            throw new IllegalStateException("PartitionContext::k not initialized");
        }
        if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
        }
        if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
        }

        // Initialize the perfectlyBalancedBlockWeights list with user-defined values
        perfectlyBalancedBlockWeights = new ArrayList<>(pCtx.k.value);
        for (int i = 0; i < pCtx.k.value; i++) {
            perfectlyBalancedBlockWeights.add(new BlockWeight(blockWeightsConstraint[i]));
        }

        // Initialize the maxBlockWeights list
        maxBlockWeights = new ArrayList<>(pCtx.k.value);
        ForkJoinPool forkJoinPool = new ForkJoinPool();

        try {
            forkJoinPool.invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int b = 0; b < pCtx.k.value; b++) {
                        // Use the individual perfectlyBalancedBlockWeights for each block to calculate the maxBlockWeight
                        long perfectlyBalancedWeight = perfectlyBalancedBlockWeights.get(b).value;
                        long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * perfectlyBalancedWeight);

                        // Calculate the actual maxBlockWeight based on maxNodeWeight
                        if (pCtx.maxNodeWeight.value == 1) {
                            maxBlockWeights.add(new BlockWeight(maxBlockWeight));
                        } else {
                            maxBlockWeights.add(new BlockWeight(Math.max(maxBlockWeight, perfectlyBalancedWeight + pCtx.maxNodeWeight.value)));
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



    public BlockWeight perfectlyBalanced(int b) {
        return perfectlyBalancedBlockWeights.get(b);
    }

}
