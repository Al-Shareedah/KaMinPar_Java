package org.alshar.common.context;

import org.alshar.common.datastructures.BlockWeight;
import org.alshar.kaminpar_shm.PartitionUtils;
import org.alshar.kaminpar_shm.kaminpar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

public class BlockWeightsContext {
    public List<BlockWeight> perfectlyBalancedBlockWeights;
    public List<BlockWeight> maxBlockWeights;
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

    public void setup(PartitionContext pCtx, Queue<BlockWeight> blockWeightQueue) {
        if (pCtx.k.value == 0) {
            throw new IllegalStateException("PartitionContext::k not initialized");
        }
        if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
        }
        if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
        }

        // Initialize the lists to hold the block weights
        maxBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));
        perfectlyBalancedBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));

        ForkJoinPool forkJoinPool = new ForkJoinPool();

        try {
            forkJoinPool.invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    if (blockWeightQueue != null && !blockWeightQueue.isEmpty()) {
                        // Copy block constraints without removing them from the queue
                        List<BlockWeight> blockWeightList = new ArrayList<>(blockWeightQueue);

                        for (int b = 0; b < pCtx.k.value; b++) {
                            if (b < blockWeightList.size()) {
                                // Use block constraints
                                BlockWeight blockConstraint = blockWeightList.get(b);
                                perfectlyBalancedBlockWeights.set(b, blockConstraint);

                                long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * blockConstraint.value);
                                if (pCtx.maxNodeWeight.value == 1) {
                                    maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                                } else {
                                    maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight, blockConstraint.value + pCtx.maxNodeWeight.value)));
                                }
                            }
                        }
                    } else {
                        // Use the default calculation if block constraints are not provided
                        for (int b = 0; b < pCtx.k.value; b++) {
                            long perfectlyBalancedBlockWeight = (long) Math.ceil(1.0 * pCtx.totalNodeWeight.value / pCtx.k.value);
                            long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * perfectlyBalancedBlockWeight);

                            perfectlyBalancedBlockWeights.set(b, new BlockWeight(perfectlyBalancedBlockWeight));
                            if (pCtx.maxNodeWeight.value == 1) {
                                maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                            } else {
                                maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight, perfectlyBalancedBlockWeight + pCtx.maxNodeWeight.value)));
                            }
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
    public void setup(PartitionContext pCtx, int inputK, Queue<BlockWeight> blockWeightQueue) {
        if (pCtx.k.value == 0) {
            throw new IllegalStateException("PartitionContext::k not initialized");
        }
        if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
        }
        if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
        }

        // Initialize the lists to hold the block weights
        maxBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));
        perfectlyBalancedBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k.value, new BlockWeight(0)));

        ForkJoinPool forkJoinPool = new ForkJoinPool();

        try {
            forkJoinPool.invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int b = 0; b < pCtx.k.value; b += 2) {
                        // Ensure there are enough weights in the queue for this bipartition
                        if (blockWeightQueue.size() < 2) {
                            throw new IllegalStateException("Not enough block weights in the queue to partition.");
                        }

                        // Pop two block sizes from the queue
                        BlockWeight blockWeight1 = blockWeightQueue.poll();
                        BlockWeight blockWeight2 = blockWeightQueue.poll();

                        // Set the weights for the two blocks resulting from the bipartition
                        perfectlyBalancedBlockWeights.set(b, blockWeight1);
                        perfectlyBalancedBlockWeights.set(b + 1, blockWeight2);

                        // Calculate the maximum block weights
                        long maxBlockWeight1 = (long) ((1.0 + pCtx.epsilon) * blockWeight1.value);
                        long maxBlockWeight2 = (long) ((1.0 + pCtx.epsilon) * blockWeight2.value);

                        // Set max block weights considering maxNodeWeight
                        if (pCtx.maxNodeWeight.value == 1) {
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight1));
                            maxBlockWeights.set(b + 1, new BlockWeight(maxBlockWeight2));
                        } else {
                            maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight1, blockWeight1.value + pCtx.maxNodeWeight.value)));
                            maxBlockWeights.set(b + 1, new BlockWeight(Math.max(maxBlockWeight2, blockWeight2.value + pCtx.maxNodeWeight.value)));
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
