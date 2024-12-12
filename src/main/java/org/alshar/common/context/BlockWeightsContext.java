package org.alshar.common.context;

import org.alshar.common.datastructures.BlockWeight;
import org.alshar.kaminpar_shm.PartitionUtils;
import org.alshar.kaminpar_shm.kaminpar;

import java.util.*;
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

    public void setup(PartitionContext pCtx, Map<BlockWeight, Boolean> blockWeightQueue) {
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
                        // Iterate over the blockWeightQueue to get available block weights
                        int b = 0;
                        for (Map.Entry<BlockWeight, Boolean> entry : blockWeightQueue.entrySet()) {
                            if (!entry.getValue() && b < pCtx.k.value) {
                                BlockWeight blockConstraint = entry.getKey();
                                // Assign the block weight to the perfectly balanced block weights
                                perfectlyBalancedBlockWeights.set(b, blockConstraint);

                                // Calculate the maximum block weight based on the epsilon value
                                long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * blockConstraint.value);
                                if (pCtx.maxNodeWeight.value == 1) {
                                    maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                                } else {
                                    maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight, blockConstraint.value + pCtx.maxNodeWeight.value)));
                                }

                                // Mark this block weight as used
                                blockWeightQueue.put(blockConstraint, true);
                                b++;
                            }

                            // If we have used all partitions, exit the loop
                            if (b >= pCtx.k.value) {
                                break;
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



    public void twoWaySetup(PartitionContext pCtx, int inputK, Map<BlockWeight, Boolean> blockWeightMap) {
        if (pCtx.k.value == 0) {
            throw new IllegalStateException("PartitionContext::k not initialized");
        }
        if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
        }
        if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
            throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
        }

        // Initialize block weights for two partitions
        maxBlockWeights = new ArrayList<>(Collections.nCopies(2, new BlockWeight(0)));
        perfectlyBalancedBlockWeights = new ArrayList<>(Collections.nCopies(2, new BlockWeight(0)));
        pCtx.combinedBlockWeights.clear();  // Clear any previous combined blocks
        pCtx.combinedBlockWeights.add(new ArrayList<>());  // For partition 1
        pCtx.combinedBlockWeights.add(new ArrayList<>());  // For partition 2
        ForkJoinPool forkJoinPool = new ForkJoinPool();

        try {
            forkJoinPool.invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    long totalNodes = pCtx.totalNodeWeight.value;
                    long remainingNodes = totalNodes;

                    // Random selection mechanism
                    Random random = new Random();

                    // Priority queue to access block weights from blockWeightMap
                    List<BlockWeight> availableBlockWeights = new ArrayList<>();
                    for (Map.Entry<BlockWeight, Boolean> entry : blockWeightMap.entrySet()) {
                        if (!entry.getValue()) {
                            availableBlockWeights.add(entry.getKey());
                        }
                    }

                    // Loop for two-way partitioning (two sizes)
                    for (int b = 0; b < 2; b++) {
                        BlockWeight mergedBlockWeight = new BlockWeight(0);
                        List<BlockWeight> combinedBlocksForThisPartition = new ArrayList<>();

                        // Randomly select blocks to add until the partition reaches the desired size
                        int numBlocksToCombine = blockWeightMap.size() / 2; // Determine how many blocks to combine for this partition
                        for (int i = 0; i < numBlocksToCombine && remainingNodes > 0 && !availableBlockWeights.isEmpty(); i++) {
                            // Randomly select a block from the available block weights
                            int randomIndex = random.nextInt(availableBlockWeights.size());
                            BlockWeight selectedBlockWeight = availableBlockWeights.remove(randomIndex);

                            mergedBlockWeight.value += selectedBlockWeight.value;
                            combinedBlocksForThisPartition.add(selectedBlockWeight);  // Track which blocks were combined
                            remainingNodes -= selectedBlockWeight.value;

                            // Mark the block weight as used in the map
                            blockWeightMap.put(selectedBlockWeight, true);
                        }

                        // Set the merged block weight for this partition
                        perfectlyBalancedBlockWeights.set(b, mergedBlockWeight);
                        pCtx.combinedBlockWeights.set(b, combinedBlocksForThisPartition);  // Store combined blocks in PartitionContext

                        // Calculate the max block weight for this partition
                        long maxBlockWeight = mergedBlockWeight.value + pCtx.absoluteEpsilon;
                        maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                    }

                    // Ensure the remaining nodes are added if any remain (fail-safe)
                    if (remainingNodes != 0) {
                        // Add the remaining nodes to the partition with the smallest weight
                        BlockWeight smallestBlock = perfectlyBalancedBlockWeights.get(0).value < perfectlyBalancedBlockWeights.get(1).value
                                ? perfectlyBalancedBlockWeights.get(0)
                                : perfectlyBalancedBlockWeights.get(1);
                        smallestBlock.value += remainingNodes;

                        // Update the max block weight
                        int smallestBlockIndex = perfectlyBalancedBlockWeights.get(0).value < perfectlyBalancedBlockWeights.get(1).value ? 0 : 1;
                        long maxBlockWeight = smallestBlock.value + pCtx.absoluteEpsilon;
                        maxBlockWeights.set(smallestBlockIndex, new BlockWeight(maxBlockWeight));
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
    public void setup(PartitionContext pCtx, int inputK, Map<BlockWeight, Boolean> blockWeightMap) {
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
                    long totalNodeWeight = pCtx.n.value;  // The total weight we need to partition
                    long remainingNodes = totalNodeWeight;

                    // Collect block weights that haven't been used yet and load them into availableWeights
                    List<BlockWeight> availableWeights = new ArrayList<>();
                    for (Map.Entry<BlockWeight, Boolean> entry : blockWeightMap.entrySet()) {
                        if (!entry.getValue()) {
                            availableWeights.add(entry.getKey());
                        }
                    }

                    // Sort available weights in descending order for easier combination finding
                    availableWeights.sort(Comparator.comparingLong(bw -> -bw.value));

                    // Check combinedBlockWeights before attempting to recombine blocks
                    List<BlockWeight> selectedWeights = new ArrayList<>();
                    for (List<BlockWeight> combinedBlockList : pCtx.combinedBlockWeights) {
                        long combinedSum = combinedBlockList.stream().mapToLong(bw -> bw.value).sum();
                        if (combinedSum == totalNodeWeight) {
                            selectedWeights.addAll(combinedBlockList);
                            combinedBlockList.forEach(bw -> blockWeightMap.put(bw, true));  // Mark as used
                            remainingNodes = 0;
                            break;
                        }
                    }

                    // If remainingNodes are greater than 0, select from availableWeights or combine again
                    if (remainingNodes > 0) {
                        List<BlockWeight> combinedBlocks = new ArrayList<>();
                        for (BlockWeight bw : availableWeights) {
                            if (remainingNodes >= bw.value) {
                                selectedWeights.add(bw);
                                combinedBlocks.add(bw);
                                remainingNodes -= bw.value;
                                blockWeightMap.put(bw, true);  // Mark it as used
                            }

                            if (remainingNodes == 0) {
                                break;
                            }
                        }

                        // If combination occurred, add the combined blocks to combinedBlockWeights
                        if (!combinedBlocks.isEmpty()) {
                            pCtx.combinedBlockWeights.add(combinedBlocks);
                        }
                    }

                    // If the exact combination of weights wasn't found, revert to the fallback method
                    if (remainingNodes != 0 || selectedWeights.size() < pCtx.k.value) {
                        // Revert to basic perfectly balanced block weight calculation
                        double blockWeight = 1.0 * pCtx.totalNodeWeight.value / inputK;

                        for (int b = 0; b < pCtx.k.value; b++) {
                            int finalK = PartitionUtils.computeFinalK(b, pCtx.k.value, inputK);
                            perfectlyBalancedBlockWeights.set(b, new BlockWeight((long) Math.ceil(finalK * blockWeight)));
                            long maxBlockWeight = perfectlyBalancedBlockWeights.get(b).value + pCtx.absoluteEpsilon;
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                        }
                        return;  // Exit after reverting to fallback method
                    }

                    // Divide the selected weights into perfectlyBalancedBlockWeights if no fallback
                    int blocksToAllocate = selectedWeights.size();
                    if (blocksToAllocate == pCtx.k.value) {
                        // Exact match, allocate directly
                        for (int b = 0; b < blocksToAllocate; b++) {
                            BlockWeight selectedBlockWeight = selectedWeights.get(b);
                            perfectlyBalancedBlockWeights.set(b, selectedBlockWeight);

                            // Calculate max block weight
                            long maxBlockWeight = selectedBlockWeight.value + pCtx.absoluteEpsilon;
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                        }
                    } else {
                        // Need to combine again and allocate
                        List<BlockWeight> combinedBlocks = new ArrayList<>();
                        for (int b = 0; b < pCtx.k.value; b++) {
                            BlockWeight mergedBlockWeight = new BlockWeight(0);

                            // Combine blocks if needed
                            while (mergedBlockWeight.value < totalNodeWeight / (pCtx.k.value - b) && !selectedWeights.isEmpty()) {
                                BlockWeight smallest = selectedWeights.remove(0);  // Get the smallest available weight
                                mergedBlockWeight.value += smallest.value;
                                combinedBlocks.add(smallest);
                            }

                            // Set the merged block weight
                            perfectlyBalancedBlockWeights.set(b, mergedBlockWeight);

                            // Calculate max block weight
                            long maxBlockWeight = mergedBlockWeight.value + pCtx.absoluteEpsilon;
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));

                            // Add combined blocks to combinedBlockWeights
                            pCtx.combinedBlockWeights.add(combinedBlocks);
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
    public BlockWeight maxBlockWeights(int b) {
        return maxBlockWeights.get(b);
    }
    public List<BlockWeight> allPerfectlyBalanced() {
        return perfectlyBalancedBlockWeights;
    }
}
