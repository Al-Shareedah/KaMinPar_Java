package org.alshar.common.context;

import org.alshar.common.datastructures.BlockWeight;
import org.alshar.common.datastructures.TreeNode;
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
        pCtx.initializeCombinedBlockWeightsTree();
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
                        TreeNode partitionNode = new TreeNode(0); // Node to represent this partition
                        List<BlockWeight> combinedBlocksForThisPartition = new ArrayList<>();

                        // Randomly select blocks to add until the partition reaches the desired size
                        int numBlocksToCombine = blockWeightMap.size() / 2; // Determine how many blocks to combine for this partition
                        for (int i = 0; i < numBlocksToCombine && remainingNodes > 0 && !availableBlockWeights.isEmpty(); i++) {
                            // Randomly select a block from the available block weights
                            int randomIndex = random.nextInt(availableBlockWeights.size());
                            BlockWeight selectedBlockWeight = availableBlockWeights.remove(randomIndex);

                            mergedBlockWeight.value += selectedBlockWeight.value;

                            // Create a TreeNode for the selected block weight and add it as a child of the partition node
                            TreeNode blockNode = new TreeNode(selectedBlockWeight.value);
                            partitionNode.addChild(blockNode);
                            combinedBlocksForThisPartition.add(selectedBlockWeight);  // Track which blocks were combined
                            remainingNodes -= selectedBlockWeight.value;

                            // Mark the block weight as used in the map
                            blockWeightMap.put(selectedBlockWeight, true);
                        }

                        // Set the merged block weight for this partition
                        perfectlyBalancedBlockWeights.set(b, mergedBlockWeight);
                        pCtx.combinedBlockWeights.set(b, combinedBlocksForThisPartition);  // Store combined blocks in PartitionContext

                        // Update the partition node label with the total weight
                        partitionNode.setLabel(mergedBlockWeight.value);

                        // Add the partition node as a child of the combinedBlockWeightsRoot
                        pCtx.combinedBlockWeightsRoot.addChild(partitionNode);

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

        long totalNodeWeight = pCtx.totalNodeWeight.value;  // The total weight we need to partition
        long remainingNodes = totalNodeWeight;

        // Collect block weights that haven't been used yet
        List<TreeNode> availableNodes = new ArrayList<>();
        flattenTree(pCtx.combinedBlockWeightsRoot, availableNodes);

        // Sort available nodes in descending order by their labels (values)
        availableNodes.sort(Comparator.comparingLong(TreeNode::getLabel).reversed());

        // Check combinedBlockWeightsRoot before attempting to recombine blocks
        List<TreeNode> selectedNodes = new ArrayList<>();
        for (TreeNode child : pCtx.combinedBlockWeightsRoot.getChildren()) {
            long combinedSum = child.getChildren().stream().mapToLong(TreeNode::getLabel).sum();
            if (combinedSum == totalNodeWeight) {
                selectedNodes.addAll(child.getChildren());
                markNodesAsUsed(child, blockWeightMap);  // Mark as used
                remainingNodes = 0;
                break;
            }
        }

        // If remainingNodes are greater than 0, select from availableNodes or combine again
        if (remainingNodes > 0) {
            List<TreeNode> combinedNodes = new ArrayList<>();
            for (TreeNode node : availableNodes) {
                if (remainingNodes >= node.getLabel()) {
                    selectedNodes.add(node);
                    combinedNodes.add(node);
                    remainingNodes -= node.getLabel();
                    blockWeightMap.put(new BlockWeight(node.getLabel()), true);  // Mark it as used
                }

                if (remainingNodes == 0) {
                    if (!combinedNodes.isEmpty()) {
                        TreeNode combinedNode = new TreeNode(combinedNodes.stream().mapToLong(TreeNode::getLabel).sum());
                        combinedNodes.forEach(combinedNode::addChild);
                        pCtx.combinedBlockWeightsRoot.addChild(combinedNode);
                    }
                    break;
                }
            }

        }

        // Divide the selected nodes into perfectlyBalancedBlockWeights if no fallback
        // Combine selectedNodes values until we get the same number of nodes as pCtx.k.value
        if (remainingNodes == 0 && selectedNodes.size() > pCtx.k.value) {
            List<TreeNode> nodeList = new ArrayList<>(selectedNodes);
            List<TreeNode> finalNodes = new ArrayList<>();

            while (nodeList.size() + finalNodes.size() > pCtx.k.value) {
                // Separate leaf nodes from combined nodes
                List<TreeNode> leafNodes = new ArrayList<>();
                List<TreeNode> combinedNodes = new ArrayList<>();
                for (TreeNode node : nodeList) {
                    if (node.getChildren().isEmpty()) {
                        leafNodes.add(node);
                    } else {
                        combinedNodes.add(node);
                    }
                }

                // Prioritize combining two leaf nodes if possible
                TreeNode firstNode, secondNode;
                if (leafNodes.size() >= 2) {
                    // Sort leaf nodes and combine the most balanced pair
                    leafNodes.sort(Comparator.comparingLong(TreeNode::getLabel));
                    firstNode = leafNodes.remove(0); // Smallest node
                    secondNode = leafNodes.remove(leafNodes.size() - 1); // Largest node
                } else if (leafNodes.size() == 1) {
                    // Combine the single leaf node with the combined node closest in value
                    TreeNode leafNode = leafNodes.remove(0);
                    combinedNodes.sort(Comparator.comparingLong(TreeNode::getLabel));
                    firstNode = leafNode;
                    secondNode = combinedNodes.remove(combinedNodes.size() - 1); // Closest larger node
                } else {
                    // Combine the two most balanced combined nodes
                    combinedNodes.sort(Comparator.comparingLong(TreeNode::getLabel));
                    firstNode = combinedNodes.remove(0); // Smallest node
                    secondNode = combinedNodes.remove(combinedNodes.size() - 1); // Largest node
                }

                // Combine the selected nodes
                long combinedValue = firstNode.getLabel() + secondNode.getLabel();
                TreeNode combinedNode = new TreeNode(combinedValue);
                combinedNode.addChild(firstNode);
                combinedNode.addChild(secondNode);

                // Find the parent node of the two nodes (if they have one)
                TreeNode parentNode = findParentNode(pCtx.combinedBlockWeightsRoot, firstNode, secondNode);
                if (parentNode == null) {
                    throw new IllegalStateException("Parent node for the selected nodes not found.");
                }

                // Remove the two nodes from the parent
                parentNode.getChildren().remove(firstNode);
                parentNode.getChildren().remove(secondNode);

                // Add the new combined node as a child of the parent
                parentNode.addChild(combinedNode);

                // Update nodeList for the next iteration
                nodeList.clear();
                nodeList.addAll(leafNodes);
                nodeList.addAll(combinedNodes);
                nodeList.add(combinedNode);
            }


            // Remaining nodes in the list are the final partitions
            finalNodes.addAll(nodeList);

            // Allocate final nodes to perfectlyBalancedBlockWeights
            for (int b = 0; b < pCtx.k.value; b++) {
                TreeNode finalNode = finalNodes.get(b);
                perfectlyBalancedBlockWeights.set(b, new BlockWeight(finalNode.getLabel()));

                // Calculate max block weight
                long maxBlockWeight = finalNode.getLabel() + pCtx.absoluteEpsilon;
                maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
            }
        } else if (remainingNodes == 0 && selectedNodes.size() == pCtx.k.value) {
            // Exact match, allocate directly
            for (int b = 0; b < pCtx.k.value; b++) {
                TreeNode selectedNode = selectedNodes.get(b);
                perfectlyBalancedBlockWeights.set(b, new BlockWeight(selectedNode.getLabel()));

                // Calculate max block weight
                long maxBlockWeight = selectedNode.getLabel() + pCtx.absoluteEpsilon;
                maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
            }
        } else {
            // Fallback to evenly splitting the weights
            double blockWeight = 1.0 * totalNodeWeight / inputK;
            for (int b = 0; b < pCtx.k.value; b++) {
                int finalK = PartitionUtils.computeFinalK(b, pCtx.k.value, inputK);
                perfectlyBalancedBlockWeights.set(b, new BlockWeight((long) Math.ceil(finalK * blockWeight)));
                long maxBlockWeight = perfectlyBalancedBlockWeights.get(b).value + pCtx.absoluteEpsilon;
                maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
            }
        }
    }


    // Helper method to flatten the tree into a list of leaf nodes
    private void flattenTree(TreeNode node, List<TreeNode> flattenedList) {
        if (node == null) return;
        if (node.getChildren().isEmpty()) {
            flattenedList.add(node);
        } else {
            for (TreeNode child : node.getChildren()) {
                flattenTree(child, flattenedList);
            }
        }
    }

    // Helper method to mark nodes as used in the blockWeightMap
    private void markNodesAsUsed(TreeNode node, Map<BlockWeight, Boolean> blockWeightMap) {
        for (TreeNode child : node.getChildren()) {
            blockWeightMap.put(new BlockWeight(child.getLabel()), true);
        }
    }








    // Helper method to find a node by its label in the tree
    private TreeNode findNodeByLabel(TreeNode root, long label) {
        if (root == null) return null;
        if (root.getLabel() == label) return root;

        for (TreeNode child : root.getChildren()) {
            TreeNode result = findNodeByLabel(child, label);
            if (result != null) return result;
        }
        return null;
    }
    // Helper method to find the parent node of two children
    private TreeNode findParentNode(TreeNode root, TreeNode node1, TreeNode node2) {
        if (root == null || root.getChildren().isEmpty()) {
            return null;
        }

        boolean containsNode1 = root.getChildren().contains(node1);
        boolean containsNode2 = root.getChildren().contains(node2);

        if (containsNode1 && containsNode2) {
            return root; // Found the parent
        }

        for (TreeNode child : root.getChildren()) {
            TreeNode result = findParentNode(child, node1, node2);
            if (result != null) {
                return result;
            }
        }
        return null;
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
