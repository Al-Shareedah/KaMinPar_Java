package org.alshar.kaminpar_shm.refinement;
import org.alshar.Context;
import org.alshar.common.GraphUtils.Edge;
import org.alshar.common.Math.Random_shm;
import org.alshar.common.ParallelUtils.ParallelFor;
import org.alshar.common.datastructures.*;
import org.alshar.common.context.*;
import org.alshar.kaminpar_shm.*;
import org.alshar.kaminpar_shm.refinement.gains.SparseGainCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GreedyBalancer extends Refiner {

    private static final boolean DEBUG = true; // Equivalent to SET_DEBUG(false)
    private static final boolean STATISTICS = false; // Equivalent to SET_STATISTICS(false)

    public static class Statistics {
        public long initialCut;
        public long finalCut;
        public AtomicInteger numSuccessfulRandomMoves = new AtomicInteger();
        public AtomicInteger numSuccessfulAdjacentMoves = new AtomicInteger();
        public AtomicInteger numUnsuccessfulRandomMoves = new AtomicInteger();
        public AtomicInteger numUnsuccessfulAdjacentMoves = new AtomicInteger();
        public AtomicInteger numMovedBorderNodes = new AtomicInteger();
        public AtomicInteger numMovedInternalNodes = new AtomicInteger();
        public AtomicInteger numPQReinserts = new AtomicInteger();
        public AtomicInteger numOverloadedBlocks = new AtomicInteger();
        public long initialOverload;
        public long finalOverload;
        public AtomicInteger totalPQSizes = new AtomicInteger();
        public AtomicInteger numFeasibleTargetBlockInits = new AtomicInteger();


        public void reset() {
            initialCut = 0;
            finalCut = 0;
            numSuccessfulRandomMoves.set(0);
            numSuccessfulAdjacentMoves.set(0);
            numUnsuccessfulRandomMoves.set(0);
            numUnsuccessfulAdjacentMoves.set(0);
            numMovedBorderNodes.set(0);
            numMovedInternalNodes.set(0);
            numPQReinserts.set(0);
            numOverloadedBlocks.set(0);
            initialOverload = 0;
            finalOverload = 0;
            totalPQSizes.set(0);
            numFeasibleTargetBlockInits.set(0);
        }

        public void print() {
            System.out.println("Greedy Node Balancer:");
            System.out.println("  * Changed cut: " + initialCut + " -> " + finalCut);
            System.out.println("  * # overloaded blocks: " + numOverloadedBlocks.get());
            System.out.println("  * # overload change: " + initialOverload + " -> " + finalOverload);
            System.out.println("  * # moved nodes: " + (numMovedBorderNodes.get() + numMovedInternalNodes.get())
                    + " (border nodes: " + numMovedBorderNodes.get()
                    + ", internal nodes: " + numMovedInternalNodes.get() + ")");
            System.out.println("  * # successful border node moves: " + numSuccessfulAdjacentMoves.get()
                    + ", # unsuccessful border node moves: " + numUnsuccessfulAdjacentMoves.get());
            System.out.println("  * # successful random node moves: " + numSuccessfulRandomMoves.get()
                    + ", # unsuccessful random node moves: " + numUnsuccessfulRandomMoves.get());
            System.out.println("  * failed moves due to gain changes: " + numPQReinserts.get());
            if (numOverloadedBlocks.get() > 0) {
                System.out.println("  * Total initial PQ sizes: " + totalPQSizes.get() + ", avg "
                        + (totalPQSizes.get() / numOverloadedBlocks.get()));
            }
            System.out.println("  * Feasible target blocks initialized: " + numFeasibleTargetBlockInits.get());
        }
    }

    private final BlockID maxK;

    private PartitionedGraph pGraph;
    private PartitionContext pCtx;

    private final DynamicBinaryMinMaxForest<NodeID, Double> pq;
    private final Map<BlockID, RatingMap<BlockID, EdgeWeight>> ratingMap;
    private final Map<BlockID, List<BlockID>> feasibleTargetBlocks;
    private final Marker marker;
    private final List<BlockWeight> pqWeight;
    private SparseGainCache gainCache = null;

    private final Statistics stats = new Statistics();
    //private SparseGainCache gainCache;

    public GreedyBalancer(Context ctx) {
        this.maxK = ctx.partition.k;
        this.pq = new DynamicBinaryMinMaxForest<>(ctx.partition.n.value, ctx.partition.k.value);
        this.marker = new Marker(ctx.partition.n.value, 1);
        this.pqWeight = new ArrayList<>(Collections.nCopies(ctx.partition.k.value, new BlockWeight(0)));
        this.ratingMap = new ConcurrentHashMap<>();
        this.feasibleTargetBlocks = new ConcurrentHashMap<>();
    }

    public void initialize(PartitionedGraph pGraph) {
        this.pGraph = pGraph;
    }
    public void trackMoves(SparseGainCache gainCache) {
        this.gainCache = gainCache;  // Initialize gainCache
    }
    @Override
    public boolean refine(PartitionedGraph pGraph, PartitionContext pCtx) {
        this.pGraph = pGraph;
        this.pCtx = pCtx;

        // Ensure marker capacity is sufficient
        assert marker.capacity() >= pGraph.n().value : "Marker capacity is insufficient";

        // Reset marker and stats
        marker.reset();
        stats.reset();

        // Calculate the initial overload
        NodeWeight initialOverload = Metrics.totalOverload(pGraph, pCtx);

        // Calculate the initial underload
        NodeWeight initialUnderload = Metrics.totalUnderload(pGraph, pCtx);

        if (initialOverload.value == 0 ) {
            if(initialUnderload.value == 0){
                return true; // No overload and no underload mean no refinement needed
            }
            forcefullyResolveUnderload(pGraph, pCtx);
            return true;
        }

        // Calculate the initial edge cut (only for debugging)
        EdgeWeight initialCut = DEBUG ? Metrics.edgeCut(pGraph) : null;

        // Initialize the priority queue
        initPQ();

        // Perform the balancing round
        BlockWeight delta = performRound();

        // Calculate the new overload after the balancing round
        NodeWeight newOverload = new NodeWeight(initialOverload.value - delta.value);
        // If there's still overload after two rounds, run the fallback method to fix it
        if (newOverload.value != 0) {
            initPQ();
            forcefullyResolveOverload(pGraph, pCtx);
            initialUnderload = Metrics.totalUnderload(pGraph, pCtx);
            if(initialUnderload.value != 0){
                forcefullyResolveUnderload(pGraph, pCtx);
            }

        }

        // Print debug information if necessary

        EdgeWeight newCut = Metrics.edgeCut(pGraph);
        //System.out.println("-> Balancer: cut=" + initialCut + ", new cut=" + newCut);


        // Print statistics if enabled
        if (STATISTICS) {
            stats.print();
        }

        // Return true if the new overload is zero (meaning balance is achieved)
        return newOverload.value == 0;
    }


    private BlockWeight performRound() {
        // Statistics tracking
        if (STATISTICS) {
            stats.initialCut = Metrics.edgeCut(pGraph).value;
            stats.initialOverload = Metrics.totalOverload(pGraph, pCtx).value;
        }

        // Reset feasible target blocks
        feasibleTargetBlocks.forEach((blockID, blockList) -> blockList.clear());

        // Thread-safe tracking of overload delta
        AtomicLong globalOverload = new AtomicLong(0);
        // Main loop: Parallel processing of each block
        ParallelFor.parallelFor(0, pGraph.k().value, 1, (start, end) -> {
            for (int from = start; from < end; ++from) {
                BlockID blockIDFrom = new BlockID(from);
                BlockWeight currentOverload = blockOverload(blockIDFrom);

                // Initialize feasible target blocks if necessary
                if (currentOverload.value > 0) {
                    List<BlockID> feasibleList = feasibleTargetBlocks.computeIfAbsent(blockIDFrom, key -> new ArrayList<>());

                    if (feasibleList.isEmpty()) {
                        initFeasibleTargetBlocks();
                        if (DEBUG) {
                            /*
                            System.out.println("Block " + blockIDFrom.value + " with overload: " + currentOverload.value + ": " +
                                    feasibleList.size() + " feasible target blocks and " +
                                    pq.size(blockIDFrom.value) + " nodes in PQ. Total weight of PQ is " + pqWeight.get(blockIDFrom.value).value);
                             */
                        }
                    }
                }

                // Continue until the block's overload is resolved or its PQ is empty
                while (currentOverload.value > 0 && !pq.empty(from)) {
                    NodeID u = pq.peekMaxId(from);
                    NodeWeight uWeight = pGraph.nodeWeight(u);
                    double expectedRelGain = pq.peekMaxKey(from);
                    pq.popMax(from);
                    pqWeight.set(from, new BlockWeight(pqWeight.get(from).value - uWeight.value));
                    assert marker.get(u);

                    // Compute the gain and attempt to move the node
                    Pair<BlockID, Double> gainPair = computeGain(u, blockIDFrom);
                    BlockID toBlock = gainPair.getKey();
                    double actualRelGain = gainPair.getValue();

                    // Gain is correct -> try moving the node
                    if (expectedRelGain == actualRelGain) {
                        boolean movedNode = false;

                        // Internal node -> move to a random underloaded block
                        if (toBlock.equals(blockIDFrom)) {
                            movedNode = moveToRandomBlock(u);
                            if (STATISTICS) {
                                if (movedNode) {
                                    stats.numSuccessfulRandomMoves.incrementAndGet();
                                } else {
                                    stats.numUnsuccessfulRandomMoves.incrementAndGet();
                                }
                                stats.numMovedInternalNodes.incrementAndGet();
                            }

                            // Border node -> move to a promising block
                        } else if (moveNodeIfPossible(u, blockIDFrom, toBlock)) {
                            movedNode = true;
                            if (STATISTICS) {
                                stats.numMovedBorderNodes.incrementAndGet();
                                stats.numSuccessfulAdjacentMoves.incrementAndGet();
                            }

                            // Border node could not be moved -> try again
                        } else {
                            if (STATISTICS) {
                                stats.numPQReinserts.incrementAndGet();
                                stats.numUnsuccessfulAdjacentMoves.incrementAndGet();
                            }
                        }

                        // Update overload if the node was successfully moved
                        if (movedNode) {
                            BlockWeight delta = new BlockWeight(Math.min(currentOverload.value, uWeight.value));
                            currentOverload = new BlockWeight(currentOverload.value - delta.value);
                            globalOverload.addAndGet(delta.value);

                            // Try adding the neighbors of the moved node to the PQ
                            for (Edge edge : pGraph.neighbors(u)) {
                                NodeID v = pGraph.edgeTarget(edge.getEdgeID());
                                if (!marker.get(v) && pGraph.block(v).equals(blockIDFrom)) {
                                    addToPQ(blockIDFrom, v);
                                }
                                marker.set(v.value, 0, false);
                            }
                        } else {
                            addToPQ(blockIDFrom, u, uWeight, actualRelGain);
                        }

                    } else {
                        // Gain changed -> try again with the new gain
                        addToPQ(blockIDFrom, u, uWeight, actualRelGain);
                        if (STATISTICS) {
                            stats.numPQReinserts.incrementAndGet();
                        }
                    }
                }

                // Ensure the block overload matches expectations after processing
                assert currentOverload.value == Math.max(0,
                        pGraph.blockWeight(blockIDFrom).value - pCtx.blockWeights.max(from).value);
            }
        });
        // Combine the overload deltas across all threads
        BlockWeight globalOverloadDelta = new BlockWeight(globalOverload.get());
        return globalOverloadDelta;
    }

    private void forcefullyResolveOverload(PartitionedGraph pGraph, PartitionContext pCtx) {
        boolean hasOverload = true;
        // Continue iterating over the blocks until all have no overload
        while (hasOverload) {
            hasOverload = false;

            // Sequentially iterate over each block
            for (int fromBlockID = 0; fromBlockID < pGraph.k().value; fromBlockID++) {
                BlockID fromBlock = new BlockID(fromBlockID);
                BlockWeight currentOverload = blockOverload2(fromBlock);

                // Skip blocks with no overload and no underload
                if (currentOverload.value <= 0 && !isUnderloaded(pGraph, pCtx, fromBlock)) {
                    continue; // Block is balanced, so skip it
                }

                // If the block is still overloaded, move the remaining nodes forcefully
                if (currentOverload.value > 0) {
                    hasOverload = true; // Set the flag to true to indicate there is still overload
                    List<BlockID> targetBlocks = findUnderloadedBlocks(pGraph, pCtx, fromBlock);

                    // If there are no underloaded blocks, skip this block
                    if (targetBlocks.isEmpty()) {
                        continue; // No targets to move to
                    }

                    while (currentOverload.value > 0 && !targetBlocks.isEmpty()) {
                        // Select a node to move (even if it increases the edge cut)
                        NodeID u = selectNodeToMove(pGraph, fromBlock);
                        // Forcefully move the node to the best possible underloaded block
                        boolean moved = false;
                        for (BlockID toBlock : targetBlocks) {
                            if (isUnderloaded(pGraph, pCtx, toBlock) && moveNodeForcefully(u, fromBlock)) {
                                NodeWeight uWeight = pGraph.nodeWeight(u);
                                BlockWeight delta = new BlockWeight(Math.min(currentOverload.value, uWeight.value));
                                currentOverload = new BlockWeight(currentOverload.value - delta.value);
                                moved = true;
                                break; // Stop once we successfully move the node
                            }
                        }

                        // If we couldn't move the node, break the loop to avoid infinite loops
                        if (!moved) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private void forcefullyResolveUnderload(PartitionedGraph pGraph, PartitionContext pCtx) {
        boolean hasUnderload = true;

        // Continue iterating over the blocks until all underloaded blocks are resolved
        while (hasUnderload) {
            hasUnderload = false;

            // Sequentially iterate over each block
            for (int toBlockID = 0; toBlockID < pGraph.k().value; toBlockID++) {
                BlockID toBlock = new BlockID(toBlockID);
                long blockWeight = pGraph.blockWeight(toBlock).value;
                long minBlockWeight = pCtx.blockWeights.perfectlyBalanced(toBlockID).value - pCtx.absoluteEpsilon;

                // Skip blocks that are not underloaded
                if (blockWeight >= minBlockWeight) {
                    continue;
                }

                hasUnderload = true; // At least one block is underloaded

                // Find blocks to take nodes from
                List<BlockID> sourceBlocks = new ArrayList<>();
                for (BlockID fromBlock : pGraph.blocks()) {
                    if (!fromBlock.equals(toBlock)) {
                        long fromBlockWeight = pGraph.blockWeight(fromBlock).value;
                        long maxBlockWeight = pCtx.blockWeights.perfectlyBalanced(fromBlock.value).value + pCtx.absoluteEpsilon;
                        long minWeight = pCtx.blockWeights.perfectlyBalanced(fromBlock.value).value - pCtx.absoluteEpsilon;
                        // Ensure the source block is neither underloaded nor overloaded after transfer
                        if (fromBlockWeight > minWeight && fromBlockWeight <= maxBlockWeight) {
                            sourceBlocks.add(fromBlock);
                        }
                    }
                }

                // If no source blocks can provide nodes, skip this underloaded block
                if (sourceBlocks.isEmpty()) {
                    continue;
                }

                // Attempt to resolve underload by taking nodes from source blocks
                while (blockWeight < minBlockWeight && !sourceBlocks.isEmpty()) {
                    boolean moved = false;

                    for (BlockID fromBlock : sourceBlocks) {
                        NodeID u = selectNodeToMove(pGraph, fromBlock);

                        // Ensure moving the node will not overload or underload the source block
                        long fromBlockWeight = pGraph.blockWeight(fromBlock).value;
                        long maxFromBlockWeight = pCtx.blockWeights.perfectlyBalanced(fromBlock.value).value + pCtx.absoluteEpsilon;
                        long minFromBlockWeight = pCtx.blockWeights.perfectlyBalanced(fromBlock.value).value - pCtx.absoluteEpsilon;

                        NodeWeight uWeight = pGraph.nodeWeight(u);
                        if (fromBlockWeight - uWeight.value >= minFromBlockWeight &&
                                fromBlockWeight - uWeight.value <= maxFromBlockWeight) {

                            // Move the node forcefully
                            if (moveNodeForcefully2(u, fromBlock, toBlock)) {
                                blockWeight += uWeight.value;
                                moved = true;
                                break; // Stop once a node is successfully moved
                            }
                        }
                    }

                    // If no node could be moved, remove the source block from consideration
                    if (!moved) {
                        sourceBlocks.removeIf(block -> {
                            long currentBlockWeight = pGraph.blockWeight(block).value; // Unique variable name
                            long minAllowedWeight = pCtx.blockWeights.perfectlyBalanced(block.value).value - pCtx.absoluteEpsilon;
                            long maxAllowedWeight = pCtx.blockWeights.perfectlyBalanced(block.value).value + pCtx.absoluteEpsilon;

                            return currentBlockWeight <= minAllowedWeight || currentBlockWeight > maxAllowedWeight;
                        });
                    }

                }
            }
        }
    }
    private boolean moveNodeForcefully2(NodeID u, BlockID fromBlock, BlockID toBlock) {
        BlockWeight maxToWeight = pCtx.blockWeights.max(toBlock.value);

        // Attempt to move the node from `fromBlock` to `toBlock`
        return pGraph.move(u, fromBlock, toBlock, maxToWeight);
    }
    private BlockWeight blockOverload2(BlockID blockID) {
        long blockWeight = pGraph.blockWeight(blockID).value;
        long maxBlockWeight = pCtx.blockWeights.max(blockID.value).value;
        long overload = Math.max(0, blockWeight - maxBlockWeight);
        return new BlockWeight(overload);
    }
    private List<BlockID> findUnderloadedBlocks(PartitionedGraph pGraph, PartitionContext pCtx, BlockID fromBlock) {
        List<BlockID> underloadedBlocks = new ArrayList<>();

        for (BlockID blockID : pGraph.blocks()) {
            if (!blockID.equals(fromBlock)) {
                long blockWeight = pGraph.blockWeight(blockID).value;
                long maxBlockWeight = pCtx.blockWeights.max(blockID.value).value;
                if (blockWeight < maxBlockWeight) {
                    underloadedBlocks.add(blockID); // This block can accept more nodes
                }
            }
        }
        return underloadedBlocks;
    }
    private NodeID selectNodeToMove(PartitionedGraph pGraph, BlockID fromBlock) {
        // Retrieve the node with the highest priority from the PQ if available
        while (!pq.empty(fromBlock.value)) {
            NodeID candidate = pq.peekMaxId(fromBlock.value);
            if (!pGraph.block(candidate).equals(fromBlock)) {
                pq.popMax(fromBlock.value);  // Remove if the node is no longer in the block
            } else {
                return candidate;  // Return the valid candidate
            }
        }

        // If the PQ is empty, find a node that would increase the edge cut the least
        NodeID bestNode = null;
        double bestGain = Double.NEGATIVE_INFINITY;  // Gain is negative when moving a node increases the edge cut

        // Define a threshold for minimum acceptable edge cut loss
        double minEdgeCutLossThreshold = -5.0;  // Loss threshold (e.g., forcefully moving a node will cause <= 5 loss)

        // Iterate over all nodes in the block to calculate the gain for each node
        for (int u = 0; u < pGraph.n().value; u++) {
            NodeID node = new NodeID(u);
            if (!pGraph.block(node).equals(fromBlock)) {
                continue;  // Skip nodes that don't belong to the current block
            }

            // Compute the gain for this node
            Pair<BlockID, Double> gainPair = computeGain(node, fromBlock);
            double gain = gainPair.getValue();

            // Select the node with the best (least negative or highest positive) gain
            if (gain > bestGain) {
                bestGain = gain;
                bestNode = node;

                // If the gain is greater than the threshold (meaning a very minimal loss), return the node early
                if (gain >= minEdgeCutLossThreshold) {
                    return bestNode;  // No need to search further, as the node has minimal impact
                }
            }
        }

        return bestNode;  // Return the node that causes the least negative gain (or best gain)
    }


    private boolean moveNodeForcefully(NodeID u, BlockID fromBlock) {
        List<BlockID> targetBlocks = feasibleTargetBlocks.get(fromBlock);

        // If the target block list is null or empty, attempt to find new target blocks
        if (targetBlocks == null || targetBlocks.isEmpty()) {
            targetBlocks = findUnderloadedBlocks(pGraph, pCtx, fromBlock);

            // If we still don't find any valid target blocks, return false
            if (targetBlocks.isEmpty()) {
                return false;
            }
        }

        // Try moving the node to any feasible block, even if it results in a higher edge cut
        for (BlockID toBlock : targetBlocks) {
            if (isUnderloaded(pGraph, pCtx, toBlock)) {
                BlockWeight maxToWeight = pCtx.blockWeights.max(toBlock.value);

                // Use the existing move method to transfer the node
                if (pGraph.move(u, fromBlock, toBlock, maxToWeight)) {
                    return true;  // Move was successful
                }
            }
        }

        // If all moves fail, return false
        return false;
    }


    private boolean isUnderloaded(PartitionedGraph pGraph, PartitionContext pCtx, BlockID block) {
        long blockWeight = pGraph.blockWeight(block).value;
        long maxBlockWeight = pCtx.blockWeights.max(block.value).value;
        return blockWeight < maxBlockWeight;
    }


    private boolean moveNodeIfPossible(NodeID u, BlockID from, BlockID to) {
        boolean moved = pGraph.move(u, from, to, pCtx.blockWeights.max(to.value));
        if (moved) {
            if (gainCache != null) {
                gainCache.move(pGraph, u.value, from.value, to.value);
            }
            return true;
        }
        return false;
    }


    private boolean moveToRandomBlock(NodeID u) {
        // Get all feasible target blocks from the available blocks
        List<BlockID> allFeasibleTargetBlocks = new ArrayList<>();

        // Collect all non-empty feasible target blocks from the map
        feasibleTargetBlocks.forEach((blockID, blockList) -> {
            if (!blockList.isEmpty()) {
                allFeasibleTargetBlocks.addAll(blockList);
            }
        });

        // If no feasible target blocks are available, return false
        if (allFeasibleTargetBlocks.isEmpty()) {
            return false;
        }

        BlockID uBlock = pGraph.block(u);
        Random_shm random = Random_shm.getInstance();

        // Continue until a move is successful or all blocks are tried
        while (!allFeasibleTargetBlocks.isEmpty()) {
            // Select a random index in the list of feasible target blocks
            int n = allFeasibleTargetBlocks.size();
            int i = random.randomIndex(0, n);
            BlockID targetBlock = allFeasibleTargetBlocks.get(i);

            // Attempt to move the node to the target block
            if (moveNodeIfPossible(u, uBlock, targetBlock)) {
                return true;
            }

            // If the move fails, remove the target block from the list and continue
            Collections.swap(allFeasibleTargetBlocks, i, n - 1);
            allFeasibleTargetBlocks.remove(n - 1);
        }

        // Return false if no feasible move was possible
        return false;
    }


    public void initPQ() {
        // Shared list of local PQs (one for each block)
        List<List<DynamicBinaryHeap<NodeID, Double>>> sharedLocalPQ = new ArrayList<>(pGraph.k().value);
        List<List<BlockWeight>> sharedLocalPQWeight = new ArrayList<>(pGraph.k().value);

        for (int i = 0; i < pGraph.k().value; i++) {
            sharedLocalPQ.add(new ArrayList<>());      // For storing PQs per block
            sharedLocalPQWeight.add(new ArrayList<>()); // For storing PQ weights per block
        }

        // Step 1: Local priority queues for each block
        ParallelFor.parallelFor(0, pGraph.n().value, 1, (start, end) -> {
            // Thread-local PQs for this thread
            List<DynamicBinaryHeap<NodeID, Double>> localPQ = new ArrayList<>(pGraph.k().value);
            List<BlockWeight> localPQWeight = new ArrayList<>(pGraph.k().value);

            for (int i = 0; i < pGraph.k().value; i++) {
                localPQ.add(new DynamicBinaryHeap<>(Double::compare)); // Initialize local PQ
                localPQWeight.add(new BlockWeight(0)); // Initialize local PQ weight tracker
            }

            for (int u = start; u < end; u++) {
                NodeID nodeID = new NodeID(u);
                BlockID blockID = pGraph.block(nodeID);
                BlockWeight overload = blockOverload(blockID);

                if (overload.value > 0) { // Node in overloaded block
                    Pair<BlockID, Double> gainPair = computeGain(nodeID, blockID);
                    BlockID maxGainer = gainPair.getKey();
                    double relGain = gainPair.getValue();

                    boolean needMoreNodes = localPQWeight.get(blockID.value).value < overload.value;
                    if (needMoreNodes || localPQ.get(blockID.value).isEmpty() || relGain > localPQ.get(blockID.value).peekKey()) {
                        // If needed, pop and replace nodes with lower gain
                        if (!needMoreNodes) {
                            NodeWeight uWeight = pGraph.nodeWeight(nodeID);
                            NodeWeight minWeight = pGraph.nodeWeight(new NodeID(localPQ.get(blockID.value).peekId().value));
                            if (localPQWeight.get(blockID.value).value + uWeight.value - minWeight.value >= overload.value) {
                                localPQ.get(blockID.value).pop();
                            }
                        }

                        // Push the current node and its relative gain
                        localPQ.get(blockID.value).push(nodeID, relGain);
                        NodeWeight uWeight = pGraph.nodeWeight(nodeID);
                        localPQWeight.set(blockID.value, new BlockWeight(localPQWeight.get(blockID.value).value + uWeight.value));
                    }
                }
            }

            // After populating local PQs, add them to the sharedLocalPQ
            synchronized (sharedLocalPQ) {
                for (int i = 0; i < localPQ.size(); i++) {
                    sharedLocalPQ.get(i).add(localPQ.get(i));  // Collect local PQs for each block
                    sharedLocalPQWeight.get(i).add(localPQWeight.get(i));  // Collect local PQ weights
                }
            }
        });

        // Step 4: Clear the global PQ before merging
        pq.clear(); // Ensure the global PQ is empty before merging

        // Step 5: Merge shared local PQs into the global PQ
        ParallelFor.parallelFor(0, pGraph.k().value, 1, (start, end) -> {
            for (int b = start; b < end; b++) {
                BlockID blockID = new BlockID(b);
                if (blockOverload(blockID).value > 0) {
                    stats.numOverloadedBlocks.incrementAndGet();
                }

                // Reset the global PQ weight tracker for this block
                pqWeight.set(b, new BlockWeight(0));

                // Access shared local PQs and weights for the block 'b'
                List<DynamicBinaryHeap<NodeID, Double>> localPQs = sharedLocalPQ.get(b);
                List<BlockWeight> localPQWeights = sharedLocalPQWeight.get(b);

                // Merge local PQs into the global PQ
                for (int i = 0; i < localPQs.size(); i++) {
                    DynamicBinaryHeap<NodeID, Double> pqForBlock = localPQs.get(i);
                    for (DynamicBinaryHeap.HeapElement<NodeID, Double> element : pqForBlock.getElements()) {
                        NodeID u = element.id;
                        double relGain = element.key;

                        addToPQ(blockID, u, pGraph.nodeWeight(u), relGain);  // Ensure gains are transferred
                    }

                    // Accumulate the weights
                    pqWeight.set(b, new BlockWeight(pqWeight.get(b).value + localPQWeights.get(i).value));
                }
                /*
                 if (!pq.empty(b)) {
                    System.out.println("PQ " + b + ": weight=" + pqWeight.get(b).value + ", " +
                            pq.peekMinKey(b) + " < key < " + pq.peekMaxKey(b));
                } else {
                    System.out.println("PQ " + b + ": empty");
                }
                 */

            }
        });

        stats.totalPQSizes.set(pq.size());
    }





    private boolean addToPQ(BlockID b, NodeID u) {
        // Ensure the node 'u' belongs to block 'b'
        assert b.equals(pGraph.block(u)) : "Block ID mismatch for node u";

        // Compute the gain for moving 'u' to another block
        Pair<BlockID, Double> gainPair = computeGain(u, b);
        double relGain = gainPair.getValue();

        // Call the second method with the computed gain
        return addToPQ(b, u, pGraph.nodeWeight(u), relGain);
    }


    private boolean addToPQ(BlockID b, NodeID u, NodeWeight uWeight, double relGain) {
        // Ensure the node weight and block ID are correct
        assert uWeight.equals(pGraph.nodeWeight(u)) : "Node weight mismatch for node u";
        assert b.equals(pGraph.block(u)) : "Block ID mismatch for node u";
        if (pq.contains(u)) {
            // Node is already in the PQ, so do not add it again
            /*
            if (DEBUG) {
                System.out.println("Node " + u.value + " is already in the PQ for block " + b.value + ". Skipping...");
            }
             */

            return false;  // Node was not added because it's already in the PQ
        }
        // Check if the block's PQ can accommodate the node based on its weight and relative gain
        if (pqWeight.get(b.value).value < blockOverload(b).value || pq.empty(b.value) || relGain > pq.peekMinKey(b.value)) {
            // Debugging information if needed
            if (DEBUG) {
                /*
                System.out.println("Add node " + u.value + " to PQ with block " + b.value + ", PQ weight " + pqWeight.get(b.value).value + ", rel gain " + relGain);
                 */

            }

            // Push the node 'u' into the priority queue for block 'b' with the computed gain
            pq.push(b.value, u, relGain);

            pqWeight.set(b.value, new BlockWeight(pqWeight.get(b.value).value + uWeight.value));

            // If the new relative gain exceeds the current minimum in the PQ, adjust the PQ
            if (relGain > pq.peekMinKey(b.value)) {
                // Get the minimum node in the PQ and its weight
                NodeID minNode = pq.peekMinId(b.value);
                NodeWeight minWeight = pGraph.nodeWeight(minNode);

                // If removing the minimum node still satisfies the overload constraint, pop it
                if (pqWeight.get(b.value).value - minWeight.value >= blockOverload(b).value) {

                    pq.popMin(b.value);

                    pqWeight.set(b.value, new BlockWeight(pqWeight.get(b.value).value - minWeight.value));
                }
            }

            return true;  // Node was successfully added to the PQ
        }

        return false;  // Node could not be added to the PQ
    }


    private Pair<BlockID, Double> computeGain(NodeID u, BlockID uBlock) {
        // Get the node weight
        NodeWeight uWeight = pGraph.nodeWeight(u);

        // Use arrays to store mutable values
        final BlockID[] maxGainer = {uBlock};
        final EdgeWeight[] maxExternalGain = {new EdgeWeight(0)};
        final EdgeWeight[] internalDegree = {new EdgeWeight(0)};

        // Retrieve or initialize the rating map for uBlock
        synchronized (ratingMap) {
            RatingMap<BlockID, EdgeWeight> map = ratingMap.get(uBlock);

            if (map == null) {
                map = new RatingMap<>(pGraph.k().value); // Initialize with a reasonable size
                ratingMap.put(uBlock, map);
            }

            // Iterate over the neighbors of the node 'u'
            for (Edge edge : pGraph.neighbors(u)) {
                NodeID v = pGraph.edgeTarget(edge.getEdgeID());
                BlockID vBlock = pGraph.block(v);

                if (!uBlock.equals(vBlock) &&
                        pGraph.blockWeight(vBlock).value + uWeight.value <= pCtx.blockWeights.max(vBlock.value).value) {
                    map.execute(pGraph.degree(u).value, adjMap -> {
                        adjMap.put(vBlock, adjMap.getOrDefault(vBlock, new EdgeWeight(0))
                                .add(pGraph.edgeWeight(edge.getEdgeID())));
                    });
                } else if (uBlock.equals(vBlock)) {
                    internalDegree[0] = internalDegree[0].add(pGraph.edgeWeight(edge.getEdgeID()));
                }
            }

            // Select the block that maximizes the gain
            Random_shm random = Random_shm.getInstance();
            map.execute(pGraph.degree(u).value, adjMap -> {
                for (Map.Entry<BlockID, EdgeWeight> entry : adjMap.entrySet()) {
                    BlockID block = entry.getKey();
                    EdgeWeight gain = entry.getValue();

                    if (gain.compareTo(maxExternalGain[0]) > 0 ||
                            (gain.compareTo(maxExternalGain[0]) == 0 && random.randomBool())) {
                        maxGainer[0] = block;
                        maxExternalGain[0] = gain;
                    }
                }
                adjMap.clear();  // Clear the map after processing
            });
        }

        EdgeWeight gain = maxExternalGain[0].subtract(internalDegree[0]);
        double relativeGain = computeRelativeGain(gain.value, uWeight.value);

        return new Pair<>(maxGainer[0], relativeGain);
    }




    private void initFeasibleTargetBlocks() {
        // If statistics are enabled, increment the number of feasible target block initializations
        if (STATISTICS) {
            stats.numFeasibleTargetBlockInits.incrementAndGet();
        }

        // Clear all thread-local feasible target blocks
        feasibleTargetBlocks.forEach((blockID, blockList) -> blockList.clear());

        // Loop over all blocks in the graph
        for (BlockID b : pGraph.blocks()) {
            // Check if the block's weight is less than the perfectly balanced weight
            if (pGraph.blockWeight(b).value < pCtx.blockWeights.perfectlyBalanced(b.value).value) {
                // Add the block to the list of feasible target blocks
                feasibleTargetBlocks.computeIfAbsent(b, key -> new ArrayList<>()).add(b);
            }
        }
    }


    private double computeRelativeGain(long absoluteGain, long weight) {
        if (absoluteGain >= 0) {
            return absoluteGain * weight;
        } else {
            return 1.0 * absoluteGain / weight;
        }
    }

    private BlockWeight blockOverload(BlockID b) {
        int blockIndex = b.value; // Assuming BlockID has an integer field 'value'
        return new BlockWeight(Math.max(0, pGraph.blockWeight(b).value - pCtx.blockWeights.max(blockIndex).value));
    }

    /*
    public void trackMoves(SparseGainCache gainCache) {
        this.gainCache = gainCache;
    }
     */

}
