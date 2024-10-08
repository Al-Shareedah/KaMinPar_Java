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
        if (true) {
            return true; // No overload means no refinement needed
        }

        // Calculate the initial edge cut (only for debugging)
        //EdgeWeight initialCut = DEBUG ? Metrics.edgeCut(pGraph) : null;

        // Initialize the priority queue
        initPQ();

        // Perform the balancing round
        BlockWeight delta = performRound();

        // Calculate the new overload after the balancing round
        NodeWeight newOverload = new NodeWeight(initialOverload.value - delta.value);

        // Print debug information if necessary
        if (DEBUG) {
            EdgeWeight newCut = Metrics.edgeCut(pGraph);
            //System.out.println("-> Balancer: cut=" + initialCut + ", new cut=" + newCut);
        }

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

        // Thread-local tracking of overload delta
        ThreadLocal<BlockWeight> overloadDelta = ThreadLocal.withInitial(() -> new BlockWeight(0));

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
                            System.out.println("Block " + blockIDFrom.value + " with overload: " + currentOverload + ": " +
                                    feasibleList.size() + " feasible target blocks and " +
                                    pq.size(blockIDFrom.value) + " nodes in PQ. Total weight of PQ is " + pqWeight.get(blockIDFrom.value).value);
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
                            overloadDelta.set(new BlockWeight(overloadDelta.get().value + delta.value));

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
        BlockWeight globalOverloadDelta = new BlockWeight(overloadDelta.get().value);
        return globalOverloadDelta;
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
        // Get the feasible target blocks for the current thread
        List<BlockID> feasibleTargetBlocksList = feasibleTargetBlocks.get(Thread.currentThread().getId());

        BlockID uBlock = pGraph.block(u);
        Random_shm random = Random_shm.getInstance();
        while (!feasibleTargetBlocksList.isEmpty()) {
            // Select a random index in the feasible target blocks list
            int n = feasibleTargetBlocksList.size();
            int i = random.randomIndex(0, n);
            BlockID targetBlock = feasibleTargetBlocksList.get(i);

            // Attempt to move the node to the target block
            if (moveNodeIfPossible(u, uBlock, targetBlock)) {
                return true;
            }

            // If the move fails, remove the target block from the list and continue
            Collections.swap(feasibleTargetBlocksList, i, n - 1);
            feasibleTargetBlocksList.remove(n - 1);
        }

        // Return false if no feasible move was possible
        return false;
    }


    public void initPQ() {
        // Step 1: Local priority queues for each block
        int k = pGraph.k().value;
        ThreadLocal<List<DynamicBinaryHeap<NodeID, Double>>> localPQ = ThreadLocal.withInitial(() -> {
            List<DynamicBinaryHeap<NodeID, Double>> pqs = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                pqs.add(new DynamicBinaryHeap<>(Double::compare));
            }
            return pqs;
        });

        ThreadLocal<List<NodeWeight>> localPQWeight = ThreadLocal.withInitial(() -> {
            List<NodeWeight> pqWeights = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                pqWeights.add(new NodeWeight(0));
            }
            return pqWeights;
        });

        // Step 2: Reset the marker
        marker.reset();

        // Step 3: Fill thread-local PQs
        ParallelFor.parallelFor(0, pGraph.n().value, 1, (start, end) -> {
            List<DynamicBinaryHeap<NodeID, Double>> pq = localPQ.get();
            List<NodeWeight> pqWeight = localPQWeight.get();

            for (int u = start; u < end; u++) {
                NodeID nodeID = new NodeID(u);
                BlockID blockID = pGraph.block(nodeID);
                BlockWeight overload = blockOverload(blockID);

                if (overload.value > 0) { // Node in overloaded block
                    Pair<BlockID, Double> gainPair = computeGain(nodeID, blockID);
                    BlockID maxGainer = gainPair.getKey();
                    double relGain = gainPair.getValue();

                    boolean needMoreNodes = pqWeight.get(blockID.value).value < overload.value;
                    if (needMoreNodes || pq.get(blockID.value).isEmpty() || relGain > pq.get(blockID.value).peekKey()) {
                        if (!needMoreNodes) {
                            NodeWeight uWeight = pGraph.nodeWeight(nodeID);
                            NodeWeight minWeight = pGraph.nodeWeight(new NodeID(pq.get(blockID.value).peekId().value));
                            if (pqWeight.get(blockID.value).value + uWeight.value - minWeight.value >= overload.value) {
                                pq.get(blockID.value).pop();
                            }
                        }
                        pq.get(blockID.value).push(nodeID, relGain);
                        marker.set(u, 0, false);
                    }
                }
            }
        });

        // Step 4: Clear the global PQ
        pq.clear();

        // Step 5: Merge thread-local PQs into the global PQ
        ParallelFor.parallelFor(0, k, 1, (start, end) -> {
            for (int b = start; b < end; b++) {
                BlockID blockID = new BlockID(b);
                if (blockOverload(blockID).value > 0) {
                    stats.numOverloadedBlocks.incrementAndGet();
                }

                pqWeight.set(b, new BlockWeight(0));

                for (DynamicBinaryHeap<NodeID, Double> pqForBlock : localPQ.get()) {
                    for (DynamicBinaryHeap.HeapElement<NodeID, Double> element : pqForBlock.getElements()) {
                        NodeID u = element.id;
                        double relGain = element.key;
                        addToPQ(blockID, u, pGraph.nodeWeight(u), relGain);
                    }
                }

                if (!pq.empty(b)) {
                    System.out.println("PQ " + b + ": weight=" + pqWeight.get(b).value + ", " +
                            pq.peekMinKey(b) + " < key < " + pq.peekMaxKey(b));
                } else {
                    System.out.println("PQ " + b + ": empty");
                }
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

        // Check if the block's PQ can accommodate the node based on its weight and relative gain
        if (pqWeight.get(b.value).value < blockOverload(b).value || pq.empty(b.value) || relGain > pq.peekMinKey(b.value)) {
            // Debugging information if needed
            if (DEBUG) {
                System.out.println("Add node " + u + " to PQ with block " + b + ", PQ weight " + pqWeight.get(b.value) + ", rel gain " + relGain);
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
        RatingMap<BlockID, EdgeWeight> map = ratingMap.get(uBlock);

        if (map == null) {
            // Initialize with a reasonable size (using pGraph.k() as the maximum number of blocks)
            map = new RatingMap<>(pGraph.k().value);
            ratingMap.put(uBlock, map);
        }

        // Iterate over the neighbors of the node 'u'
        for (Edge edge : pGraph.neighbors(u)) {
            NodeID v = pGraph.edgeTarget(edge.getEdgeID());
            BlockID vBlock = pGraph.block(v);

            // If 'v' is in a different block and the move would not overload 'vBlock'
            if (!uBlock.equals(vBlock) &&
                    pGraph.blockWeight(vBlock).value + uWeight.value <= pCtx.blockWeights.max(vBlock.value).value) {
                // Accumulate external degree for this adjacent block
                map.execute(pGraph.degree(u).value, adjMap ->
                        adjMap.put(vBlock, adjMap.getOrDefault(vBlock, new EdgeWeight(0)).add(pGraph.edgeWeight(edge.getEdgeID()))));
            } else if (uBlock.equals(vBlock)) {
                // Accumulate internal degree
                internalDegree[0] = internalDegree[0].add(pGraph.edgeWeight(edge.getEdgeID()));
            }
        }

        // Select the block that maximizes the gain
        Random_shm random = Random_shm.getInstance();  // Assuming you have a Random class instance
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
            // Clear the map after processing
            adjMap.clear();
        });

        // Compute the absolute and relative gain based on the internal and external degrees
        EdgeWeight gain = maxExternalGain[0].subtract(internalDegree[0]);
        double relativeGain = computeRelativeGain(gain.value, uWeight.value);

        // Return the block with the best gain and the relative gain
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
