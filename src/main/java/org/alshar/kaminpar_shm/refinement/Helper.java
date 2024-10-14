package org.alshar.kaminpar_shm.refinement;
import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.GraphUtils.*;
import org.alshar.common.Logger;
import org.alshar.common.datastructures.*;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.timer.Timer_km;
import org.alshar.kaminpar_shm.PartitionUtils;
import org.alshar.kaminpar_shm.coarsening.Coarsener;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.kaminpar_shm.initialPartitioning.InitialPartitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static org.alshar.common.GraphUtils.SubgraphExtractor.*;
import static org.alshar.common.Math.MathUtils.splitIntegral;
import static org.alshar.kaminpar_shm.PartitionUtils.computeFinalK;
import static org.alshar.kaminpar_shm.PartitionUtils.computeMaxClusterWeight;

public class Helper {

    public static void updatePartitionContext(PartitionContext currentPCtx, PartitionedGraph pGraph, BlockID inputK) {
        currentPCtx.setup(pGraph.getGraph());
        currentPCtx.k = pGraph.k();
        currentPCtx.blockWeights.setup(currentPCtx, inputK.value);
    }

    public static PartitionedGraph uncoarsenOnce(Coarsener coarsener, PartitionedGraph pGraph, PartitionContext currentPCtx, PartitionContext inputPCtx) {
        // Start timing the "Uncoarsening" process
        try (var timer = Timer_km.global().startScopedTimer("Uncoarsening")) {

            if (!coarsener.isEmpty()) {
                pGraph = coarsener.uncoarsen(pGraph);
                updatePartitionContext(currentPCtx, pGraph, inputPCtx.k);
            }

            return pGraph;
        }
    }


    public static void refine(Refiner refiner, PartitionedGraph pGraph, PartitionContext currentPCtx) {
        try (var refinementTimer = Timer_km.global().startScopedTimer("Refinement")) {
            refiner.initialize(pGraph);
            refiner.refine(pGraph, currentPCtx);
        }
    }


    public static PartitionedGraph bipartition(Graph graph, BlockID finalK, Context inputCtx, GlobalInitialPartitionerMemoryPool ipMCtxPool) {
        // Create the InitialPartitioner
        InitialPartitioner partitioner = new InitialPartitioner(graph, inputCtx, finalK, ipMCtxPool.local().get());

        // Access the block constraints from inputCtx.partition without using poll
        Queue<BlockWeight> blockConstraints = inputCtx.partition.blockConstraints;

        if (blockConstraints != null && !blockConstraints.isEmpty()) {
            // Copy the block constraints into a list
            List<BlockWeight> blockConstraintList = new ArrayList<>(blockConstraints);

            // Combine block weights into 2 blocks
            List<BlockWeight> combinedPerfectlyBalancedWeights = new ArrayList<>();
            long firstBlockWeightValue = 0;
            long secondBlockWeightValue = 0;

            // Iterate over the copied list and combine all block weights into 2 sums
            for (int i = 0; i < blockConstraintList.size(); i++) {
                if (i % 2 == 0) {
                    firstBlockWeightValue += blockConstraintList.get(i).value;
                } else {
                    secondBlockWeightValue += blockConstraintList.get(i).value;
                }
            }

            combinedPerfectlyBalancedWeights.add(new BlockWeight(firstBlockWeightValue));
            combinedPerfectlyBalancedWeights.add(new BlockWeight(secondBlockWeightValue));

            // Now calculate the max block weights
            List<BlockWeight> combinedMaxWeights = new ArrayList<>();
            double epsilon = partitioner.p_ctx.epsilon;
            NodeWeight maxNodeWeight = partitioner.p_ctx.maxNodeWeight;

            long maxFirstBlockWeight = (long) ((1.0 + epsilon) * firstBlockWeightValue);
            long maxSecondBlockWeight = (long) ((1.0 + epsilon) * secondBlockWeightValue);

            if (maxNodeWeight.value == 1) {
                combinedMaxWeights.add(new BlockWeight(maxFirstBlockWeight));
                combinedMaxWeights.add(new BlockWeight(maxSecondBlockWeight));
            } else {
                combinedMaxWeights.add(new BlockWeight(Math.max(maxFirstBlockWeight, firstBlockWeightValue + maxNodeWeight.value)));
                combinedMaxWeights.add(new BlockWeight(Math.max(maxSecondBlockWeight, secondBlockWeightValue + maxNodeWeight.value)));
            }

            // Assign the combined block weights to the partitioner's context
            partitioner.p_ctx.blockWeights.perfectlyBalancedBlockWeights = combinedPerfectlyBalancedWeights;
            partitioner.p_ctx.blockWeights.maxBlockWeights = combinedMaxWeights;

            // Assign the combined block weights and max block weights to the new variables
            inputCtx.partition.bipartition_blockWeights[0] = new BlockWeight(firstBlockWeightValue);
            inputCtx.partition.bipartition_blockWeights[1] = new BlockWeight(secondBlockWeightValue);
            inputCtx.partition.bipartition_MaxblockWeights[0] = new BlockWeight(maxFirstBlockWeight);
            inputCtx.partition.bipartition_MaxblockWeights[1] = new BlockWeight(maxSecondBlockWeight);


        }

        // Perform the partitioning
        PartitionedGraph pGraph = partitioner.partition();

        // Return the partitioner to the memory pool
        ipMCtxPool.local().put(partitioner.free());

        return pGraph;
    }

    public static PartitionedGraph bipartitionWithQueue(
            Graph graph, BlockID finalK, Context inputCtx, GlobalInitialPartitionerMemoryPool ipMCtxPool) {

        // Call the modified InitialPartitioner constructor
        InitialPartitioner partitioner = new InitialPartitioner(graph, inputCtx, finalK, ipMCtxPool.local().get(), true);
        PartitionedGraph pGraph = partitioner.partition();
        ipMCtxPool.local().put(partitioner.free());

        return pGraph;
    }


    public static void extendPartitionRecursive(
            Graph graph,
            StaticArray<BlockID> partition,
            BlockID b0,
            BlockID k,
            BlockID finalK,
            Context inputCtx,
            SubgraphMemory subgraphMemory,
            SubgraphMemoryStartPosition position,
            TemporaryGraphExtractionBufferPool extractionPool,
            GlobalInitialPartitionerMemoryPool ipMCtxPool) {

        // Ensure that the number of blocks (k) is greater than 1
        if (k.value <= 1) {
            throw new IllegalArgumentException("Block count k must be greater than 1.");
        }
        PartitionedGraph pGraph;

        // Start a timer for the bipartition process
        try (var bipartitionTimer = Timer_km.global().startScopedTimer("Extend Partition")) {
            // Perform the initial bipartition of the graph
            pGraph = bipartitionWithQueue(graph, finalK, inputCtx, ipMCtxPool);
        }

        // Split k and finalK into two parts
        BlockID[] finalKs = splitIntegral(finalK);
        BlockID[] ks = splitIntegral(k);
        BlockID[] b = new BlockID[]{b0, b0.add(ks[0])};


        // Update the partition to reflect the bipartition
        int node = 0;
        for (int i = 0; i < partition.size(); i++) {
            BlockID block = partition.get(i);

            // Check for block matching b0 and ensure node is within bounds
            if (block != null && block.equals(b0) && node < pGraph.n().value) {
                partition.set(i, b[pGraph.block(new NodeID(node)).value]);
                node++;
            } else if (block == null) {
                // Handle the case where the block is null (optional, depending on your logic)
                System.out.println("Warning: Encountered null block at position " + i);
            }
        }

        // Ensure that all nodes have been processed
        int processedNodes = node;
        if (processedNodes != pGraph.n().value) {
            throw new IllegalStateException("Mismatch in the number of processed nodes and partition size.");
        }

        // Check if further recursion is needed
        if (k.value > 2) {
            // Extract subgraphs from the partitioned graph
            SequentialSubgraphExtractionResult extraction = extractSubgraphsSequential(
                    pGraph,
                    convertBlockIDArrayToIntArray(finalKs),
                    position,
                    subgraphMemory,
                    extractionPool.local()
            );

            List<Graph> subgraphs = extraction.subgraphs;
            List<SubgraphMemoryStartPosition> positions = extraction.positions;

            // Recursively extend the partitions for each subgraph
            for (int i = 0; i < 2; i++) {
                if (ks[i].value > 1) {
                    extendPartitionRecursive(
                            subgraphs.get(i),
                            partition,
                            b[i],
                            ks[i],
                            finalKs[i],
                            inputCtx,
                            subgraphMemory,
                            positions.get(i),
                            extractionPool,
                            ipMCtxPool
                    );
                }
            }
        }
    }
    private static int[] convertBlockIDArrayToIntArray(BlockID[] blockIDArray) {
        int[] intArray = new int[blockIDArray.length];
        for (int i = 0; i < blockIDArray.length; i++) {
            intArray[i] = blockIDArray[i].value;
        }
        return intArray;
    }


    public static PartitionedGraph extendPartition(
            PartitionedGraph pGraph,
            BlockID kPrime,
            Context inputCtx,
            PartitionContext currentPCtx,
            SubgraphMemory subgraphMemory,
            TemporaryGraphExtractionBufferPool extractionPool,
            GlobalInitialPartitionerMemoryPool ipMCtxPool) {

        try (var initialPartitioningTimer = Timer_km.global().startScopedTimer("Initial partitioning")) {

            // Extract subgraphs from the partitioned graph
            SubgraphExtractionResult extraction;
            try (var extractionTimer = Timer_km.global().startScopedTimer("Extract subgraphs")) {
                extraction = extractSubgraphs(pGraph, inputCtx.partition.k, subgraphMemory);
            }

            // Initialize subgraph partitions
            StaticArray<StaticArray<BlockID>> subgraphPartitions;
            try (var allocationTimer = Timer_km.global().startScopedTimer("Allocation")) {
                subgraphPartitions = new StaticArray<>(extraction.subgraphs.size());
                for (int i = 0; i < extraction.subgraphs.size(); i++) {
                    subgraphPartitions.set(i, new StaticArray<>(extraction.subgraphs.get(i).n().value));
                }
            }

            final StaticArray<StaticArray<BlockID>> finalSubgraphPartitions = subgraphPartitions;
            final SubgraphMemory finalSubgraphMemory = subgraphMemory;
            final Context finalInputCtx = inputCtx;
            final SubgraphExtractionResult finalExtraction = extraction;
            int currentK = pGraph.k().value;

            // Initialize each partition in finalSubgraphPartitions to BlockID(0)
            for (int i = 0; i < finalSubgraphPartitions.size(); i++) {
                StaticArray<BlockID> subgraphPartition = finalSubgraphPartitions.get(i);
                for (int j = 0; j < subgraphPartition.size(); j++) {
                    subgraphPartition.set(j, new BlockID(0)); // Initialize to BlockID(0)
                }
            }

            // Parallel bipartitioning of subgraphs
            try (var bipartitioningTimer = Timer_km.global().startScopedTimer("Bipartitioning")) {
                ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                    @Override
                    protected void compute() {
                        for (BlockID b = new BlockID(0); b.value < finalExtraction.subgraphs.size(); b = b.add(1)) {
                            Graph subgraph = finalExtraction.subgraphs.get(b.value);
                            BlockID finalKb = new BlockID(computeFinalK(b.value, currentK, finalInputCtx.partition.k.value));
                            BlockID subgraphK = (kPrime.equals(finalInputCtx.partition.k)) ? finalKb : new BlockID(kPrime.value / currentK);

                            if (subgraphK.value > 1) {
                                extendPartitionRecursive(
                                        subgraph,
                                        finalSubgraphPartitions.get(b.value),
                                        new BlockID(0),
                                        subgraphK,
                                        finalKb,
                                        finalInputCtx,
                                        finalSubgraphMemory,
                                        finalExtraction.positions.get(b.value),
                                        extractionPool,
                                        ipMCtxPool
                                );
                            }
                        }
                    }
                });
            }

            // Convert the StaticArray to List for use in copySubgraphPartitions
            List<StaticArray<BlockID>> subgraphPartitionsList = new ArrayList<>();
            for (int i = 0; i < finalSubgraphPartitions.size(); i++) {
                subgraphPartitionsList.add(finalSubgraphPartitions.get(i));
            }

            // Copy subgraph partitions into the main partitioned graph
            try (var copySubgraphPartitionsTimer = Timer_km.global().startScopedTimer("Copy subgraph partitions")) {
                pGraph = copySubgraphPartitions(pGraph, subgraphPartitionsList, kPrime, inputCtx.partition.k, finalExtraction.nodeMapping);
            }

            // Update the partition context
            updatePartitionContext(currentPCtx, pGraph, inputCtx.partition.k);
        }

        return pGraph;
    }


    public static PartitionedGraph extendPartition(
            PartitionedGraph pGraph,
            BlockID kPrime,
            Context inputCtx,
            PartitionContext currentPCtx,
            TemporaryGraphExtractionBufferPool extractionPool,
            GlobalInitialPartitionerMemoryPool ipMCtxPool) {

        // Create and resize subgraph memory
        SubgraphMemory memory = new SubgraphMemory();
        memory.resize(pGraph.n(), inputCtx.partition.k, pGraph.m(), pGraph.getGraph().nodeWeighted(), pGraph.getGraph().edgeWeighted());

        // Call the overloaded version of extendPartition with memory
        pGraph = extendPartition(pGraph, kPrime, inputCtx, currentPCtx, memory, extractionPool, ipMCtxPool);

        return pGraph;

    }

    public static boolean coarsenOnce(
            Coarsener coarsener,
            Graph graph,
            Context inputCtx,
            PartitionContext currentPCtx) {

        try (var coarseningTimer = Timer_km.global().startScopedTimer("Coarsening")) {
            // Calculate the maximum cluster weight
            NodeWeight maxClusterWeight = PartitionUtils.computeMaxClusterWeight(inputCtx.coarsening, graph, inputCtx.partition);

            // Compute the coarse graph
            Pair<Graph, Boolean> result = coarsener.computeCoarseGraph(maxClusterWeight, new NodeID(0));

            // Extract the results
            Graph cGraph = result.getKey();
            boolean shrunk = result.getValue();

            // If the graph was shrunk, update the partition context
            if (shrunk) {
                currentPCtx.setup(cGraph);
            }

            // Return whether the graph was shrunk
            return shrunk;
        }
    }


    public static BlockID computeKForN(NodeID n, Context inputCtx) {
        if (n.value < 2 * inputCtx.coarsening.contractionLimit) {
            return new BlockID(2);
        }
        BlockID kPrime = new BlockID(1 << (int) Math.ceil(Math.log(n.value / inputCtx.coarsening.contractionLimit) / Math.log(2)));
        return new BlockID(Math.max(2, Math.min(kPrime.value, inputCtx.partition.k.value)));
    }

    // Implementations for computeNumCopies, selectBest, and computeNumThreadsForParallelIP omitted for brevity.
}