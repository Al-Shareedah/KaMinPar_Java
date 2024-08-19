package org.alshar.kaminpar_shm.refinement;
import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.GraphUtils.*;
import org.alshar.common.datastructures.Pair;
import org.alshar.common.datastructures.StaticArray;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.common.context.PartitionContext;
import org.alshar.kaminpar_shm.PartitionUtils;
import org.alshar.kaminpar_shm.coarsening.Coarsener;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.kaminpar_shm.initialPartitioning.InitialPartitioner;

import java.util.ArrayList;
import java.util.List;
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
        if (!coarsener.isEmpty()) {
            pGraph = coarsener.uncoarsen(pGraph);
            updatePartitionContext(currentPCtx, pGraph, inputPCtx.k);
        }
        return pGraph;
    }

    public static void refine(Refiner refiner, PartitionedGraph pGraph, PartitionContext currentPCtx) {
        refiner.initialize(pGraph);
        refiner.refine(pGraph, currentPCtx);
    }

    public static PartitionedGraph bipartition(Graph graph, BlockID finalK, Context inputCtx, GlobalInitialPartitionerMemoryPool  ipMCtxPool) {
        InitialPartitioner partitioner = new InitialPartitioner(graph, inputCtx, finalK, ipMCtxPool.local().get());
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

        // Perform the initial bipartition of the graph
        PartitionedGraph pGraph = bipartition(graph, finalK, inputCtx, ipMCtxPool);

        // Split k and finalK into two parts
        BlockID[] finalKs = splitIntegral(finalK);
        BlockID[] ks = splitIntegral(k);
        BlockID[] b = new BlockID[]{b0, b0.add(ks[0])};

        // Update the partition to reflect the bipartition
        for (int i = 0; i < partition.size(); i++) {
            BlockID block = partition.get(i);
            if (block.equals(b0)) {
                partition.set(i, b[pGraph.block(new NodeID(i)).value]);
            }
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


    public static void extendPartition(
            PartitionedGraph pGraph,
            BlockID kPrime,
            Context inputCtx,
            PartitionContext currentPCtx,
            SubgraphMemory subgraphMemory,
            TemporaryGraphExtractionBufferPool extractionPool,
            GlobalInitialPartitionerMemoryPool ipMCtxPool) {

        // Extract subgraphs from the partitioned graph
        SubgraphExtractionResult extraction = extractSubgraphs(pGraph, inputCtx.partition.k, subgraphMemory);

        // Initialize subgraph partitions
        StaticArray<StaticArray<BlockID>> subgraphPartitions = new StaticArray<>(extraction.subgraphs.size());
        for (int i = 0; i < extraction.subgraphs.size(); i++) {
            subgraphPartitions.set(i, new StaticArray<>(extraction.subgraphs.get(i).n().value));
        }

        final StaticArray<StaticArray<BlockID>> finalSubgraphPartitions = subgraphPartitions;
        final SubgraphMemory finalSubgraphMemory = subgraphMemory;
        final Context finalInputCtx = inputCtx;
        final SubgraphExtractionResult finalExtraction = extraction;
        int currentK = pGraph.k().value;
        // Parallel bipartitioning of subgraphs
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

        // Convert the StaticArray to List for use in copySubgraphPartitions
        List<StaticArray<BlockID>> subgraphPartitionsList = new ArrayList<>();
        for (int i = 0; i < finalSubgraphPartitions.size(); i++) {
            subgraphPartitionsList.add(finalSubgraphPartitions.get(i));
        }

        // Copy subgraph partitions into the main partitioned graph
        pGraph = copySubgraphPartitions(pGraph, subgraphPartitionsList, kPrime, inputCtx.partition.k, finalExtraction.nodeMapping);

        // Update the partition context
        updatePartitionContext(currentPCtx, pGraph, inputCtx.partition.k);
    }

    public static void extendPartition(
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
        extendPartition(pGraph, kPrime, inputCtx, currentPCtx, memory, extractionPool, ipMCtxPool);
    }

    public static boolean coarsenOnce(
            Coarsener coarsener,
            Graph graph,
            Context inputCtx,
            PartitionContext currentPCtx) {

        NodeWeight maxClusterWeight = PartitionUtils.computeMaxClusterWeight(inputCtx.coarsening, graph, inputCtx.partition);
        Pair<Graph, Boolean> result = coarsener.computeCoarseGraph(maxClusterWeight, new NodeID(0));

        Graph cGraph = result.getKey();
        boolean shrunk = result.getValue();

        if (shrunk) {
            currentPCtx.setup(cGraph);
        }

        return shrunk;
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