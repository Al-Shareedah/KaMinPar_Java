package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.BlockWeight;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.kaminpar_shm.initialPartitioning.InitialCoarsener.MemoryContext;
import org.alshar.common.Math.MathUtils;
import org.alshar.common.context.*;
import org.alshar.kaminpar_shm.Metrics;
import org.alshar.kaminpar_shm.PartitionUtils;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.common.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InitialPartitioner {
    // Separate MemoryContext structure
    public static class MemoryContext {
        InitialCoarsener.MemoryContext coarsenerMCtx = new InitialCoarsener.MemoryContext();
        InitialRefiner.MemoryContext refinerMCtx = new InitialRefiner.MemoryContext();
        PoolBipartitioner.MemoryContext poolMCtx = new PoolBipartitioner.MemoryContext();

        public long memoryInKB() {
            return coarsenerMCtx.memoryInKB() + refinerMCtx.memoryInKB() + poolMCtx.memoryInKB();
        }
    }

    private MemoryContext m_ctx;
    private final Graph graph;
    private final InitialPartitioningContext i_ctx;
    public PartitionContext p_ctx;
    private InitialCoarsener coarsener;
    private InitialRefiner refiner;
    private int numBipartitionRepetitions;

    public InitialPartitioner(Graph graph, Context ctx, BlockID finalK) {
        this(graph, ctx, finalK, new MemoryContext());
    }

    public InitialPartitioner(Graph graph, Context ctx, BlockID finalK, MemoryContext mCtx) {
        this.m_ctx = mCtx;
        this.graph = graph;
        this.i_ctx = ctx.initialPartitioning;
        this.coarsener = new InitialCoarsener(graph, i_ctx.coarsening, m_ctx.coarsenerMCtx);

        BlockID[] finalKs = MathUtils.splitIntegral(finalK);
        this.p_ctx = PartitionUtils.createBipartitionContext(graph, finalKs[0], finalKs[1], ctx.partition);

        this.refiner = InitialRefinerFactory.createInitialRefiner(graph, p_ctx, i_ctx.refinement, m_ctx.refinerMCtx);

        this.numBipartitionRepetitions = (int) Math.ceil(i_ctx.repetitionMultiplier * finalK.getValue() / MathUtils.ceilLog2(ctx.partition.k.value));
    }
    public InitialPartitioner(Graph graph, Context ctx, BlockID finalK, MemoryContext mCtx, boolean TwoWay) {
        this.m_ctx = mCtx;
        this.graph = graph;
        this.i_ctx = ctx.initialPartitioning;
        this.coarsener = new InitialCoarsener(graph, i_ctx.coarsening, m_ctx.coarsenerMCtx);

        BlockID[] finalKs = MathUtils.splitIntegral(finalK);

        this.p_ctx = PartitionUtils.createInitialBipartitionContext(graph, finalKs[0], finalKs[1], ctx.partition);


        this.refiner = InitialRefinerFactory.createInitialRefiner(graph, p_ctx, i_ctx.refinement, m_ctx.refinerMCtx);

        this.numBipartitionRepetitions = (int) Math.ceil(i_ctx.repetitionMultiplier * finalK.getValue() / MathUtils.ceilLog2(ctx.partition.k.value));
    }


    public MemoryContext free() {
        m_ctx.refinerMCtx = refiner.free();
        m_ctx.coarsenerMCtx = coarsener.free();
        return m_ctx;
    }

    public PartitionedGraph partition() {
        Graph cGraph = coarsen();

        //Logger.log("Calling bipartitioner on coarsest graph with n=" + cGraph.n().value + " m=" + cGraph.m().value);
        PoolBipartitionerFactory factory = new PoolBipartitionerFactory();
        PoolBipartitioner bipartitioner = factory.create(cGraph, p_ctx, i_ctx, m_ctx.poolMCtx);
        bipartitioner.setNumRepetitions(numBipartitionRepetitions);
        PartitionedGraph pGraph = bipartitioner.bipartition();
        m_ctx.poolMCtx = bipartitioner.free();

        //Logger.log("Bipartitioner result: cut=" + Metrics.edgeCutSeq(pGraph).value + " imbalance=" + Metrics.imbalance(pGraph) + " feasible=" + Metrics.isFeasible(pGraph, p_ctx));

        return uncoarsen(pGraph);
    }
    public PartitionedGraph partitionRecursive() {
        Graph cGraph = coarsen();
        // Configure p_ctx with the closest matching block weights
        configurePartitionContext(cGraph);
        //Logger.log("Calling bipartitioner on coarsest graph with n=" + cGraph.n().value + " m=" + cGraph.m().value);
        PoolBipartitionerFactory factory = new PoolBipartitionerFactory();
        PoolBipartitioner bipartitioner = factory.create(cGraph, p_ctx, i_ctx, m_ctx.poolMCtx);
        bipartitioner.setNumRepetitions(numBipartitionRepetitions);
        PartitionedGraph pGraph = bipartitioner.bipartition();
        m_ctx.poolMCtx = bipartitioner.free();

        //Logger.log("Bipartitioner result: cut=" + Metrics.edgeCutSeq(pGraph).value + " imbalance=" + Metrics.imbalance(pGraph) + " feasible=" + Metrics.isFeasible(pGraph, p_ctx));

        return uncoarsen(pGraph);
    }

    private void configurePartitionContext(Graph cGraph) {
        long target = cGraph.totalNodeWeight().value;
        List<List<BlockWeight>> combinedBlockWeights = p_ctx.combinedBlockWeights;

        // Flatten all block weights into a single list to simplify pair-searching.
        // Keep track of their original positions if needed, but here we only need the values.
        List<BlockWeight> allWeights = new ArrayList<>();
        for (List<BlockWeight> list : combinedBlockWeights) {
            allWeights.addAll(list);
        }

        // If we must ensure two weights, but we have fewer than 2 total weights, throw an error
        if (allWeights.size() < 2) {
            throw new IllegalStateException("Not enough block weights to find a pair.");
        }

        long closestDifference = Long.MAX_VALUE;
        BlockWeight bestA = null;
        BlockWeight bestB = null;

        // Use a set to avoid testing the same pair more than once.
        // We'll store pairs of indices to ensure uniqueness.
        Set<String> testedPairs = new HashSet<>();

        // Try all unique pairs
        for (int i = 0; i < allWeights.size(); i++) {
            for (int j = i + 1; j < allWeights.size(); j++) {
                // Generate a key for the pair (assuming order doesn't matter, use sorted indices)
                String pairKey = i + "-" + j;
                if (testedPairs.contains(pairKey)) {
                    continue; // skip already tested pair
                }
                testedPairs.add(pairKey);

                long sum = allWeights.get(i).value + allWeights.get(j).value;
                long diff = Math.abs(target - sum);

                if (diff < closestDifference) {
                    closestDifference = diff;
                    bestA = allWeights.get(i);
                    bestB = allWeights.get(j);
                }
            }
        }

        // Ensure we found a valid pair
        if (bestA == null || bestB == null) {
            throw new IllegalStateException("No suitable block weights found to match the total node weight.");
        }

        // Set perfectly balanced block weights
        p_ctx.blockWeights.perfectlyBalancedBlockWeights = new ArrayList<>();
        p_ctx.blockWeights.perfectlyBalancedBlockWeights.add(new BlockWeight(bestA.value));
        p_ctx.blockWeights.perfectlyBalancedBlockWeights.add(new BlockWeight(bestB.value));

        // Set max block weights by adding absoluteEpsilon
        p_ctx.blockWeights.maxBlockWeights = new ArrayList<>();
        p_ctx.blockWeights.maxBlockWeights.add(new BlockWeight(bestA.value + p_ctx.absoluteEpsilon));
        p_ctx.blockWeights.maxBlockWeights.add(new BlockWeight(bestB.value + p_ctx.absoluteEpsilon));
    }

    private Graph coarsen() {
        CoarseningContext cCtx = new CoarseningContext();
        cCtx.contractionLimit = i_ctx.coarsening.contractionLimit;
        cCtx.clusterWeightLimit = i_ctx.coarsening.clusterWeightLimit;
        cCtx.clusterWeightMultiplier = i_ctx.coarsening.clusterWeightMultiplier;
        NodeWeight maxClusterWeight = PartitionUtils.computeMaxClusterWeight(cCtx, graph, p_ctx);

        Graph cGraph = graph;
        boolean shrunk = true;

        //Logger.log("Coarsen: n=" + cGraph.n().value + " m=" + cGraph.m().value);

        while (shrunk && cGraph.n().getValue() > cCtx.contractionLimit) {
            Graph newCGraph = coarsener.coarsen(StaticMaxClusterWeight.of(maxClusterWeight));
            shrunk = newCGraph != cGraph;

            //Logger.log("-> n=" + newCGraph.n().value + " m=" + newCGraph.m().value + " maxClusterWeight=" + maxClusterWeight.value + (shrunk ? "" : " ==> terminate"));

            if (shrunk) {
                cGraph = newCGraph;
            }
        }

        return cGraph;
    }

    private PartitionedGraph uncoarsen(PartitionedGraph pGraph) {
        //Logger.log("Uncoarsen: n=" + pGraph.n().value + " m=" + pGraph.m().value);

        while (!coarsener.empty()) {
            pGraph = coarsener.uncoarsen(pGraph);
            refiner.initialize(pGraph.getGraph());
            refiner.refine(pGraph, p_ctx);

            //Logger.log("-> n=" + pGraph.n().value + " m=" + pGraph.m().value + " cut=" + Metrics.edgeCutSeq(pGraph).value + " imbalance=" + Metrics.imbalance(pGraph) + " feasible=" + Metrics.isFeasible(pGraph, p_ctx));
        }

        return pGraph;
    }
}
