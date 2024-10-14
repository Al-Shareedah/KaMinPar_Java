package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.kaminpar_shm.initialPartitioning.InitialCoarsener.MemoryContext;
import org.alshar.common.Math.MathUtils;
import org.alshar.common.context.*;
import org.alshar.kaminpar_shm.Metrics;
import org.alshar.kaminpar_shm.PartitionUtils;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.common.Logger;
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
    public InitialPartitioner(Graph graph, Context ctx, BlockID finalK, MemoryContext mCtx, boolean useQueue) {
        this.m_ctx = mCtx;
        this.graph = graph;
        this.i_ctx = ctx.initialPartitioning;
        this.coarsener = new InitialCoarsener(graph, i_ctx.coarsening, m_ctx.coarsenerMCtx);

        BlockID[] finalKs = MathUtils.splitIntegral(finalK);

        this.p_ctx = PartitionUtils.createBipartitionContext(graph, finalKs[0], finalKs[1], ctx.partition);


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
