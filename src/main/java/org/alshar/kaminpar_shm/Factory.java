package org.alshar.kaminpar_shm;
import org.alshar.Graph;
import org.alshar.Context;
import org.alshar.common.context.*;
import org.alshar.common.enums.*;
import org.alshar.kaminpar_shm.coarsening.ClusteringCoarsener;
import org.alshar.kaminpar_shm.coarsening.Coarsener;
import org.alshar.kaminpar_shm.coarsening.LPClustering;
import org.alshar.kaminpar_shm.refinement.GreedyBalancer;
import org.alshar.kaminpar_shm.refinement.LabelPropagationRefiner;
import org.alshar.kaminpar_shm.refinement.MultiRefiner;
import org.alshar.kaminpar_shm.refinement.Refiner;


import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
public class Factory {
    public static Optional<Partitioner> createPartitioner(Graph graph, Context ctx) {
        if (ctx.partitioning.mode == PartitioningMode.DEEP) {
            return Optional.of((Partitioner) new DeepMultilevelPartitioner(graph, ctx));
        } else {
            return Optional.empty();
        }
    }

    public static Coarsener createCoarsener(Graph graph, CoarseningContext cCtx) {
        switch (cCtx.algorithm) {
            case LABEL_PROPAGATION:
                return new ClusteringCoarsener(
                        new LPClustering(graph.n(), cCtx), graph, cCtx
                );
            default:
                throw new IllegalArgumentException("Unsupported clustering algorithm: " + cCtx.algorithm);
        }
    }
    public static Refiner createRefiner(Context ctx) {
        Map<RefinementAlgorithm, Refiner> refiners = new HashMap<>();
        for (RefinementAlgorithm algorithm : ctx.refinement.algorithms) {
            refiners.computeIfAbsent(algorithm, alg -> createRefiner(ctx, alg));
        }
        return new MultiRefiner(refiners, ctx.refinement.algorithms);
    }

    private static Refiner createRefiner(Context ctx, RefinementAlgorithm algorithm) {
        switch (algorithm) {

            case LABEL_PROPAGATION:
                return new LabelPropagationRefiner(ctx);
            case GREEDY_BALANCER:
                return new GreedyBalancer(ctx);
            default:
                throw new IllegalArgumentException("Unsupported refinement algorithm: " + algorithm);
        }
    }

}
