package org.alshar.kaminpar_shm;
import org.alshar.common.GraphUtils.Edge;
import org.alshar.common.context.*;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
public class Metrics {

    public static EdgeWeight edgeCut(PartitionedGraph pGraph) {
        AtomicLong cut = new AtomicLong(0);

        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                ForkJoinPool.commonPool().submit(() -> {
                    IntStream.range(0, pGraph.n().value).parallel().forEach(u -> {
                        pGraph.neighbors(new NodeID(u)).forEach(edge -> {
                            if (pGraph.block(new NodeID(u)).value != pGraph.block(edge.v).value) {
                                cut.addAndGet(pGraph.edgeWeight(edge.e).value);
                            }
                        });
                    });
                }).join();
            }
        });

        long totalCut = cut.get();
        assert totalCut % 2 == 0 : "Edge cut should be even.";
        return new EdgeWeight(totalCut / 2);
    }

    public static EdgeWeight edgeCutSeq(PartitionedGraph pGraph) {
        long cut = 0;

        for (int u = 0; u < pGraph.n().value; u++) {
            for (Edge edge : pGraph.neighbors(new NodeID(u))) {
                if (pGraph.block(new NodeID(u)).value != pGraph.block(edge.v).value) {
                    cut += pGraph.edgeWeight(edge.e).value;
                }
            }
        }

        assert cut % 2 == 0 : "Edge cut should be even.";
        return new EdgeWeight(cut / 2);
    }

    public static double imbalance(PartitionedGraph pGraph) {
        double perfectBlockWeight = Math.ceil(1.0 * pGraph.totalNodeWeight().value / pGraph.k().value);

        double maxImbalance = 0.0;
        for (int b = 0; b < pGraph.k().value; b++) {
            maxImbalance = Math.max(maxImbalance, pGraph.blockWeight(new BlockID(b)).value / perfectBlockWeight - 1.0);
        }

        return maxImbalance;
    }

    public static NodeWeight totalOverload(PartitionedGraph pGraph, PartitionContext pCtx) {
        long totalOverload = 0;
        for (int b = 0; b < pGraph.k().value; b++) {
            totalOverload += Math.max(0, pGraph.blockWeight(new BlockID(b)).value - pCtx.blockWeights.max(b).value);
        }

        return new NodeWeight(totalOverload);
    }

    public static boolean isBalanced(PartitionedGraph pGraph, PartitionContext pCtx) {
        for (int b = 0; b < pGraph.k().value; b++) {
            if (pGraph.blockWeight(new BlockID(b)).value > pCtx.blockWeights.max(b).value) {
                return false;
            }
        }
        return true;
    }

    public static boolean isFeasible(PartitionedGraph pGraph, BlockID inputK, double eps) {
        double maxBlockWeight = Math.ceil((1.0 + eps) * pGraph.totalNodeWeight().value / inputK.value);

        for (int b = 0; b < pGraph.k().value; b++) {
            int finalKb = PartitionUtils.computeFinalK(b, pGraph.k().value, inputK.value);
            if (pGraph.blockWeight(new BlockID(b)).value > maxBlockWeight * finalKb + pGraph.maxNodeWeight().value) {
                return false;
            }
        }
        return true;
    }

    public static boolean isFeasible(PartitionedGraph pGraph, PartitionContext pCtx) {
        return isBalanced(pGraph, pCtx);
    }
}