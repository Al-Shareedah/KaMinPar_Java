package org.alshar.common;
import org.alshar.Context;

import java.io.PrintStream;

import org.alshar.common.Math.Random_shm;
import org.alshar.common.enums.*;
import org.alshar.common.context.*;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.alshar.common.LoggerMacros.LOG;

public class cio {
    public static void printDelimiter(String caption, char ch) {
        if (!caption.isEmpty()) {
            LOG.log(ch + " " + caption + " " + ch);
        } else {
            LOG.log(String.valueOf(ch).repeat(80));
        }
        LOG.flush();
    }

    public static void printDelimiter(String caption) {
        printDelimiter(caption, '#');
    }

    public static void printKaminparBanner() {
        LOG.log("KaMinPar - KaHIP Graph Partitioning");
    }

    public static void printDkaminparBanner() {
        LOG.log("DKaMinPar - Distributed KaHIP Graph Partitioning");
    }

    public static void printBuildIdentifier() {
        LOG.log("Build Identifier: " + Environment.GIT_SHA1);
    }

    public static void printBuildDatatypes(Class<?> nodeIDClass, Class<?> edgeIDClass, Class<?> nodeWeightClass, Class<?> edgeWeightClass) {
        LOG.log("Data type sizes:");
        LOG.log("  Nodes IDs: " + Integer.BYTES + " bytes | Node weights: " + Integer.BYTES + " bytes");
        LOG.log("  Edges IDs: " + Integer.BYTES + " bytes | Edge weights: " + Integer.BYTES + " bytes");
    }

    public static <NodeID, EdgeID, LocalNodeWeight, LocalEdgeWeight, IPNodeWeight, IPEdgeWeight> void printBuildDatatypesExtended() {
        LOG.log("Data type sizes:");
        LOG.log("  Nodes IDs: " + Integer.BYTES
                + " bytes | Node weights (Local): " + Integer.BYTES
                + " bytes | Node weights (IP): " + Integer.BYTES + " bytes");
        LOG.log("  Edges IDs: " + Integer.BYTES
                + " bytes | Edge weights (Local): " + Integer.BYTES
                + " bytes | Edge weights (IP): " + Integer.BYTES + " bytes");
    }

    // Additional print methods for various contexts...

    public static void print(CoarseningContext c_ctx, PrintStream out) {
        out.println("Contraction limit:            " + c_ctx.contractionLimit);
        out.println("Cluster weight limit:         " + c_ctx.clusterWeightLimit + " x " + c_ctx.clusterWeightMultiplier);
        out.println("Clustering algorithm:         " + c_ctx.algorithm);
        if (c_ctx.algorithm == ClusteringAlgorithm.LABEL_PROPAGATION) {
            print(c_ctx.lp, out);
        }
    }

    public static void print(LabelPropagationCoarseningContext lp_ctx, PrintStream out) {
        out.println("  Number of iterations:       " + lp_ctx.numIterations);
        out.println("  High degree threshold:      " + lp_ctx.largeDegreeThreshold);
        out.println("  Max degree:                 " + lp_ctx.maxNumNeighbors);
        out.println("  2-hop clustering:           " + lp_ctx.twoHopStrategy + ", if |Vcoarse| > "
                + String.format("%.2f", lp_ctx.twoHopThreshold) + " * |V|");
        out.println("  Isolated nodes:             " + lp_ctx.isolatedNodesStrategy);
    }

    public static void print(InitialPartitioningContext i_ctx, PrintStream out) {
        out.println("Adaptive algorithm selection: " + (i_ctx.useAdaptiveBipartitionerSelection ? "yes" : "no"));
    }

    public static void print(RefinementContext r_ctx, PrintStream out) {
        List<String> algorithms = r_ctx.algorithms.stream()
                .map(Enum::name)
                .collect(Collectors.toList());
        out.println("Refinement algorithms:        [" + String.join(" -> ", algorithms) + "]");
        if (r_ctx.algorithms.contains(RefinementAlgorithm.LABEL_PROPAGATION)) {
            out.println("Label propagation:");
            out.println("  Number of iterations:       " + r_ctx.lp.numIterations);
        }
        if (r_ctx.algorithms.contains(RefinementAlgorithm.KWAY_FM)) {
            out.println("k-way FM:");
            out.println("  Number of iterations:       " + r_ctx.kwayFM.numIterations
                    + " [or improvement drops below < " + 100.0 * (1.0 - r_ctx.kwayFM.abortionThreshold) + "%]");
            out.println("  Number of seed nodes:       " + r_ctx.kwayFM.numSeedNodes);
            out.println("  Locking strategies:         seed nodes: "
                    + (r_ctx.kwayFM.unlockSeedNodes ? "unlock" : "lock") + ", locally moved nodes:"
                    + (r_ctx.kwayFM.unlockLocallyMovedNodes ? "unlock" : "lock"));
            out.println("  Gain cache:                 " + r_ctx.kwayFM.gainCacheStrategy);
            if (r_ctx.kwayFM.gainCacheStrategy == GainCacheStrategy.HYBRID) {
                out.println("  High-degree threshold:");
                out.println("    based on k:               " + r_ctx.kwayFM.kBasedHighDegreeThreshold);
                out.println("    constant:                 " + r_ctx.kwayFM.constantHighDegreeThreshold);
            }
        }
        if (r_ctx.algorithms.contains(RefinementAlgorithm.JET)) {
            out.println("Jet refinement:               " + RefinementAlgorithm.JET);
            out.println("  Number of iterations:       max " + r_ctx.jet.numIterations + ", or "
                    + r_ctx.jet.numFruitlessIterations + " fruitless (improvement < "
                    + 100.0 * (1 - r_ctx.jet.fruitlessThreshold) + "%)");
            out.println("  Penalty factors:            coarse " + r_ctx.jet.coarseNegativeGainFactor
                    + ", fine " + r_ctx.jet.fineNegativeGainFactor);
            out.println("  Balancing algorithm:        " + r_ctx.jet.balancingAlgorithm);
        }
    }

    public static void print(PartitionContext p_ctx, PrintStream out) {
        final long maxBlockWeight = p_ctx.blockWeights.max(0).value;
        final long size = Math.max(Math.max(p_ctx.n.value, p_ctx.m.value), maxBlockWeight);
        final int width = (int) Math.ceil(Math.log10(size));

        out.printf("  Number of nodes:            %" + width + "d", p_ctx.n.value);
        if (p_ctx.n.value == p_ctx.totalNodeWeight.value) {
            out.println(" (unweighted)");
        } else {
            out.println(" (total weight: " + p_ctx.totalNodeWeight + ")");
        }
        out.printf("  Number of edges:            %" + width + "d", p_ctx.m.value);
        if (p_ctx.m.value == p_ctx.totalEdgeWeight.value) {
            out.println(" (unweighted)");
        } else {
            out.println(" (total weight: " + p_ctx.totalEdgeWeight + ")");
        }
        out.println("Number of blocks:             " + p_ctx.k.value);
        out.println("Maximum block weight:         " + p_ctx.blockWeights.max(0).value + " ("
                + p_ctx.blockWeights.perfectlyBalanced(0).value + " + " + 100 * p_ctx.epsilon + "%)");
    }

    public static void print(PartitioningContext p_ctx, PrintStream out) {
        out.println("Partitioning mode:            " + p_ctx.mode);
        if (p_ctx.mode == PartitioningMode.DEEP) {
            out.println("  Deep initial part. mode:    " + p_ctx.deepInitialPartitioningMode);
            out.println("  Deep initial part. load:    " + p_ctx.deepInitialPartitioningLoad);
        }
    }

    public static void print(Context ctx, PrintStream out) {
        out.println("Execution mode:               " + ctx.parallel.numThreads);
        out.println("Seed:                         " + Random_shm.getSeed());
        out.println("Graph:                        " + ctx.debug.graphName
                + " [ordering: " + ctx.rearrangeBy + "]");
        print(ctx.partition, out);
        printDelimiter("Partitioning Scheme", '-');
        print(ctx.partitioning, out);
        printDelimiter("Coarsening", '-');
        print(ctx.coarsening, out);
        printDelimiter("Initial Partitioning", '-');
        print(ctx.initialPartitioning, out);
        printDelimiter("Refinement", '-');
        print(ctx.refinement, out);
    }

    // Conversion methods from string to enum types
    public static Map<String, GraphOrdering> getGraphOrderings() {
        Map<String, GraphOrdering> map = new HashMap<>();
        map.put("natural", GraphOrdering.NATURAL);
        map.put("deg-buckets", GraphOrdering.DEGREE_BUCKETS);
        map.put("degree-buckets", GraphOrdering.DEGREE_BUCKETS);
        return map;
    }

    public static Map<String, ClusteringAlgorithm> getClusteringAlgorithms() {
        Map<String, ClusteringAlgorithm> map = new HashMap<>();
        map.put("noop", ClusteringAlgorithm.NOOP);
        map.put("lp", ClusteringAlgorithm.LABEL_PROPAGATION);
        return map;
    }

    public static Map<String, ClusterWeightLimit> getClusterWeightLimits() {
        Map<String, ClusterWeightLimit> map = new HashMap<>();
        map.put("epsilon-block-weight", ClusterWeightLimit.EPSILON_BLOCK_WEIGHT);
        map.put("static-block-weight", ClusterWeightLimit.BLOCK_WEIGHT);
        map.put("one", ClusterWeightLimit.ONE);
        map.put("zero", ClusterWeightLimit.ZERO);
        return map;
    }

    public static Map<String, RefinementAlgorithm> getKwayRefinementAlgorithms() {
        Map<String, RefinementAlgorithm> map = new HashMap<>();
        map.put("noop", RefinementAlgorithm.NOOP);
        map.put("lp", RefinementAlgorithm.LABEL_PROPAGATION);
        map.put("fm", RefinementAlgorithm.KWAY_FM);
        map.put("jet", RefinementAlgorithm.JET);
        map.put("greedy-balancer", RefinementAlgorithm.GREEDY_BALANCER);
        map.put("mtkahypar", RefinementAlgorithm.MTKAHYPAR);
        return map;
    }

    public static Map<String, FMStoppingRule> getFmStoppingRules() {
        Map<String, FMStoppingRule> map = new HashMap<>();
        map.put("simple", FMStoppingRule.SIMPLE);
        map.put("adaptive", FMStoppingRule.ADAPTIVE);
        return map;
    }

    public static Map<String, PartitioningMode> getPartitioningModes() {
        Map<String, PartitioningMode> map = new HashMap<>();
        map.put("deep", PartitioningMode.DEEP);
        map.put("rb", PartitioningMode.RB);
        map.put("kway", PartitioningMode.KWAY);
        return map;
    }

    public static Map<String, InitialPartitioningMode> getInitialPartitioningModes() {
        Map<String, InitialPartitioningMode> map = new HashMap<>();
        map.put("sequential", InitialPartitioningMode.SEQUENTIAL);
        map.put("async-parallel", InitialPartitioningMode.ASYNCHRONOUS_PARALLEL);
        map.put("sync-parallel", InitialPartitioningMode.SYNCHRONOUS_PARALLEL);
        return map;
    }

    public static Map<String, GainCacheStrategy> getGainCacheStrategies() {
        Map<String, GainCacheStrategy> map = new HashMap<>();
        map.put("sparse", GainCacheStrategy.SPARSE);
        map.put("dense", GainCacheStrategy.DENSE);
        map.put("on-the-fly", GainCacheStrategy.ON_THE_FLY);
        map.put("hybrid", GainCacheStrategy.HYBRID);
        map.put("tracing", GainCacheStrategy.TRACING);
        return map;
    }

    public static Map<String, TwoHopStrategy> getTwoHopStrategies() {
        Map<String, TwoHopStrategy> map = new HashMap<>();
        map.put("disable", TwoHopStrategy.DISABLE);
        map.put("match", TwoHopStrategy.MATCH);
        map.put("match-threadwise", TwoHopStrategy.MATCH_THREADWISE);
        map.put("cluster", TwoHopStrategy.CLUSTER);
        map.put("cluster-threadwise", TwoHopStrategy.CLUSTER_THREADWISE);
        map.put("legacy", TwoHopStrategy.LEGACY);
        return map;
    }

    public static Map<String, IsolatedNodesClusteringStrategy> getIsolatedNodesClusteringStrategies() {
        Map<String, IsolatedNodesClusteringStrategy> map = new HashMap<>();
        map.put("keep", IsolatedNodesClusteringStrategy.KEEP);
        map.put("match", IsolatedNodesClusteringStrategy.MATCH);
        map.put("cluster", IsolatedNodesClusteringStrategy.CLUSTER);
        map.put("match-during-two-hop", IsolatedNodesClusteringStrategy.MATCH_DURING_TWO_HOP);
        map.put("cluster-during-two-hop", IsolatedNodesClusteringStrategy.CLUSTER_DURING_TWO_HOP);
        return map;
    }
}