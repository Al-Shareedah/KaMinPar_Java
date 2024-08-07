package org.alshar;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.List;
import java.util.stream.Collectors;

public class KaminparArguments {
    public static void createAllOptions(JCommander commander, Context ctx) {
        createPartitioningOptions(commander, ctx);
        createDebugOptions(commander, ctx);
        createCoarseningOptions(commander, ctx);
        createInitialPartitioningOptions(commander, ctx);
        createRefinementOptions(commander, ctx);
    }

    public static void createPartitioningOptions(JCommander commander, Context ctx) {
        commander.addCommand("Partitioning", new Object() {
            @Parameter(names = {"-e", "--epsilon"}, description = "Maximum allowed imbalance, e.g. 0.03 for 3%. Must be strictly positive.")
            double epsilon = ctx.partition.epsilon;

            @Parameter(names = {"-m", "--p-mode"}, description = "Partitioning scheme.")
            String mode = ctx.partitioning.mode.toString();

            @Parameter(names = {"--p-deep-initial-partitioning-mode"}, description = "Chooses the initial partitioning mode.")
            String deepInitialPartitioningMode = ctx.partitioning.deepInitialPartitioningMode.toString();

            @Parameter(names = {"--p-deep-initial-partitioning-load"}, description = "Fraction of cores used for the coarse graph replication phase.")
            double deepInitialPartitioningLoad = ctx.partitioning.deepInitialPartitioningLoad;

            @Parameter(names = {"--rearrange-by"}, description = "Criteria by which the graph is sorted and rearrange.")
            String rearrangeBy = ctx.rearrangeBy.toString();
        });
    }

    public static void createCoarseningOptions(JCommander commander, Context ctx) {
        commander.addCommand("Coarsening", new Object() {
            @Parameter(names = {"--c-contraction-limit"}, description = "Upper limit for the number of nodes per block in the coarsest graph.")
            int contractionLimit = ctx.coarsening.contractionLimit;

            @Parameter(names = {"--c-clustering-algorithm"}, description = "Clustering algorithm.")
            String clusteringAlgorithm = ctx.coarsening.algorithm.toString();

            @Parameter(names = {"--c-cluster-weight-limit"}, description = "Maximum cluster weight limit.")
            String clusterWeightLimit = ctx.coarsening.clusterWeightLimit.toString();

            @Parameter(names = {"--c-cluster-weight-multiplier"}, description = "Multiplicator of the maximum cluster weight base value.")
            double clusterWeightMultiplier = ctx.coarsening.clusterWeightMultiplier;

            @Parameter(names = {"--c-coarsening-convergence-threshold"}, description = "Coarsening convergence threshold.")
            double convergenceThreshold = ctx.coarsening.convergenceThreshold;

            @Parameter(names = {"--c-lp-num-iterations"}, description = "Maximum number of label propagation iterations.")
            int lpNumIterations = ctx.coarsening.lp.numIterations;

            @Parameter(names = {"--c-lp-active-large-degree-threshold"}, description = "Threshold for ignoring nodes with large degree.")
            int lpLargeDegreeThreshold = ctx.coarsening.lp.largeDegreeThreshold;

            @Parameter(names = {"--c-lp-max-num-neighbors"}, description = "Limit the neighborhood to this many nodes.")
            int lpMaxNumNeighbors = ctx.coarsening.lp.maxNumNeighbors;

            @Parameter(names = {"--c-lp-two-hop-strategy"}, description = "Two-hop strategy.")
            String twoHopStrategy = ctx.coarsening.lp.twoHopStrategy.toString();

            @Parameter(names = {"--c-lp-two-hop-threshold"}, description = "Two-hop threshold.")
            double twoHopThreshold = ctx.coarsening.lp.twoHopThreshold;

            @Parameter(names = {"--c-lp-isolated-nodes-strategy"}, description = "Strategy for handling isolated nodes.")
            String isolatedNodesStrategy = ctx.coarsening.lp.isolatedNodesStrategy.toString();
        });
    }

    public static void createInitialPartitioningOptions(JCommander commander, Context ctx) {
        commander.addCommand("InitialPartitioning", new Object() {
            @Parameter(names = {"--i-r-disable"}, description = "Disable initial refinement.")
            boolean refinementDisabled = ctx.initialPartitioning.refinement.disabled;
        });
    }

    public static void createRefinementOptions(JCommander commander, Context ctx) {
        commander.addCommand("Refinement", new Object() {
            @Parameter(names = {"--r-algorithms"}, description = "Sequence of refinement algorithms.")
            List<String> algorithms = ctx.refinement.algorithms.stream().map(Enum::toString).collect(Collectors.toList());

            @Parameter(names = {"--r-lp-num-iterations"}, description = "Number of label propagation iterations to perform.")
            int lpNumIterations = ctx.refinement.lp.numIterations;

            @Parameter(names = {"--r-lp-active-large-degree-threshold"}, description = "Ignore nodes that have a degree larger than this threshold.")
            int lpLargeDegreeThreshold = ctx.refinement.lp.largeDegreeThreshold;

            @Parameter(names = {"--r-lp-max-num-neighbors"}, description = "Maximum number of neighbors to consider for each node.")
            int lpMaxNumNeighbors = ctx.refinement.lp.maxNumNeighbors;

            @Parameter(names = {"--r-fm-num-iterations"}, description = "Number of FM iterations to perform.")
            int fmNumIterations = ctx.refinement.kwayFM.numIterations;

            @Parameter(names = {"--r-fm-num-seed-nodes"}, description = "Number of seed nodes used to initialize a single localized search.")
            int fmNumSeedNodes = ctx.refinement.kwayFM.numSeedNodes;

            @Parameter(names = {"--r-fm-abortion-threshold"}, description = "Stop FM iterations if the edge cut reduction falls below this threshold.")
            double fmAbortionThreshold = ctx.refinement.kwayFM.abortionThreshold;

            @Parameter(names = {"--r-fm-lock-locally-moved-nodes"}, description = "Unlock all nodes after a batch that were only moved thread-locally.")
            boolean unlockLocallyMovedNodes = ctx.refinement.kwayFM.unlockLocallyMovedNodes;

            @Parameter(names = {"--r-fm-lock-seed-nodes"}, description = "Keep seed nodes locked even if they were never moved.")
            boolean unlockSeedNodes = ctx.refinement.kwayFM.unlockSeedNodes;

            @Parameter(names = {"--r-fm-gc"}, description = "Gain cache strategy.")
            String gainCacheStrategy = ctx.refinement.kwayFM.gainCacheStrategy.toString();

            @Parameter(names = {"--r-fm-gc-const-hd-threshold"}, description = "Constant threshold for high-degree nodes.")
            int fmGcConstHdThreshold = ctx.refinement.kwayFM.constantHighDegreeThreshold;

            @Parameter(names = {"--r-fm-gc-k-based-hd-threshold"}, description = "Multiplier for k-based high-degree nodes.")
            double fmGcKBasedHdThreshold = ctx.refinement.kwayFM.kBasedHighDegreeThreshold;

            @Parameter(names = {"--r-fm-dbg-batch-stats"}, description = "Compute and output detailed statistics about FM batches.")
            boolean dbgComputeBatchStats = ctx.refinement.kwayFM.dbgComputeBatchStats;

            @Parameter(names = {"--r-jet-num-iterations"}, description = "Number of Jet iterations.")
            int jetNumIterations = ctx.refinement.jet.numIterations;

            @Parameter(names = {"--r-jet-num-fruitless-iterations"}, description = "Number of fruitless iterations.")
            int jetNumFruitlessIterations = ctx.refinement.jet.numFruitlessIterations;

            @Parameter(names = {"--r-jet-fruitless-threshold"}, description = "Fruitless threshold.")
            double jetFruitlessThreshold = ctx.refinement.jet.fruitlessThreshold;

            @Parameter(names = {"--r-jet-coarse-negative-gain-factor"}, description = "Coarse negative gain factor.")
            double jetCoarseNegativeGainFactor = ctx.refinement.jet.coarseNegativeGainFactor;

            @Parameter(names = {"--r-jet-fine-negative-gain-factor"}, description = "Fine negative gain factor.")
            double jetFineNegativeGainFactor = ctx.refinement.jet.fineNegativeGainFactor;

            @Parameter(names = {"--r-mtkahypar-config"}, description = "Mt-KaHyPar config filename.")
            String mtkahyparConfigFilename = ctx.refinement.mtkahypar.configFilename;

            @Parameter(names = {"--r-mtkahypar-config-fine"}, description = "Mt-KaHyPar fine config filename.")
            String mtkahyparFineConfigFilename = ctx.refinement.mtkahypar.fineConfigFilename;

            @Parameter(names = {"--r-mtkahypar-config-coarse"}, description = "Mt-KaHyPar coarse config filename.")
            String mtkahyparCoarseConfigFilename = ctx.refinement.mtkahypar.coarseConfigFilename;
        });
    }

    public static void createDebugOptions(JCommander commander, Context ctx) {
        commander.addCommand("Debug", new Object() {
            @Parameter(names = {"--d-dump-graph-filename"}, description = "Dump graph filename.")
            String dumpGraphFilename = ctx.debug.dumpGraphFilename;

            @Parameter(names = {"--d-dump-partition-filename"}, description = "Dump partition filename.")
            String dumpPartitionFilename = ctx.debug.dumpPartitionFilename;

            @Parameter(names = {"--d-dump-toplevel-graph"}, description = "Write the toplevel graph to disk.")
            boolean dumpToplevelGraph = ctx.debug.dumpToplevelGraph;

            @Parameter(names = {"--d-dump-toplevel-partition"}, description = "Write the partition of the toplevel graph to disk.")
            boolean dumpToplevelPartition = ctx.debug.dumpToplevelPartition;

            @Parameter(names = {"--d-dump-coarsest-graph"}, description = "Write the coarsest graph to disk.")
            boolean dumpCoarsestGraph = ctx.debug.dumpCoarsestGraph;

            @Parameter(names = {"--d-dump-coarsest-partition"}, description = "Write partition of the coarsest graph to disk.")
            boolean dumpCoarsestPartition = ctx.debug.dumpCoarsestPartition;

            @Parameter(names = {"--d-dump-graph-hierarchy"}, description = "Write the entire graph hierarchy to disk.")
            boolean dumpGraphHierarchy = ctx.debug.dumpGraphHierarchy;

            @Parameter(names = {"--d-dump-partition-hierarchy"}, description = "Write the entire partition hierarchy to disk.")
            boolean dumpPartitionHierarchy = ctx.debug.dumpPartitionHierarchy;

            @Parameter(names = {"--d-dump-everything"}, description = "Activate all --d-dump-* options.")
            boolean dumpEverything;

            public void setDumpEverything(boolean dumpEverything) {
                this.dumpToplevelGraph = true;
                this.dumpToplevelPartition = true;
                this.dumpCoarsestGraph = true;
                this.dumpCoarsestPartition = true;
                this.dumpGraphHierarchy = true;
                this.dumpPartitionHierarchy = true;
            }
        });
    }
}

