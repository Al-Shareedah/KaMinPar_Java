package org.alshar;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Presets {
    public static Context createContextByPresetName(String name) {
        switch (name) {
            case "default":
                return createDefaultContext();
            case "fast":
                return createFastContext();
            case "largek":
                return createLargekContext();
            case "strong":
            case "fm":
                return createStrongContext();
            case "jet":
                return createJetContext();
            case "noref":
                return createNorefContext();
            default:
                throw new IllegalArgumentException("Invalid preset name");
        }
    }

    public static Set<String> getPresetNames() {
        Set<String> presetNames = new HashSet<>();
        presetNames.add("default");
        presetNames.add("fast");
        presetNames.add("largek");
        presetNames.add("strong");
        presetNames.add("fm");
        presetNames.add("jet");
        presetNames.add("noref");
        return presetNames;
    }

    public static Context createDefaultContext() {
        Context ctx = new Context();
        ctx.rearrangeBy = Context.GraphOrdering.DEGREE_BUCKETS;
        ctx.partitioning = new Context.PartitioningContext();
        ctx.partitioning.mode = Context.PartitioningMode.DEEP;
        ctx.partitioning.deepInitialPartitioningMode = Context.InitialPartitioningMode.SEQUENTIAL;
        ctx.partitioning.deepInitialPartitioningLoad = 1.0;

        ctx.partition = new Context.PartitionContext();
        ctx.partition.epsilon = 0.03;
        ctx.partition.k = Integer.MAX_VALUE; // kInvalidBlockID equivalent

        ctx.coarsening = new Context.CoarseningContext();
        ctx.coarsening.algorithm = Context.ClusteringAlgorithm.LABEL_PROPAGATION;
        ctx.coarsening.lp = new Context.LabelPropagationCoarseningContext();
        ctx.coarsening.lp.numIterations = 5;
        ctx.coarsening.lp.largeDegreeThreshold = 1000000;
        ctx.coarsening.lp.maxNumNeighbors = 200000;
        ctx.coarsening.lp.twoHopStrategy = Context.TwoHopStrategy.MATCH_THREADWISE;
        ctx.coarsening.lp.twoHopThreshold = 0.5;
        ctx.coarsening.lp.isolatedNodesStrategy = Context.IsolatedNodesClusteringStrategy.KEEP;
        ctx.coarsening.contractionLimit = 2000;
        ctx.coarsening.enforceContractionLimit = false;
        ctx.coarsening.convergenceThreshold = 0.05;
        ctx.coarsening.clusterWeightLimit = Context.ClusterWeightLimit.EPSILON_BLOCK_WEIGHT;
        ctx.coarsening.clusterWeightMultiplier = 1.0;

        ctx.initialPartitioning = new Context.InitialPartitioningContext();
        ctx.initialPartitioning.coarsening = new Context.InitialCoarseningContext();
        ctx.initialPartitioning.coarsening.contractionLimit = 20;
        ctx.initialPartitioning.coarsening.convergenceThreshold = 0.05;
        ctx.initialPartitioning.coarsening.largeDegreeThreshold = 1000000;
        ctx.initialPartitioning.coarsening.clusterWeightLimit = Context.ClusterWeightLimit.BLOCK_WEIGHT;
        ctx.initialPartitioning.coarsening.clusterWeightMultiplier = 1.0 / 12.0;

        ctx.initialPartitioning.refinement = new Context.InitialRefinementContext();
        ctx.initialPartitioning.refinement.disabled = false;
        ctx.initialPartitioning.refinement.stoppingRule = Context.FMStoppingRule.SIMPLE;
        ctx.initialPartitioning.refinement.numFruitlessMoves = 100;
        ctx.initialPartitioning.refinement.alpha = 1.0;
        ctx.initialPartitioning.refinement.numIterations = 5;
        ctx.initialPartitioning.refinement.improvementAbortionThreshold = 0.0001;

        ctx.initialPartitioning.repetitionMultiplier = 1.0;
        ctx.initialPartitioning.minNumRepetitions = 10;
        ctx.initialPartitioning.minNumNonAdaptiveRepetitions = 5;
        ctx.initialPartitioning.maxNumRepetitions = 50;
        ctx.initialPartitioning.numSeedIterations = 1;
        ctx.initialPartitioning.useAdaptiveBipartitionerSelection = true;

        ctx.refinement = new Context.RefinementContext();
        ctx.refinement.algorithms.add(Context.RefinementAlgorithm.GREEDY_BALANCER);
        ctx.refinement.algorithms.add(Context.RefinementAlgorithm.LABEL_PROPAGATION);
        ctx.refinement.lp = new Context.LabelPropagationRefinementContext();
        ctx.refinement.lp.numIterations = 5;
        ctx.refinement.lp.largeDegreeThreshold = 1000000;
        ctx.refinement.lp.maxNumNeighbors = Integer.MAX_VALUE;

        ctx.refinement.kwayFM = new Context.KwayFMRefinementContext();
        ctx.refinement.kwayFM.numSeedNodes = 10;
        ctx.refinement.kwayFM.alpha = 1.0;
        ctx.refinement.kwayFM.numIterations = 10;
        ctx.refinement.kwayFM.unlockLocallyMovedNodes = true;
        ctx.refinement.kwayFM.unlockSeedNodes = true;
        ctx.refinement.kwayFM.useExactAbortionThreshold = false;
        ctx.refinement.kwayFM.abortionThreshold = 0.999;
        ctx.refinement.kwayFM.gainCacheStrategy = Context.GainCacheStrategy.DENSE;
        ctx.refinement.kwayFM.constantHighDegreeThreshold = 0;
        ctx.refinement.kwayFM.kBasedHighDegreeThreshold = 1.0;
        ctx.refinement.kwayFM.dbgComputeBatchStats = false;

        ctx.refinement.jet = new Context.JetRefinementContext();
        ctx.refinement.jet.numIterations = 0;
        ctx.refinement.jet.numFruitlessIterations = 12;
        ctx.refinement.jet.fruitlessThreshold = 0.999;
        ctx.refinement.jet.fineNegativeGainFactor = 0.25;
        ctx.refinement.jet.coarseNegativeGainFactor = 0.75;
        ctx.refinement.jet.balancingAlgorithm = Context.RefinementAlgorithm.GREEDY_BALANCER;

        ctx.parallel = new Context.ParallelContext();
        ctx.parallel.numThreads = 1;

        ctx.debug = new Context.DebugContext();
        ctx.debug.graphName = "";
        ctx.debug.dumpGraphFilename = "n%n_m%m_k%k_seed%seed.metis";
        ctx.debug.dumpPartitionFilename = "n%n_m%m_k%k_seed%seed.part";
        ctx.debug.dumpToplevelGraph = false;
        ctx.debug.dumpToplevelPartition = false;
        ctx.debug.dumpCoarsestGraph = false;
        ctx.debug.dumpCoarsestPartition = false;
        ctx.debug.dumpGraphHierarchy = false;
        ctx.debug.dumpPartitionHierarchy = false;

        return ctx;
    }

    public static Context createFastContext() {
        Context ctx = createDefaultContext();
        ctx.partitioning.deepInitialPartitioningMode = Context.InitialPartitioningMode.ASYNCHRONOUS_PARALLEL;
        ctx.partitioning.deepInitialPartitioningLoad = 0.5;
        ctx.coarsening.lp.numIterations = 1;
        ctx.initialPartitioning.minNumRepetitions = 1;
        ctx.initialPartitioning.minNumNonAdaptiveRepetitions = 1;
        ctx.initialPartitioning.maxNumRepetitions = 1;
        return ctx;
    }

    public static Context createLargekContext() {
        Context ctx = createDefaultContext();
        ctx.initialPartitioning.minNumRepetitions = 4;
        ctx.initialPartitioning.minNumNonAdaptiveRepetitions = 2;
        ctx.initialPartitioning.maxNumRepetitions = 4;
        return ctx;
    }

    public static Context createStrongContext() {
        Context ctx = createDefaultContext();
        ctx.refinement.algorithms = Arrays.asList(
                Context.RefinementAlgorithm.GREEDY_BALANCER,
                Context.RefinementAlgorithm.LABEL_PROPAGATION,
                Context.RefinementAlgorithm.KWAY_FM,
                Context.RefinementAlgorithm.GREEDY_BALANCER
        );
        return ctx;
    }

    public static Context createJetContext() {
        Context ctx = createDefaultContext();
        ctx.refinement.algorithms = Arrays.asList(
                Context.RefinementAlgorithm.GREEDY_BALANCER,
                Context.RefinementAlgorithm.JET
        );
        return ctx;
    }

    public static Context createNorefContext() {
        Context ctx = createDefaultContext();
        ctx.refinement.algorithms.clear();
        return ctx;
    }
}

