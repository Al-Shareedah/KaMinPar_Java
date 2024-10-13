package org.alshar;

import org.alshar.common.context.*;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.BlockWeight;
import org.alshar.common.enums.*;

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

        // Set Graph Ordering
        ctx.rearrangeBy = GraphOrdering.DEGREE_BUCKETS;

        // Set Partitioning Context
        ctx.partitioning = new PartitioningContext();
        ctx.partitioning.mode = PartitioningMode.DEEP;
        ctx.partitioning.deepInitialPartitioningMode = InitialPartitioningMode.SEQUENTIAL;
        ctx.partitioning.deepInitialPartitioningLoad = 1.0;

        // Set Partition Context
        ctx.partition = new PartitionContext();
        ctx.partition.epsilon = 0.01;
        ctx.partition.k = new BlockID(Integer.MAX_VALUE); // kInvalidBlockID equivalent

        // Set block constraints
        ctx.partition.blockConstraints.add(new BlockWeight(300));
        ctx.partition.blockConstraints.add(new BlockWeight(250));
        ctx.partition.blockConstraints.add(new BlockWeight(400));
        ctx.partition.blockConstraints.add(new BlockWeight(183));

        // Set Coarsening Context
        ctx.coarsening = new CoarseningContext();
        ctx.coarsening.algorithm = ClusteringAlgorithm.LABEL_PROPAGATION;

        // Label Propagation Coarsening Context
        ctx.coarsening.lp = new LabelPropagationCoarseningContext();
        ctx.coarsening.lp.numIterations = 5;
        ctx.coarsening.lp.largeDegreeThreshold = 1000000;
        ctx.coarsening.lp.maxNumNeighbors = 200000;
        ctx.coarsening.lp.twoHopStrategy = TwoHopStrategy.MATCH_THREADWISE;
        ctx.coarsening.lp.twoHopThreshold = 0.5;
        ctx.coarsening.lp.isolatedNodesStrategy = IsolatedNodesClusteringStrategy.KEEP;

        // Additional Coarsening Settings
        ctx.coarsening.contractionLimit = 113;
        ctx.coarsening.enforceContractionLimit = false;
        ctx.coarsening.convergenceThreshold = 0.05;
        ctx.coarsening.clusterWeightLimit = ClusterWeightLimit.EPSILON_BLOCK_WEIGHT;
        ctx.coarsening.clusterWeightMultiplier = 1.0;

        // Set Initial Partitioning Context
        ctx.initialPartitioning = new InitialPartitioningContext();

        // Initial Coarsening Context within Initial Partitioning
        ctx.initialPartitioning.coarsening = new InitialCoarseningContext();
        ctx.initialPartitioning.coarsening.contractionLimit = 20;
        ctx.initialPartitioning.coarsening.convergenceThreshold = 0.05;
        ctx.initialPartitioning.coarsening.largeDegreeThreshold = 1000000;
        ctx.initialPartitioning.coarsening.clusterWeightLimit = ClusterWeightLimit.BLOCK_WEIGHT;
        ctx.initialPartitioning.coarsening.clusterWeightMultiplier = 1.0 / 12.0;

        // Initial Refinement Context within Initial Partitioning
        ctx.initialPartitioning.refinement = new InitialRefinementContext();
        ctx.initialPartitioning.refinement.disabled = false;
        ctx.initialPartitioning.refinement.stoppingRule = FMStoppingRule.SIMPLE;
        ctx.initialPartitioning.refinement.numFruitlessMoves = 100;
        ctx.initialPartitioning.refinement.alpha = 1.0;
        ctx.initialPartitioning.refinement.numIterations = 5;
        ctx.initialPartitioning.refinement.improvementAbortionThreshold = 0.0001;

        // Initial Partitioning Repetition Context
        ctx.initialPartitioning.repetitionMultiplier = 1.0;
        ctx.initialPartitioning.minNumRepetitions = 10;
        ctx.initialPartitioning.minNumNonAdaptiveRepetitions = 5;
        ctx.initialPartitioning.maxNumRepetitions = 50;
        ctx.initialPartitioning.numSeedIterations = 1;
        ctx.initialPartitioning.useAdaptiveBipartitionerSelection = true;

        // Set Refinement Context
        ctx.refinement = new RefinementContext();
        ctx.refinement.algorithms.add(RefinementAlgorithm.GREEDY_BALANCER);
        ctx.refinement.algorithms.add(RefinementAlgorithm.LABEL_PROPAGATION);

        // Label Propagation Refinement Context within Refinement
        ctx.refinement.lp = new LabelPropagationRefinementContext();
        ctx.refinement.lp.numIterations = 5;
        ctx.refinement.lp.largeDegreeThreshold = 1000000;
        ctx.refinement.lp.maxNumNeighbors = Integer.MAX_VALUE;

        // k-way FM Refinement Context within Refinement
        ctx.refinement.kwayFM = new KwayFMRefinementContext();
        ctx.refinement.kwayFM.numSeedNodes = 10;
        ctx.refinement.kwayFM.alpha = 1.0;
        ctx.refinement.kwayFM.numIterations = 10;
        ctx.refinement.kwayFM.unlockLocallyMovedNodes = true;
        ctx.refinement.kwayFM.unlockSeedNodes = true;
        ctx.refinement.kwayFM.useExactAbortionThreshold = false;
        ctx.refinement.kwayFM.abortionThreshold = 0.999;
        ctx.refinement.kwayFM.gainCacheStrategy = GainCacheStrategy.DENSE;
        ctx.refinement.kwayFM.constantHighDegreeThreshold = 0;
        ctx.refinement.kwayFM.kBasedHighDegreeThreshold = 1.0;
        ctx.refinement.kwayFM.dbgComputeBatchStats = false;

        // Jet Refinement Context within Refinement
        ctx.refinement.jet = new JetRefinementContext();
        ctx.refinement.jet.numIterations = 0;
        ctx.refinement.jet.numFruitlessIterations = 12;
        ctx.refinement.jet.fruitlessThreshold = 0.999;
        ctx.refinement.jet.fineNegativeGainFactor = 0.25;
        ctx.refinement.jet.coarseNegativeGainFactor = 0.75;
        ctx.refinement.jet.balancingAlgorithm = RefinementAlgorithm.GREEDY_BALANCER;

        // MtKaHyPar Refinement Context within Refinement
        ctx.refinement.mtkahypar = new MtKaHyParRefinementContext();
        ctx.refinement.mtkahypar.configFilename = "";
        ctx.refinement.mtkahypar.fineConfigFilename = "";
        ctx.refinement.mtkahypar.coarseConfigFilename = "";

        // Set Parallel Context
        ctx.parallel = new ParallelContext();
        ctx.parallel.numThreads = 1;

        // Set Debug Context
        ctx.debug = new DebugContext();
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
        ctx.partitioning.deepInitialPartitioningMode = InitialPartitioningMode.ASYNCHRONOUS_PARALLEL;
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
                RefinementAlgorithm.GREEDY_BALANCER,
                RefinementAlgorithm.LABEL_PROPAGATION,
                RefinementAlgorithm.KWAY_FM,
                RefinementAlgorithm.GREEDY_BALANCER
        );
        return ctx;
    }

    public static Context createJetContext() {
        Context ctx = createDefaultContext();
        ctx.refinement.algorithms = Arrays.asList(
                RefinementAlgorithm.GREEDY_BALANCER,
                RefinementAlgorithm.JET
        );
        return ctx;
    }

    public static Context createNorefContext() {
        Context ctx = createDefaultContext();
        ctx.refinement.algorithms.clear();
        return ctx;
    }
}

