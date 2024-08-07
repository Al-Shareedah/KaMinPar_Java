package org.alshar;

import org.alshar.kaminpar_shm.kaminpar;
import org.alshar.kaminpar_shm.kaminpar.BlockID;
import org.alshar.kaminpar_shm.kaminpar.BlockWeight;
import org.alshar.kaminpar_shm.kaminpar.NodeID;
import org.alshar.kaminpar_shm.kaminpar.NodeWeight;
import org.alshar.kaminpar_shm.kaminpar.EdgeID;
import org.alshar.kaminpar_shm.kaminpar.EdgeWeight;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static org.alshar.kaminpar_shm.PartitionUtils.computeFinalK;

public class Context {
    public enum GraphOrdering {
        NATURAL, DEGREE_BUCKETS
    }

    public enum ClusteringAlgorithm {
        NOOP, LABEL_PROPAGATION
    }

    public enum ClusterWeightLimit {
        EPSILON_BLOCK_WEIGHT, BLOCK_WEIGHT, ONE, ZERO
    }

    public enum TwoHopStrategy {
        DISABLE, MATCH, MATCH_THREADWISE, CLUSTER, CLUSTER_THREADWISE, LEGACY
    }

    public enum IsolatedNodesClusteringStrategy {
        KEEP, MATCH, CLUSTER, MATCH_DURING_TWO_HOP, CLUSTER_DURING_TWO_HOP
    }

    public enum RefinementAlgorithm {
        LABEL_PROPAGATION, KWAY_FM, GREEDY_BALANCER, JET, MTKAHYPAR, NOOP
    }

    public enum FMStoppingRule {
        SIMPLE, ADAPTIVE
    }

    public enum GainCacheStrategy {
        SPARSE, DENSE, ON_THE_FLY, HYBRID, TRACING
    }

    public enum InitialPartitioningMode {
        SEQUENTIAL, ASYNCHRONOUS_PARALLEL, SYNCHRONOUS_PARALLEL
    }

    public enum PartitioningMode {
        DEEP, RB, KWAY
    }

    public static class LabelPropagationCoarseningContext {
        int numIterations;
        int largeDegreeThreshold;
        int maxNumNeighbors;
        TwoHopStrategy twoHopStrategy;
        double twoHopThreshold;
        IsolatedNodesClusteringStrategy isolatedNodesStrategy;
    }

    public static class CoarseningContext {
        ClusteringAlgorithm algorithm;
        LabelPropagationCoarseningContext lp;
        int contractionLimit;
        boolean enforceContractionLimit;
        double convergenceThreshold;
        ClusterWeightLimit clusterWeightLimit;
        double clusterWeightMultiplier;
    }

    public static class LabelPropagationRefinementContext {
        int numIterations;
        int largeDegreeThreshold;
        int maxNumNeighbors;
    }

    public static class KwayFMRefinementContext {
        int numSeedNodes;
        double alpha;
        int numIterations;
        boolean unlockLocallyMovedNodes;
        boolean unlockSeedNodes;
        boolean useExactAbortionThreshold;
        double abortionThreshold;
        GainCacheStrategy gainCacheStrategy;
        int constantHighDegreeThreshold;
        double kBasedHighDegreeThreshold;
        boolean dbgComputeBatchStats;
    }

    public static class JetRefinementContext {
        int numIterations;
        int numFruitlessIterations;
        double fruitlessThreshold;
        double fineNegativeGainFactor;
        double coarseNegativeGainFactor;
        RefinementAlgorithm balancingAlgorithm;
    }

    public static class MtKaHyParRefinementContext {
        String configFilename;
        String coarseConfigFilename;
        String fineConfigFilename;
    }

    public static class InitialCoarseningContext {
        int contractionLimit;
        double convergenceThreshold;
        int largeDegreeThreshold;
        ClusterWeightLimit clusterWeightLimit;
        double clusterWeightMultiplier;
    }

    public static class InitialRefinementContext {
        boolean disabled;
        FMStoppingRule stoppingRule;
        int numFruitlessMoves;
        double alpha;
        int numIterations;
        double improvementAbortionThreshold;
    }

    public static class InitialPartitioningContext {
        InitialCoarseningContext coarsening;
        InitialRefinementContext refinement;
        double repetitionMultiplier;
        int minNumRepetitions;
        int minNumNonAdaptiveRepetitions;
        int maxNumRepetitions;
        int numSeedIterations;
        boolean useAdaptiveBipartitionerSelection;
    }

    public static class RefinementContext {
        List<RefinementAlgorithm> algorithms = new ArrayList<>();
        LabelPropagationRefinementContext lp;
        KwayFMRefinementContext kwayFM;
        JetRefinementContext jet;
        MtKaHyParRefinementContext mtkahypar;
    }

    public static class PartitioningContext {
        PartitioningMode mode;
        InitialPartitioningMode deepInitialPartitioningMode;
        double deepInitialPartitioningLoad;
    }

    public static class PartitionContext {
        double epsilon;
        int k;
        NodeID n = kaminpar.kInvalidNodeID;
        EdgeID m = kaminpar.kInvalidEdgeID;
        NodeWeight totalNodeWeight = kaminpar.kInvalidNodeWeight;
        EdgeWeight totalEdgeWeight = kaminpar.kInvalidEdgeWeight;
        NodeWeight maxNodeWeight = kaminpar.kInvalidNodeWeight;

        void setupBlockWeights() {
            blockWeights.setup(this);
        }

        public void setup(Graph graph) {
            n = new NodeID(graph.n().value);
            m = new EdgeID(graph.m().value);
            totalNodeWeight = new NodeWeight(graph.totalNodeWeight().value);
            totalEdgeWeight = new EdgeWeight(graph.totalEdgeWeight().value);
            maxNodeWeight = new NodeWeight(graph.maxNodeWeight().value);
            setupBlockWeights();
        }
    }

    public static class ParallelContext {
        int numThreads;
    }

    public static class DebugContext {
        String graphName;
        String dumpGraphFilename;
        String dumpPartitionFilename;
        boolean dumpToplevelGraph;
        boolean dumpToplevelPartition;
        boolean dumpCoarsestGraph;
        boolean dumpCoarsestPartition;
        boolean dumpGraphHierarchy;
        boolean dumpPartitionHierarchy;
    }

    public static class BlockWeightsContext {
        private List<BlockWeight> perfectlyBalancedBlockWeights;
        private List<BlockWeight> maxBlockWeights;

        public void setup(PartitionContext pCtx) {
            if (pCtx.k == 0) {
                throw new IllegalStateException("PartitionContext::k not initialized");
            }
            if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
                throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
            }
            if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
                throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
            }

            long perfectlyBalancedBlockWeight = (long) Math.ceil(1.0 * pCtx.totalNodeWeight.value / pCtx.k);
            long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * perfectlyBalancedBlockWeight);

            maxBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k, new BlockWeight(0)));
            perfectlyBalancedBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k, new BlockWeight(0)));

            ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int b = 0; b < pCtx.k; b++) {
                        perfectlyBalancedBlockWeights.set(b, new BlockWeight(perfectlyBalancedBlockWeight));
                        if (pCtx.maxNodeWeight.value == 1) {
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                        } else {
                            maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight, perfectlyBalancedBlockWeight + pCtx.maxNodeWeight.value)));
                        }
                    }
                }
            });
        }

        public void setup(PartitionContext pCtx, int inputK) {
            if (pCtx.k == 0) {
                throw new IllegalStateException("PartitionContext::k not initialized");
            }
            if (pCtx.totalNodeWeight == kaminpar.kInvalidNodeWeight) {
                throw new IllegalStateException("PartitionContext::total_node_weight not initialized");
            }
            if (pCtx.maxNodeWeight == kaminpar.kInvalidNodeWeight) {
                throw new IllegalStateException("PartitionContext::max_node_weight not initialized");
            }

            double blockWeight = 1.0 * pCtx.totalNodeWeight.value / inputK;

            maxBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k, new BlockWeight(0)));
            perfectlyBalancedBlockWeights = new ArrayList<>(Collections.nCopies(pCtx.k, new BlockWeight(0)));

            ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int b = 0; b < pCtx.k; b++) {
                        int finalK = computeFinalK(b, pCtx.k, inputK);
                        perfectlyBalancedBlockWeights.set(b, new BlockWeight((long) Math.ceil(finalK * blockWeight)));
                        long maxBlockWeight = (long) ((1.0 + pCtx.epsilon) * perfectlyBalancedBlockWeights.get(b).value);
                        if (pCtx.maxNodeWeight.value == 1) {
                            maxBlockWeights.set(b, new BlockWeight(maxBlockWeight));
                        } else {
                            maxBlockWeights.set(b, new BlockWeight(Math.max(maxBlockWeight, perfectlyBalancedBlockWeights.get(b).value + pCtx.maxNodeWeight.value)));
                        }
                    }
                }
            });
        }

        public List<BlockWeight> allMax() {
            return maxBlockWeights;
        }

        public List<BlockWeight> allPerfectlyBalanced() {
            return perfectlyBalancedBlockWeights;
        }

    }

    public GraphOrdering rearrangeBy;
    public PartitioningContext partitioning;
    public PartitionContext partition;
    public CoarseningContext coarsening;
    public InitialPartitioningContext initialPartitioning;
    public RefinementContext refinement;
    public ParallelContext parallel;
    public DebugContext debug;

    public void setup(Graph graph) {
        partition.setup(graph);
    }
}
