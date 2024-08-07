package org.alshar.kaminpar_shm;

import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.StaticArray;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class kaminpar {

    public static class KaMinPar {
        private int numThreads;
        private int maxTimerDepth = Integer.MAX_VALUE;
        private OutputLevel outputLevel = OutputLevel.APPLICATION;
        private Context ctx;
        private Graph graph;
        private boolean wasRearranged = false;

        public KaMinPar(int numThreads, Context ctx) {
            this.numThreads = numThreads;
            this.ctx = ctx;
            this.gc = Executors.newFixedThreadPool(numThreads);
            resetGlobalTimer();
        }

        public static void reseed(int seed) {
            // Logic to reseed
            Random.getInstance().setSeed(seed);
        }

        public void setOutputLevel(OutputLevel outputLevel) {
            this.outputLevel = outputLevel;
        }

        public void setMaxTimerDepth(int maxTimerDepth) {
            this.maxTimerDepth = maxTimerDepth;
        }

        public Context getContext() {
            return ctx;
        }

        public void takeGraph(NodeID n, EdgeID[] xadj, NodeID[] adjncy, NodeWeight[] vwgt, EdgeWeight[] adjwgt) {
            borrowAndMutateGraph(n, xadj, adjncy, vwgt, adjwgt);
        }

        public void borrowAndMutateGraph(NodeID n, EdgeID[] xadj, NodeID[] adjncy, NodeWeight[] vwgt, EdgeWeight[] adjwgt) {
            // Logic to borrow and mutate graph
            EdgeID m = new EdgeID(xadj[(int) n.value].value);
            StaticArray<EdgeID> nodes = new StaticArray<>(xadj);
            StaticArray<NodeID> edges = new StaticArray<>(adjncy);
            StaticArray<NodeWeight> nodeWeights = (vwgt == null) ? new StaticArray<>(0) : new StaticArray<>(vwgt);
            StaticArray<EdgeWeight> edgeWeights = (adjwgt == null) ? new StaticArray<>(0) : new StaticArray<>(adjwgt);

            graph = new Graph(nodes, edges, nodeWeights, edgeWeights, false);
        }

        public void copyGraph(NodeID n, EdgeID[] xadj, NodeID[] adjncy, NodeWeight[] vwgt, EdgeWeight[] adjwgt) {
            // Logic to copy graph
            EdgeID m = new EdgeID(xadj[(int) n.value].value);
            boolean hasNodeWeights = vwgt != null;
            boolean hasEdgeWeights = adjwgt != null;

            StaticArray<EdgeID> nodes = new StaticArray<>(xadj.length);
            StaticArray<NodeID> edges = new StaticArray<>(adjncy.length);
            StaticArray<NodeWeight> nodeWeights = hasNodeWeights ? new StaticArray<>(vwgt.length) : new StaticArray<>(0);
            StaticArray<EdgeWeight> edgeWeights = hasEdgeWeights ? new StaticArray<>(adjwgt.length) : new StaticArray<>(0);

            nodes.set((int) n.value, new EdgeID(xadj[(int) n.value].value));
            ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int u = 0; u < n.value; u++) {
                        nodes.set(u, new EdgeID(xadj[u].value));
                        if (hasNodeWeights) {
                            nodeWeights.set(u, new NodeWeight(vwgt[u].value));
                        }
                    }
                }
            });

            ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                @Override
                protected void compute() {
                    for (int e = 0; e < m.value; e++) {
                        edges.set(e, new NodeID(adjncy[e].value));
                        if (hasEdgeWeights) {
                            edgeWeights.set(e, new EdgeWeight(adjwgt[e].value));
                        }
                    }
                }
            });

            graph = new Graph(nodes, edges, nodeWeights, edgeWeights, false);
        }

        public EdgeWeight computePartition(BlockID k, BlockID[] partition) {
            // Logic to compute partition
            boolean quietMode = outputLevel == OutputLevel.QUIET;
            Logger.setQuietMode(quietMode);

            // Print banners and input summary
            cio.printKaminparBanner();
            cio.printBuildIdentifier();
            cio.printBuildDatatypes(NodeID.class, EdgeID.class, NodeWeight.class, EdgeWeight.class);
            cio.printDelimiter("Input Summary", '#');

            double originalEpsilon = ctx.partition.epsilon;
            ctx.parallel.numThreads = numThreads;
            ctx.partition.k = k;

            // Setup graph dependent context parameters
            ctx.setup(graph);

            // Initialize console output
            if (outputLevel.compareTo(OutputLevel.APPLICATION) >= 0) {
                cio.print(ctx, System.out);
            }

            Timer.start("Partitioning");
            if (ctx.rearrangeBy == GraphOrdering.DEGREE_BUCKETS && !wasRearranged) {
                graph = GraphUtils.rearrangeByDegreeBuckets(ctx, graph);
                wasRearranged = true;
            }

            // Perform actual partitioning
            PartitionedGraph pGraph = Factory.createPartitioner(graph, ctx).partition();

            // Re-integrate isolated nodes that were cut off during preprocessing
            if (graph.permuted()) {
                NodeID numIsolatedNodes = GraphUtils.integrateIsolatedNodes(graph, originalEpsilon, ctx);
                pGraph = GraphUtils.assignIsolatedNodes(pGraph, numIsolatedNodes, ctx.partition);
            }
            Timer.stop();

            Timer.start("IO");
            if (graph.permuted()) {
                ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                    @Override
                    protected void compute() {
                        for (long u = 0; u < pGraph.n(); u++) {
                            partition[(int) u] = pGraph.block(graph.mapOriginalNode(new NodeID(u)));
                        }
                    }
                });
            } else {
                ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                    @Override
                    protected void compute() {
                        for (long u = 0; u < pGraph.n(); u++) {
                            partition[(int) u] = pGraph.block(new NodeID(u));
                        }
                    }
                });
            }
            Timer.stop();

            // Print some statistics
            Timer.stop(); // stop root timer
            if (outputLevel.compareTo(OutputLevel.APPLICATION) >= 0) {
                printStatistics(ctx, pGraph, maxTimerDepth, outputLevel == OutputLevel.EXPERIMENT);
            }

            EdgeWeight finalCut = Metrics.edgeCut(pGraph);

            resetGlobalTimer();

            return finalCut;
        }

        private void printStatistics(Context ctx, PartitionedGraph pGraph, int maxTimerDepth, boolean parseable) {
            EdgeWeight cut = Metrics.edgeCut(pGraph);
            double imbalance = Metrics.imbalance(pGraph);
            boolean feasible = Metrics.isFeasible(pGraph, ctx.partition);

            cio.printDelimiter("Result Summary");

            // Statistics output that is easy to parse
            if (parseable) {
                Logger.log("RESULT cut=" + cut.value + " imbalance=" + imbalance + " feasible=" + feasible + " k=" + pGraph.k());
                if (Timers.isEnabled()) {
                    Logger.log("TIME ");
                    Timers.printMachineReadable(System.out);
                } else {
                    Logger.log("TIME disabled");
                }
            }

            if (Timers.isEnabled()) {
                Timers.printHumanReadable(System.out, maxTimerDepth);
            } else {
                Logger.log("Global Timers: disabled");
            }
            Logger.log();
            Logger.log("Partition summary:");
            if (pGraph.k() != ctx.partition.k.value) {
                Logger.log(Logger.RED + "  Number of blocks: " + pGraph.k());
            } else {
                Logger.log("  Number of blocks: " + pGraph.k());
            }
            Logger.log("  Edge cut:         " + cut.value);
            Logger.log("  Imbalance:        " + imbalance);
            if (feasible) {
                Logger.log("  Feasible:         yes");
            } else {
                Logger.log(Logger.RED + "  Feasible:         no");
            }
        }

        private void resetGlobalTimer() {
            // Logic to reset the global timer if timers are enabled
            if (Timers.isEnabled()) {
                Timers.reset();
            }
        }
    }

    public static class NodeID {
        public long value;

        public NodeID(long value) {
            this.value = value;
        }
    }

    public static class EdgeID {
        public long value;

        public EdgeID(long value) {
            this.value = value;
        }
    }

    public static class NodeWeight {
        public long value;

        public NodeWeight(long value) {
            this.value = value;
        }
    }

    public static class EdgeWeight {
        public long value;

        public EdgeWeight(long value) {
            this.value = value;
        }
    }

    public static class BlockID {
        public int value;

        public BlockID(int value) {
            this.value = value;
        }
    }

    public static class BlockWeight {
        public long value;

        public BlockWeight(long value) {
            this.value = value;
        }
    }

    public enum OutputLevel {
        QUIET,
        PROGRESS,
        APPLICATION,
        EXPERIMENT
    }

    public enum GraphOrdering {
        NATURAL,
        DEGREE_BUCKETS
    }

    public enum ClusteringAlgorithm {
        NOOP,
        LABEL_PROPAGATION
    }

    public enum ClusterWeightLimit {
        EPSILON_BLOCK_WEIGHT,
        BLOCK_WEIGHT,
        ONE,
        ZERO
    }

    public enum TwoHopStrategy {
        DISABLE,
        MATCH,
        MATCH_THREADWISE,
        CLUSTER,
        CLUSTER_THREADWISE,
        LEGACY
    }

    public enum IsolatedNodesClusteringStrategy {
        KEEP,
        MATCH,
        CLUSTER,
        MATCH_DURING_TWO_HOP,
        CLUSTER_DURING_TWO_HOP
    }

    public enum RefinementAlgorithm {
        LABEL_PROPAGATION,
        KWAY_FM,
        GREEDY_BALANCER,
        JET,
        MTKAHYPAR,
        NOOP
    }

    public enum FMStoppingRule {
        SIMPLE,
        ADAPTIVE
    }

    public enum GainCacheStrategy {
        SPARSE,
        DENSE,
        ON_THE_FLY,
        HYBRID,
        TRACING
    }

    public enum InitialPartitioningMode {
        SEQUENTIAL,
        ASYNCHRONOUS_PARALLEL,
        SYNCHRONOUS_PARALLEL
    }

    public enum PartitioningMode {
        DEEP,
        RB,
        KWAY
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

        public boolean coarseningShouldConverge(int oldN, int newN) {
            return (1.0 - 1.0 * newN / oldN) <= convergenceThreshold;
        }
    }

    public static class LabelPropagationRefinementContext {
        long numIterations;
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
        long constantHighDegreeThreshold;
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
        long numIterations;
        double improvementAbortionThreshold;
    }

    public static class InitialPartitioningContext {
        InitialCoarseningContext coarsening;
        InitialRefinementContext refinement;
        double repetitionMultiplier;
        long minNumRepetitions;
        long minNumNonAdaptiveRepetitions;
        long maxNumRepetitions;
        long numSeedIterations;
        boolean useAdaptiveBipartitionerSelection;
    }

    public static class BlockWeightsContext {
        private List<BlockWeight> perfectlyBalancedBlockWeights;
        private List<BlockWeight> maxBlockWeights;

        public void setup(Context.PartitionContext pCtx) {
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

        public void setup(Context.PartitionContext pCtx, int inputK) {
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
                        int finalK = PartitionUtils.computeFinalK(b, pCtx.k, inputK);
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
}

