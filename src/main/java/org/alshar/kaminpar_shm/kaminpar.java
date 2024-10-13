package org.alshar.kaminpar_shm;

import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.GraphUtils.Permutator;
import org.alshar.common.Logger;
import org.alshar.common.datastructures.*;
import org.alshar.common.Math.Random_shm;
import org.alshar.common.cio;
import org.alshar.common.enums.*;
import org.alshar.common.timer.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadPoolExecutor;

public class kaminpar {
    public static final int kInvalidNodeID = Integer.MAX_VALUE;
    public static final int kInvalidEdgeID = Integer.MAX_VALUE;
    public static final NodeWeight kInvalidNodeWeight = new NodeWeight(Integer.MAX_VALUE);
    public static final EdgeWeight kInvalidEdgeWeight = new EdgeWeight(Integer.MAX_VALUE);
    public static final BlockWeight kInvalidBlockWeight = new BlockWeight(Integer.MAX_VALUE);
    public static final BlockID kInvalidBlockID = new BlockID(Integer.MAX_VALUE);
    public static class KaMinPar {
        private int numThreads;
        private int maxTimerDepth = Integer.MAX_VALUE;
        private OutputLevel outputLevel = OutputLevel.APPLICATION;
        private Context ctx;
        private Graph graph;
        private boolean wasRearranged = false;
        private ThreadPoolExecutor gc;

        public KaMinPar(int numThreads, Context ctx) {
            this.numThreads = numThreads;
            this.ctx = ctx;
            this.gc = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
            resetGlobalTimer();
        }

        public static void reseed(int seed) {
            // Logic to reseed
            Random_shm.reseed(seed);
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

        public void takeGraph(int n, StaticArray<EdgeID> xadj, StaticArray<NodeID> adjncy, StaticArray<NodeWeight> vwgt, StaticArray<EdgeWeight> adjwgt) {
            borrowAndMutateGraph(n, xadj, adjncy, vwgt, adjwgt);
        }

        public void borrowAndMutateGraph(int n, StaticArray<EdgeID> xadj, StaticArray<NodeID> adjncy, StaticArray<NodeWeight> vwgt, StaticArray<EdgeWeight> adjwgt) {
            // Logic to borrow and mutate graph
            EdgeID m = new EdgeID(xadj.get(n).value);
            StaticArray<EdgeID> nodes = new StaticArray<>(xadj);
            StaticArray<NodeID> edges = new StaticArray<>(adjncy);
            // Initialize nodeWeights and edgeWeights based on the condition
            StaticArray<NodeWeight> nodeWeights;
            if (vwgt.size() == 0) {
                nodeWeights = new StaticArray<>(xadj.size());
                for (int i = 0; i < nodeWeights.size(); i++) {
                    nodeWeights.set(i, new NodeWeight(1)); // Initialize with value 1
                }
            } else {
                nodeWeights = new StaticArray<>(vwgt);
            }

            StaticArray<EdgeWeight> edgeWeights;
            if (adjwgt.size() == 0) {
                edgeWeights = new StaticArray<>(adjncy.size());
                for (int i = 0; i < edgeWeights.size(); i++) {
                    edgeWeights.set(i, new EdgeWeight(1)); // Initialize with value 1
                }
            } else {
                edgeWeights = new StaticArray<>(adjwgt);
            }

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

            Timer_km.global().startTimer("Partitioning");

            // Perform actual partitioning
            Partitioner partitioner = Factory.createPartitioner(graph, ctx)
                    .orElseThrow(() -> new IllegalStateException("Partitioner could not be created"));
            final PartitionedGraphWrapper pGraphWrapper = new PartitionedGraphWrapper();
            pGraphWrapper.pGraph = partitioner.partition();

            // Re-integrate isolated nodes that were cut off during preprocessing
            if (graph.permuted()) {
                NodeID numIsolatedNodes = Permutator.integrateIsolatedNodes(graph, originalEpsilon, ctx);
                pGraphWrapper.pGraph = Permutator.assignIsolatedNodes(pGraphWrapper.pGraph, numIsolatedNodes, ctx.partition);
            }
            // Stop the Partitioning timer
            Timer_km.global().stopTimer();

            Timer_km.global().startTimer("IO");
            if (graph.permuted()) {
                ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                    @Override
                    protected void compute() {
                        for (long u = 0; u < pGraphWrapper.pGraph.n().value; u++) {
                            partition[(int) u] = pGraphWrapper.pGraph.block(graph.mapOriginalNode(new NodeID((int) u)));
                        }
                    }
                });
            } else {
                ForkJoinPool.commonPool().invoke(new RecursiveAction() {
                    @Override
                    protected void compute() {
                        for (long u = 0; u < pGraphWrapper.pGraph.n().value; u++) {
                            partition[(int) u] = pGraphWrapper.pGraph.block(new NodeID((int)u));
                        }
                    }
                });
            }
            Timer_km.global().stopTimer();

            Timer_km.global().stopTimer();
            if (outputLevel.compareTo(OutputLevel.APPLICATION) >= 0) {
                printStatistics(ctx, pGraphWrapper.pGraph, maxTimerDepth, outputLevel == OutputLevel.EXPERIMENT);
            }

            EdgeWeight finalCut = Metrics.edgeCut(pGraphWrapper.pGraph);

            // Reset the global timer
            Timer_km.global().reset();

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
                if (Timer_km.global().isEnabled()) {
                    Logger.log("TIME ");
                    Timer_km.global().printMachineReadable(System.out, maxTimerDepth);
                } else {
                    Logger.log("TIME disabled");
                }
            }

            if (Timer_km.global().isEnabled()) {
                Timer_km.global().printHumanReadable(System.out, maxTimerDepth);
            } else {
                Logger.log("Global Timers: disabled");
            }
            Logger.log("");
            Logger.log("Partition summary:");
            if (pGraph.k().value != ctx.partition.k.value) {
                Logger.log("  Number of blocks: " + pGraph.k().value);
            } else {
                Logger.log("  Number of blocks: " + pGraph.k().value);
            }
            Logger.log("  Edge cut:         " + cut.value);
            Logger.log("  Imbalance:        " + imbalance);
            if (feasible) {
                Logger.log("  Feasible:         yes");
            } else {
                Logger.log("  Feasible:         no");
            }
        }

        private void resetGlobalTimer() {
            if (Timer_km.global().isEnabled()) {
                Timer_km.global().reset();
            }
        }
    }





}

