package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.Graph;
import org.alshar.common.Seq;
import org.alshar.common.context.InitialPartitioningContext;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.datastructures.StaticArray;
import org.alshar.common.Logger;
import org.alshar.kaminpar_shm.Metrics;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.kaminpar_shm.initialPartitioning.InitialRefiner;

import java.util.ArrayList;
import java.util.List;
public class PoolBipartitioner {

    private static final boolean DEBUG = false;

    private final Graph graph;
    private final PartitionContext pCtx;
    private final InitialPartitioningContext iCtx;
    private final int minNumRepetitions;
    private final int minNumNonAdaptiveRepetitions;
    private final int maxNumRepetitions;
    private int numRepetitions;

    private MemoryContext mCtx;

    private StaticArray<BlockID> bestPartition;
    private EdgeWeight bestCut = new EdgeWeight(Long.MAX_VALUE);
    private boolean bestFeasible = false;
    private double bestImbalance = 0.0;
    private int bestBipartitioner;

    private StaticArray<BlockID> currentPartition;

    private final List<String> bipartitionerNames = new ArrayList<>();
    private final List<Bipartitioner> bipartitioners = new ArrayList<>();
    private final InitialRefiner refiner;

    private final List<RunningVariance> runningStatistics = new ArrayList<>();
    private final Statistics statistics = new Statistics();

    public static class MemoryContext {
        public GreedyGraphGrowingBipartitioner.MemoryContext gggMCtx = new GreedyGraphGrowingBipartitioner.MemoryContext();
        public BfsBipartitionerBase.MemoryContext bfsMCtx = new BfsBipartitionerBase.MemoryContext();
        public RandomBipartitioner.MemoryContext randMCtx = new RandomBipartitioner.MemoryContext();
        public InitialRefiner.MemoryContext refMCtx = new InitialRefiner.MemoryContext();

        public long memoryInKB() {
            return gggMCtx.memoryInKB() + bfsMCtx.memoryInKB() + randMCtx.memoryInKB() + refMCtx.memoryInKB();
        }
    }

    public static class BipartitionerStatistics {
        public List<EdgeWeight> cuts = new ArrayList<>();
        public double cutMean;
        public double cutVariance;
        public int numFeasiblePartitions;
        public int numInfeasiblePartitions;
    }

    public static class Statistics {
        public List<BipartitionerStatistics> perBipartitioner = new ArrayList<>();
        public EdgeWeight bestCut;
        public int bestBipartitioner;
        public boolean bestFeasible;
        public double bestImbalance;
        public int numBalancedPartitions;
        public int numImbalancedPartitions;
    }

    public PoolBipartitioner(Graph graph, PartitionContext pCtx, InitialPartitioningContext iCtx, MemoryContext mCtx) {
        this.graph = graph;
        this.pCtx = pCtx;
        this.iCtx = iCtx;
        this.minNumRepetitions = iCtx.minNumRepetitions;
        this.minNumNonAdaptiveRepetitions = iCtx.minNumNonAdaptiveRepetitions;
        this.maxNumRepetitions = iCtx.maxNumRepetitions;
        this.mCtx = mCtx;
        this.refiner = InitialRefinerFactory.createInitialRefiner(graph, pCtx, iCtx.refinement, mCtx.refMCtx);
        this.refiner.initialize(graph);
        this.bestPartition = new StaticArray<>(graph.n().value);
        this.currentPartition = new StaticArray<>(graph.n().value);
    }

    public void registerBipartitioner(String name, Bipartitioner bipartitioner) {
        bipartitionerNames.add(name);
        bipartitioners.add(bipartitioner);
        runningStatistics.add(new RunningVariance());
        statistics.perBipartitioner.add(new BipartitionerStatistics());
    }

    public String getBipartitionerName(int i) {
        return bipartitionerNames.get(i);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void reset() {
        int n = bipartitioners.size();
        runningStatistics.clear();
        for (int i = 0; i < n; i++) {
            runningStatistics.add(new RunningVariance());
        }
        statistics.perBipartitioner.clear();
        for (int i = 0; i < n; i++) {
            statistics.perBipartitioner.add(new BipartitionerStatistics());
        }
        bestFeasible = false;
        bestCut = new EdgeWeight(Long.MAX_VALUE);
        bestImbalance = 0.0;
        bestPartition = new StaticArray<>(graph.n().value);
    }

    public PartitionedGraph bipartition() {
        int repetitions = Math.max(Math.min(numRepetitions, maxNumRepetitions), minNumRepetitions);
        for (int rep = 0; rep < repetitions; rep++) {
            for (int i = 0; i < bipartitioners.size(); i++) {
                if (rep < minNumNonAdaptiveRepetitions || !iCtx.useAdaptiveBipartitionerSelection || likelyToImprove(i)) {
                    runBipartitioner(i);
                }
            }
        }
        finalizeStatistics();
        if (DEBUG) {
            printStatistics();
        }
        return new PartitionedGraph(new Seq(),graph, new BlockID(2), bestPartition);

    }

    public MemoryContext free() {
        mCtx.refMCtx = refiner.free();
        return mCtx;
    }

    public void setNumRepetitions(int numRepetitions) {
        this.numRepetitions = numRepetitions;
    }

    private boolean likelyToImprove(int i) {
        RunningVariance rv = runningStatistics.get(i);
        double[] meanAndVariance = rv.get();
        double mean = meanAndVariance[0];
        double variance = meanAndVariance[1];
        double rhs = (mean - bestCut.value) / 2;
        return variance > rhs * rhs;
    }

    private void finalizeStatistics() {
        for (int i = 0; i < bipartitioners.size(); i++) {
            RunningVariance rv = runningStatistics.get(i);
            double[] meanAndVariance = rv.get();
            statistics.perBipartitioner.get(i).cutMean = meanAndVariance[0];
            statistics.perBipartitioner.get(i).cutVariance = meanAndVariance[1];
        }
        statistics.bestCut = bestCut;
        statistics.bestFeasible = bestFeasible;
        statistics.bestImbalance = bestImbalance;
        statistics.bestBipartitioner = bestBipartitioner;
    }

    private void printStatistics() {
        int numRunsTotal = 0;

        for (int i = 0; i < bipartitioners.size(); i++) {
            BipartitionerStatistics stats = statistics.perBipartitioner.get(i);
            int numRuns = stats.numFeasiblePartitions + stats.numInfeasiblePartitions;
            numRunsTotal += numRuns;

            Logger.log("- " + bipartitionerNames.get(i));
            Logger.log("  * num=" + numRuns + " num_feasible_partitions=" + stats.numFeasiblePartitions + " num_infeasible_partitions=" + stats.numInfeasiblePartitions);
            Logger.log("  * cut_mean=" + stats.cutMean + " cut_variance=" + stats.cutVariance + " cut_std_dev=" + Math.sqrt(stats.cutVariance));
        }

        Logger.log("Winner: " + bipartitionerNames.get(bestBipartitioner));
        Logger.log(" * cut=" + bestCut.value + " imbalance=" + bestImbalance + " feasible=" + bestFeasible);
        Logger.log("# of runs: " + numRunsTotal + " of " + bipartitioners.size() * Math.max(Math.min(numRepetitions, maxNumRepetitions), minNumRepetitions));
    }

    private void runBipartitioner(int i) {
        //Logger.log("Running bipartitioner " + bipartitionerNames.get(i) + " on graph with n=" + graph.n().value + " m=" + graph.m().value);
        PartitionedGraph pGraph = bipartitioners.get(i).bipartition(currentPartition);
        //Logger.log(" -> running refiner ...");
        refiner.refine(pGraph, pCtx);
        //Logger.log(" -> cut=" + Metrics.edgeCut(pGraph).value + " imbalance=" + Metrics.imbalance(pGraph));

        EdgeWeight currentCut = Metrics.edgeCutSeq(pGraph);
        double currentImbalance = Metrics.imbalance(pGraph);
        boolean currentFeasible = Metrics.isFeasible(pGraph, pCtx);
        currentPartition = pGraph.takeRawPartition();

        if (currentFeasible) {
            statistics.perBipartitioner.get(i).cuts.add(currentCut);
            statistics.perBipartitioner.get(i).numFeasiblePartitions++;
            runningStatistics.get(i).update(currentCut.value);
        } else {
            statistics.perBipartitioner.get(i).numInfeasiblePartitions++;
        }

        if (!bestFeasible && currentFeasible ||
                (bestFeasible == currentFeasible && (currentCut.compareTo(bestCut) < 0 ||
                        (currentCut.equals(bestCut) && currentImbalance < bestImbalance)))) {
            bestCut = currentCut;
            bestImbalance = currentImbalance;
            bestFeasible = currentFeasible;
            bestBipartitioner = i;
            StaticArray<BlockID> temp = currentPartition;
            currentPartition = bestPartition;
            bestPartition = temp;
        }
    }

    private static class RunningVariance {
        private int count = 0;
        private double mean = 0.0;
        private double M2 = 0.0;

        public double[] get() {
            if (count == 0) {
                return new double[]{Double.MAX_VALUE, 0.0};
            } else if (count < 2) {
                return new double[]{mean, 0.0};
            } else {
                return new double[]{mean, M2 / count};
            }
        }

        public void reset() {
            mean = 0.0;
            count = 0;
            M2 = 0.0;
        }

        public void update(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            M2 += delta * delta2;
        }
    }
}
