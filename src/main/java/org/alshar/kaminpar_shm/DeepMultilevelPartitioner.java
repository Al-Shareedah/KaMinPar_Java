package org.alshar.kaminpar_shm;

import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.GraphUtils.*;
import org.alshar.common.Logger;
import org.alshar.common.context.*;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.EdgeID;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.common.enums.InitialPartitioningMode;
import org.alshar.kaminpar_shm.coarsening.Coarsener;
import org.alshar.kaminpar_shm.refinement.Helper;
import org.alshar.kaminpar_shm.refinement.Refiner;

import java.util.*;

import static org.alshar.common.cio.printDelimiter;
import static org.alshar.kaminpar_shm.PartitionUtils.computeMaxClusterWeight;
import static org.alshar.kaminpar_shm.refinement.Helper.coarsenOnce;
import static org.alshar.kaminpar_shm.refinement.Helper.computeKForN;

public class DeepMultilevelPartitioner extends Partitioner {
    private final Graph inputGraph;
    private final Context inputCtx;
    private PartitionContext currentPCtx;

    // Coarsening
    private final Coarsener coarsener;

    // Refinement
    private final Refiner refiner;

    // Initial partitioning -> subgraph extraction
    private SubgraphMemory subgraphMemory = new SubgraphMemory();

    private TemporaryGraphExtractionBufferPool ipExtractionPool = new TemporaryGraphExtractionBufferPool();

    // Initial partitioning
    private GlobalInitialPartitionerMemoryPool ipMCtxPool = new GlobalInitialPartitionerMemoryPool();


    public DeepMultilevelPartitioner(Graph inputGraph, Context inputCtx) {
        this.inputGraph = inputGraph;
        this.inputCtx = inputCtx;
        this.currentPCtx = inputCtx.partition;
        this.coarsener = Factory.createCoarsener(inputGraph, inputCtx.coarsening);
        this.refiner = Factory.createRefiner(inputCtx);
    }

    @Override
    public PartitionedGraph partition() {
        printDelimiter("Partitioning");

        Graph cGraph = coarsen();
        PartitionedGraph pGraph = initialPartition(cGraph);

        boolean refined = false;
        pGraph = uncoarsen(pGraph, refined);

        if (!refined || pGraph.k().value < inputCtx.partition.k.value) {
            if (!refined) {
                refine(pGraph);
            }
            if (pGraph.k().value < inputCtx.partition.k.value) {
                extendPartition(pGraph, inputCtx.partition.k);
                refine(pGraph);
            }
        }

        // Calculate connected components in each block before returning pGraph
        calculateConnectedComponentsAndCCM(pGraph);

        printStatistics();
        return pGraph;
    }

    private PartitionedGraph uncoarsen(PartitionedGraph pGraph, boolean refined) {
        while (!coarsener.isEmpty()) {  // Check if there are more levels to uncoarsen
            Logger.log("Uncoarsening -> Level " + coarsener.size());

            pGraph = uncoarsenOnce(pGraph);
            refine(pGraph);
            refined = true;

            BlockID desiredK = computeKForN(pGraph.n(), inputCtx);
            if (pGraph.k().value < desiredK.value) {
                extendPartition(pGraph, desiredK);
                refined = false;
            }
        }
        return pGraph;
    }


    private PartitionedGraph uncoarsenOnce(PartitionedGraph pGraph) {
        return Helper.uncoarsenOnce(coarsener, pGraph, currentPCtx, inputCtx.partition);
    }

    private void refine(PartitionedGraph pGraph) {
        // If requested, dump the current partition to disk before refinement ...
        //Debug.dumpPartitionHierarchy(pGraph, coarsener.size(), "pre-refinement", inputCtx);

        Logger.log("  Running refinement on " + pGraph.k().value + " blocks");
        Helper.refine(refiner, pGraph, currentPCtx);

        Logger.log("    Cut:       " + Metrics.edgeCut(pGraph).value);
        Logger.log("    Imbalance: " + Metrics.imbalance(pGraph));
        Logger.log("    Feasible:  " + (Metrics.isFeasible(pGraph, currentPCtx) ? "yes" : "no"));

        // ... and dump it after refinement.
        //Debug.dumpPartitionHierarchy(pGraph, coarsener.size(), "post-refinement", inputCtx);
    }


    private void extendPartition(PartitionedGraph pGraph, BlockID kPrime) {
        Logger.log("  Extending partition from " + pGraph.k().value + " blocks to " + kPrime.value + " blocks");

        // Call the method in the Helper class
        Helper.extendPartition(pGraph, kPrime, inputCtx, currentPCtx, ipExtractionPool, ipMCtxPool);

        Logger.log("    Cut:       " + Metrics.edgeCut(pGraph).value);
        Logger.log("    Imbalance: " + Metrics.imbalance(pGraph));
    }


    private Graph coarsen() {
        Graph cGraph = inputGraph;
        NodeID prevCGraphN = cGraph.n();
        EdgeID prevCGraphM = cGraph.m();
        boolean shrunk = true;

        while (shrunk && cGraph.n().value > initialPartitioningThreshold().value) {


            // Store the size of the previous coarse graph, so that we can pre-allocate subgraphMemory
            prevCGraphN = cGraph.n();
            prevCGraphM = cGraph.m();

            // Build next coarse graph
            shrunk = coarsenOnce(coarsener, cGraph, inputCtx, currentPCtx);
            cGraph = coarsener.coarsestGraph();

            // Pre-allocate subgraphMemory if needed
            if (subgraphMemory.isEmpty() && computeKForN(cGraph.n(), inputCtx).value < inputCtx.partition.k.value) {
                subgraphMemory.resize(prevCGraphN, inputCtx.partition.k, prevCGraphM, true, true);
            }


            // Print some metrics for the coarse graphs
            NodeWeight maxClusterWeight = computeMaxClusterWeight(inputCtx.coarsening, cGraph, inputCtx.partition);
            Logger.log("Coarsening -> Level " + coarsener.size());
            Logger.log("  Number of nodes: " + cGraph.n().value + " | Number of edges: " + cGraph.m().value);
            Logger.log("  Maximum node weight: " + cGraph.maxNodeWeight().value + " <= " + maxClusterWeight.value);
        }

        if (subgraphMemory.isEmpty()) {
            subgraphMemory.resize(prevCGraphN, inputCtx.partition.k, prevCGraphM, true, true);
        }

        if (shrunk) {
            Logger.log("==> Coarsening terminated with less than " + initialPartitioningThreshold().value + " nodes.");
        } else {
            Logger.log("==> Coarsening converged.");
        }

        return cGraph;
    }
    private void calculateCCM(List<Integer> actualSizes, int totalNodes) {
        List<Integer> desiredSizes = Arrays.asList(7538, 2261, 678, 203);
        int totalDifference = 0;
        int partitionsNotMeetingSize = 0;
        double totalPercentageOff = 0.0;

        for (int i = 0; i < desiredSizes.size(); i++) {
            int actualSize = (i < actualSizes.size()) ? actualSizes.get(i) : 0;
            int difference = Math.abs(desiredSizes.get(i) - actualSize);

            totalDifference += difference;

            if (difference > 0) {
                partitionsNotMeetingSize++;
                totalPercentageOff += (double) difference / desiredSizes.get(i) * 100.0;
            }
        }

        double ncdm = (double) totalDifference / totalNodes;
        double percentageNotMeetingSize = (double) partitionsNotMeetingSize / desiredSizes.size() * 100.0;
        double averagePercentageOff = partitionsNotMeetingSize > 0 ? totalPercentageOff / partitionsNotMeetingSize : 0.0;

        Logger.log("Cardinality Compliance Metric (CCM): " + ncdm);
        Logger.log("Percentage of partitions not meeting desired size: " + percentageNotMeetingSize + "%");
        Logger.log("Average percentage by which partitions missed the desired size: " + averagePercentageOff + "%");
    }
    private void calculateConnectedComponentsAndCCM(PartitionedGraph pGraph) {
        Map<BlockID, Set<NodeID>> blockNodes = new HashMap<>();
        for (NodeID u : pGraph.nodes()) {
            blockNodes.computeIfAbsent(pGraph.block(u), k -> new HashSet<>()).add(u);
        }

        List<Integer> actualSizes = new ArrayList<>();
        for (Map.Entry<BlockID, Set<NodeID>> entry : blockNodes.entrySet()) {
            BlockID blockId = entry.getKey();
            Set<NodeID> nodes = entry.getValue();

            Map<NodeID, Boolean> visitedNodes = new HashMap<>();
            for (NodeID node : nodes) {
                visitedNodes.put(node, false);
            }

            Map<NodeID, List<NodeID>> connectedComponents = new HashMap<>();

            for (NodeID node : nodes) {
                if (!visitedNodes.get(node)) {
                    List<NodeID> component = new ArrayList<>();
                    Stack<NodeID> stack = new Stack<>();
                    stack.push(node);

                    while (!stack.isEmpty()) {
                        NodeID u = stack.pop();

                        if (!visitedNodes.get(u)) {
                            visitedNodes.put(u, true);
                            component.add(u);

                            for (NodeID v : pGraph.adjacentNodes(u)) {
                                if (pGraph.block(v).equals(blockId) && !visitedNodes.get(v)) {
                                    stack.push(v);
                                }
                            }
                        }
                    }

                    connectedComponents.put(node, component);
                }
            }

            actualSizes.add(nodes.size());
        }

        // Calculate CCM based on the desired sizes and actual block sizes
        calculateCCM(actualSizes, pGraph.n().value);
    }




    private NodeID initialPartitioningThreshold() {
        if (helperParallelIpMode(inputCtx.partitioning.deepInitialPartitioningMode)) {
            return new NodeID(inputCtx.parallel.numThreads * inputCtx.coarsening.contractionLimit);
        } else {
            return new NodeID(2 * inputCtx.coarsening.contractionLimit);
        }
    }

    private PartitionedGraph initialPartition(Graph graph) {
        PartitionedGraph pGraph;
        switch (inputCtx.partitioning.deepInitialPartitioningMode) {
            case SEQUENTIAL:
                pGraph = Helper.bipartition(graph, inputCtx.partition.k, inputCtx, ipMCtxPool);
                break;
                /*
            case SYNCHRONOUS_PARALLEL:
                pGraph = new SyncInitialPartitioner(inputCtx, ipMCtxPool, ipExtractionPool)
                        .partition(coarsener, currentPCtx);
                break;
            case ASYNCHRONOUS_PARALLEL:
                pGraph = new AsyncInitialPartitioner(inputCtx, ipMCtxPool, ipExtractionPool)
                        .partition(coarsener, currentPCtx);
                break;

                 */
            default:
                throw new IllegalStateException("Unexpected value: " + inputCtx.partitioning.deepInitialPartitioningMode);
        }
        Helper.updatePartitionContext(currentPCtx, pGraph, inputCtx.partition.k);
        // Log the metrics for the initial partition
        Logger.log("  Number of blocks: " + pGraph.k().value);
        Logger.log("  Cut:              " + Metrics.edgeCut(pGraph).value);
        Logger.log("  Imbalance:        " + Metrics.imbalance(pGraph));
        Logger.log("  Feasible:         " + (Metrics.isFeasible(pGraph, currentPCtx) ? "yes" : "no"));
        return pGraph;
    }
    private boolean helperParallelIpMode(InitialPartitioningMode mode) {
        return mode == InitialPartitioningMode.ASYNCHRONOUS_PARALLEL ||
                mode == InitialPartitioningMode.SYNCHRONOUS_PARALLEL;
    }


    private void printStatistics() {
        long numIpMCtxObjects = 0;
        long maxIpMCtxObjects = 0;
        long minIpMCtxObjects = Long.MAX_VALUE;
        long ipMCtxMemoryInKB = 0;

        for (InitialPartitionerMemoryPool pool : ipMCtxPool.all()) {
            int poolSize = pool.size();
            numIpMCtxObjects += poolSize;
            maxIpMCtxObjects = Math.max(maxIpMCtxObjects, poolSize);
            minIpMCtxObjects = Math.min(minIpMCtxObjects, poolSize);
            ipMCtxMemoryInKB += pool.memoryInKB();
        }

        Logger.log("Initial partitioning: Memory pool");
        Logger.log(" * # of pool objects: " + minIpMCtxObjects + " <= " + (double) numIpMCtxObjects / inputCtx.parallel.numThreads + " <= " + maxIpMCtxObjects);
        Logger.log(" * total memory: " + ipMCtxMemoryInKB / 1000 + " Mb");

        long extractionNodesReallocs = 0;
        long extractionEdgesReallocs = 0;
        long extractionMemoryInKB = 0;

        for (TemporarySubgraphMemory buffer : ipExtractionPool.all()) {
            extractionNodesReallocs += buffer.getNumNodeReallocs();
            extractionEdgesReallocs += buffer.getNumEdgeReallocs();
            extractionMemoryInKB += buffer.memoryInKB();
        }


        Logger.log("Extraction buffer pool:");
        Logger.log(" * # of node buffer reallocs: " + extractionNodesReallocs + ", # of edge buffer reallocs: " + extractionEdgesReallocs);
        Logger.log(" * total memory: " + extractionMemoryInKB / 1000 + " Mb");
    }

}
