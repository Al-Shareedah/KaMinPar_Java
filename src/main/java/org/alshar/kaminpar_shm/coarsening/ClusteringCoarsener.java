package org.alshar.kaminpar_shm.coarsening;

import org.alshar.Graph;
import org.alshar.common.GraphUtils.ContractResult;
import org.alshar.common.GraphUtils.MemoryContext;
import org.alshar.common.datastructures.*;
import org.alshar.common.context.CoarseningContext;
import org.alshar.kaminpar_shm.PartitionedGraph;

import java.util.ArrayList;
import java.util.List;

public class ClusteringCoarsener extends Coarsener {
    private final Graph inputGraph;
    private Graph currentGraph;
    private final List<Graph> hierarchy = new ArrayList<>();
    private final List<int[]> mapping = new ArrayList<>();

    private final Clusterer clusteringAlgorithm;
    private final CoarseningContext cCtx;

    public ClusteringCoarsener(Clusterer clusteringAlgorithm, Graph inputGraph, CoarseningContext cCtx) {
        this.inputGraph = inputGraph;
        this.currentGraph = inputGraph;
        this.clusteringAlgorithm = clusteringAlgorithm;
        this.cCtx = cCtx;
    }

    @Override
    public Pair<Graph, Boolean> computeCoarseGraph(NodeWeight maxClusterWeight, NodeID toSize) {
        // Set parameters for clustering
        clusteringAlgorithm.setMaxClusterWeight(maxClusterWeight);
        clusteringAlgorithm.setDesiredClusterCount(toSize);

        // Compute clustering
        int[] clusteringArray = clusteringAlgorithm.computeClustering(currentGraph);

        // Convert int[] to StaticArray<Integer>
        StaticArray<Integer> clustering = new StaticArray<>(clusteringArray.length);
        for (int i = 0; i < clusteringArray.length; i++) {
            clustering.set(i, clusteringArray[i]);
        }

        // Contract the graph based on the clustering
        Contraction.MemoryContext mCtx = new Contraction.MemoryContext(currentGraph.n().value);
        ContractResult result = Contraction.contract(currentGraph, clustering, mCtx);

        // Extract the contracted graph from the result
        Graph contractedGraph = result.getCoarseGraph();

        // Determine if coarsening has converged
        boolean converged = cCtx.coarseningShouldConverge(currentGraph.n().value, contractedGraph.n().value);

        // Update the hierarchy and mapping
        hierarchy.add(contractedGraph);
        mapping.add(clusteringArray); // Keep the original int[] for the mapping
        currentGraph = contractedGraph;

        // Return the new coarse graph and whether coarsening has not converged
        return new Pair<>(currentGraph, !converged);
    }

    @Override
    public PartitionedGraph uncoarsen(PartitionedGraph pGraph) {
        // Uncoarsen logic (assuming correct mapping and hierarchy usage)
        int[] lastMapping = mapping.remove(mapping.size() - 1);
        Graph previousGraph = hierarchy.remove(hierarchy.size() - 1);
        Graph finerGraph = hierarchy.isEmpty() ? inputGraph : hierarchy.get(hierarchy.size() - 1);

        // Create the uncoarsened partition
        StaticArray<BlockID> partition = new StaticArray<>(finerGraph.n().value);
        for (int u = 0; u < finerGraph.n().value; ++u) {
            partition.set(u, pGraph.block(new NodeID(lastMapping[u])));
        }

        // Update the current graph
        currentGraph = finerGraph;

        return new PartitionedGraph(finerGraph, pGraph.k(), partition);
    }

    @Override
    public Graph coarsestGraph() {
        return currentGraph;
    }

    @Override
    public int size() {
        return hierarchy.size();
    }

    @Override
    public void initialize(Graph graph) {
        // Re-initialize the coarsener with a new graph
        this.currentGraph = graph;
        hierarchy.clear();
        mapping.clear();
    }

    public CoarseningContext getContext() {
        return cCtx;
    }
}
