package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.Graph;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.NodeID;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.common.datastructures.StaticArray;
import org.alshar.common.Seq;


import java.util.ArrayList;
import java.util.List;
public class SequentialGraphHierarchy {
    private final Graph finestGraph;
    private final List<List<NodeID>> coarseMappings;
    private final List<Graph> coarseGraphs;

    // Constructor
    public SequentialGraphHierarchy(Graph finestGraph) {
        this.finestGraph = finestGraph;
        this.coarseMappings = new ArrayList<>();
        this.coarseGraphs = new ArrayList<>();
    }
    public SequentialGraphHierarchy() {
        this.finestGraph = new Graph();  // Assuming Graph has a no-argument constructor
        this.coarseMappings = new ArrayList<>();
        this.coarseGraphs = new ArrayList<>();
    }


    // Disable copying
    public SequentialGraphHierarchy(SequentialGraphHierarchy other) {
        throw new UnsupportedOperationException("Copying of SequentialGraphHierarchy is not allowed");
    }

    public SequentialGraphHierarchy clone() {
        throw new UnsupportedOperationException("Copying of SequentialGraphHierarchy is not allowed");
    }

    // Move constructor
    public SequentialGraphHierarchy(SequentialGraphHierarchy other, boolean move) {
        this.finestGraph = other.finestGraph;
        this.coarseMappings = new ArrayList<>(other.coarseMappings);
        this.coarseGraphs = new ArrayList<>(other.coarseGraphs);
    }

    // Method to take a coarse graph and its mapping
    public void takeCoarseGraph(Graph coarseGraph, List<NodeID> coarseMapping) {
        assert coarsestGraph().n().value == coarseMapping.size();
        this.coarseMappings.add(new ArrayList<>(coarseMapping));
        this.coarseGraphs.add(coarseGraph);
    }

    // Get the coarsest graph
    public Graph coarsestGraph() {
        return coarseGraphs.isEmpty() ? finestGraph : coarseGraphs.get(coarseGraphs.size() - 1);
    }

    // Pop and project the coarse partitioned graph onto a finer graph
    public PartitionedGraph popAndProject(PartitionedGraph coarsePGraph) {
        assert !coarseGraphs.isEmpty();
        assert coarseGraphs.get(coarseGraphs.size() - 1).equals(coarsePGraph.getGraph());

        // Get the mapping and coarse graph to project onto
        List<NodeID> coarseMapping = new ArrayList<>(coarseMappings.remove(coarseMappings.size() - 1));

        // Get the second-coarsest graph (or finest graph if there's only one)
        Graph finerGraph = getSecondCoarsestGraph();

        assert finerGraph.n().value == coarseMapping.size();

        // Project the coarse partitioned graph to the finer graph
        StaticArray<BlockID> partition = new StaticArray<>(finerGraph.n().value);
        for (NodeID u : finerGraph.nodes()) {
            partition.set(u.value, coarsePGraph.block(coarseMapping.get(u.value)));
        }

        // This removes the last coarse graph since we're done with it
        coarseGraphs.remove(coarseGraphs.size() - 1);

        // This destroys underlying Graph wrapped in coarsePGraph
        return new PartitionedGraph(new Seq(), finerGraph, coarsePGraph.k(), partition);
    }

    // Get the number of coarse graphs
    public int size() {
        return coarseGraphs.size();
    }

    // Check if there are any coarse graphs
    public boolean isEmpty() {
        return coarseGraphs.isEmpty();
    }

    // Get the coarse mappings
    public List<List<NodeID>> getCoarseMappings() {
        return coarseMappings;
    }

    // Get the coarse graphs
    public List<Graph> getCoarseGraphs() {
        return coarseGraphs;
    }

    // Get the second-coarsest graph (private helper method)
    private Graph getSecondCoarsestGraph() {
        assert !coarseGraphs.isEmpty();
        return coarseGraphs.size() > 1 ? coarseGraphs.get(coarseGraphs.size() - 2) : finestGraph;
    }
}
