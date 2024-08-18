package org.alshar.kaminpar_shm.coarsening;

import org.alshar.Graph;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.common.datastructures.Pair;
import org.alshar.kaminpar_shm.PartitionedGraph;

public abstract class Coarsener {

    public Coarsener() {
        // Default constructor
    }

    public Coarsener(Coarsener other) {
        // Deleted copy constructor equivalent
        throw new UnsupportedOperationException("Copy constructor is not supported.");
    }

    public Coarsener(Coarsener other, boolean isMoveConstructor) {
        // Deleted move constructor equivalent
        throw new UnsupportedOperationException("Move constructor is not supported.");
    }

    // Coarsen the currently coarsest graph with a static maximum node weight.
    public abstract Pair<Graph, Boolean> computeCoarseGraph(NodeWeight maxClusterWeight, NodeID toSize);

    // Return the currently coarsest graph, or the input graph, if no coarse graphs have been computed so far.
    public abstract Graph coarsestGraph();

    // Return number of coarse graphs that have already been computed.
    public abstract int size();

    // Whether we have not computed any coarse graphs so far.
    public boolean isEmpty() {
        return size() == 0;
    }

    // Projects a partition of the currently coarsest graph onto the next finer graph and frees the currently coarsest graph.
    public abstract PartitionedGraph uncoarsen(PartitionedGraph pGraph);

    // Re-initialize this coarsener object with a new graph.
    public abstract void initialize(Graph graph);
}
