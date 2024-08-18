package org.alshar;

import org.alshar.common.enums.*;
import org.alshar.common.context.*;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static org.alshar.kaminpar_shm.PartitionUtils.computeFinalK;

public class Context {
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
