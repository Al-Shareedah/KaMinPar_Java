package org.alshar.kaminpar_shm.refinement;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.enums.RefinementAlgorithm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiRefiner extends Refiner {
    private final Map<RefinementAlgorithm, Refiner> refiners;
    private final List<RefinementAlgorithm> order;

    public MultiRefiner(Map<RefinementAlgorithm, Refiner> refiners, List<RefinementAlgorithm> order) {
        this.refiners = new HashMap<>(refiners);
        this.order = List.copyOf(order);
    }

    @Override
    public void initialize(PartitionedGraph pGraph) {
        // The initialize method is intentionally empty, as per the C++ implementation.
    }

    @Override
    public boolean refine(PartitionedGraph pGraph, PartitionContext pCtx) {
        boolean foundImprovement = false;
        for (RefinementAlgorithm algorithm : order) {
            Refiner refiner = refiners.get(algorithm);
            if (refiner != null) {
                refiner.initialize(pGraph);
                foundImprovement |= refiner.refine(pGraph, pCtx);
            }
        }
        return foundImprovement;
    }
}