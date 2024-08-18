package org.alshar.kaminpar_shm.refinement;
import org.alshar.common.context.*;
import org.alshar.kaminpar_shm.*;
public abstract class Refiner {
    public Refiner() {
    }

    public Refiner(Refiner other) {
        throw new UnsupportedOperationException("Copy constructor is not supported");
    }

    public Refiner(Refiner other, boolean isCopy) {
        throw new UnsupportedOperationException("Copy constructor is not supported");
    }

    public void initialize(PartitionedGraph pGraph) {
        // To be implemented by subclasses
    }

    public abstract boolean refine(PartitionedGraph pGraph, PartitionContext pCtx);

    public Refiner(Refiner other, boolean isCopy, boolean isMove) {
        throw new UnsupportedOperationException("Move constructor is not supported");
    }

    public void operatorAssign(Refiner other) {
        throw new UnsupportedOperationException("Assignment operator is not supported");
    }
}

