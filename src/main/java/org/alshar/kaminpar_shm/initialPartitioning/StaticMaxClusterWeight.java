package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;

import java.util.function.Function;

@FunctionalInterface
public interface StaticMaxClusterWeight extends Function<NodeID, NodeWeight> {
    NodeWeight apply(NodeID nodeId);

    static StaticMaxClusterWeight of(NodeWeight maxClusterWeight) {
        return nodeId -> maxClusterWeight;
    }
}