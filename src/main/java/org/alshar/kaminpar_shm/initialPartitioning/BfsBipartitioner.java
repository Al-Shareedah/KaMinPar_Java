package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.context.InitialPartitioningContext;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.*;
import org.alshar.kaminpar_shm.refinement.Marker;

import java.util.List;

// BFS Bipartitioner implementation
public class BfsBipartitioner extends BfsBipartitionerBase {
    private final List<Queue<NodeID>> queues;
    private final Marker marker;
    private final int numSeedIterations;
    private final BlockSelectionStrategy blockSelectionStrategy;

    public BfsBipartitioner(Graph graph, PartitionContext pCtx, InitialPartitioningContext iCtx, MemoryContext mCtx, BlockSelectionStrategy strategy) {
        super(graph, pCtx, iCtx);
        this.queues = mCtx.queues;
        this.marker = mCtx.marker;
        this.numSeedIterations = iCtx.numSeedIterations;
        this.blockSelectionStrategy = strategy;

        if (marker.capacity() < graph.n().getValue()) {
            marker.resize(graph.n().getValue());
        }
        if (queues.get(0).capacity() < graph.n().getValue()) {
            queues.get(0).resize(graph.n().getValue());
        }
        if (queues.get(1).capacity() < graph.n().getValue()) {
            queues.get(1).resize(graph.n().getValue());
        }
    }

    @Override
    protected void bipartitionImpl() {
        // Implementation of bipartition algorithm similar to the original C++ code
        Pair<NodeID, NodeID> farAwayNodes = SeedNodeUtils.findFarAwayNodes(graph, numSeedIterations);
        NodeID startA = farAwayNodes.getKey();
        NodeID startB = farAwayNodes.getValue();

        queues.get(0).pushTail(startA);
        queues.get(1).pushTail(startB);
        marker.set(startA.getValue(), 0, true);
        marker.set(startB.getValue(), 1, true);

        BlockID active = new BlockID(0);

        while (marker.firstUnmarkedElement(2) < graph.n().getValue()) {
            if (queues.get(active.getValue()).empty()) {
                NodeID firstUnassignedNode = new NodeID(marker.firstUnmarkedElement(2));
                if (marker.get(firstUnassignedNode.getValue(), active.getValue())) {
                    active = new BlockID(1 - active.getValue());
                    continue;
                }
                queues.get(active.getValue()).pushTail(firstUnassignedNode);
                marker.set(firstUnassignedNode.getValue(), active.getValue(), true);
            }

            NodeID u = queues.get(active.getValue()).head();
            queues.get(active.getValue()).popHead();

            if (!marker.get(u.getValue(), 2)) {
                NodeWeight weight = blockWeights.get(active.getValue());
                boolean assignmentAllowed = weight.add(graph.nodeWeight(u)).compareTo(pCtx.blockWeights.max(active.getValue())) <= 0;
                active = assignmentAllowed ? active : new BlockID(1 - active.getValue());

                setBlock(u, active);
                marker.set(u.getValue(), 2, true);

                for (NodeID v : graph.adjacentNodes(u)) {
                    if (marker.get(v.getValue(), 2) || marker.get(v.getValue(), active.getValue())) {
                        continue;
                    }
                    queues.get(active.getValue()).pushTail(v);
                    marker.set(v.getValue(), active.getValue(), true);
                }
            }

            active = blockSelectionStrategy.select(active, blockWeights, pCtx, queues);
        }

        marker.reset();
        queues.get(0).clear();
        queues.get(1).clear();
    }
}
