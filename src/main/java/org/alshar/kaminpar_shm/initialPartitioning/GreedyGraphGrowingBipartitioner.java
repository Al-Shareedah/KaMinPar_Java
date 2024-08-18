package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.common.GraphUtils.Edge;
import org.alshar.common.datastructures.*;
import org.alshar.kaminpar_shm.refinement.Marker;
import org.alshar.Graph;
import org.alshar.common.Math.Random_shm;
import org.alshar.common.context.InitialPartitioningContext;
import org.alshar.common.context.PartitionContext;

import java.util.Arrays;
public class GreedyGraphGrowingBipartitioner extends Bipartitioner {
    public static class MemoryContext {
        BinaryMinHeap<EdgeWeight> queue = new BinaryMinHeap<>(0, new EdgeWeight(Long.MIN_VALUE));

        Marker marker = new Marker(0, 1);

        public long memoryInKB() {
            return queue.memoryInKB() + marker.memoryInKB();
        }
    }

    private final BinaryMinHeap<EdgeWeight> queue;
    private final Marker marker;

    public GreedyGraphGrowingBipartitioner(
            Graph graph,
            PartitionContext pCtx,
            InitialPartitioningContext iCtx,
            MemoryContext mCtx
    ) {
        super(graph, pCtx, iCtx);
        this.queue = mCtx.queue;
        this.marker = mCtx.marker;

        if (queue.capacity() < graph.n().value) {
            queue.resize(graph.n().getValue());
        }
        if (marker.size() < graph.n().getValue()) {
            marker.resize(graph.n().getValue());
        }
    }

    @Override
    protected void bipartitionImpl() {
        if (graph.n().getValue() == 0) return;

        Arrays.fill(partition.getArray(), V1);
        // Correct the method call to set the total node weight
        blockWeights.add(V1.value, new NodeWeight(graph.totalNodeWeight().value));


        Random_shm rand = Random_shm.getInstance();

        do {
            // find random unmarked node
            NodeID startNode = new NodeID(0);
            int counter = 0;
            do {
                startNode = new NodeID(rand.randomIndex(0, graph.n().getValue()));
                counter++;
            } while (marker.get(startNode) && counter < 5);
            if (marker.get(startNode)) {
                startNode = new NodeID(marker.firstUnmarkedElement(0));
            }

            queue.push(startNode.getValue(), computeNegativeGain(startNode));
            marker.set(startNode.getValue(), 0, true);

            while (!queue.isEmpty()) {
                NodeID u = new NodeID(queue.peekId());
                if (queue.peekKey().equals(computeNegativeGain(u))) {
                    queue.pop();
                    changeBlock(u, V2);
                    if (blockWeights.get(V2.value).compareTo(pCtx.blockWeights.perfectlyBalanced(V2.value)) >= 0) {
                        break;
                    }

                    for (Edge e : graph.neighbors(u)) {
                        NodeID v = graph.edgeTarget(e.getEdgeID());
                        if (partition.get(u.value).equals(V2)) continue;

                        if (marker.get(v)) {
                            queue.decreasePriority(v.getValue(), new EdgeWeight(2 * graph.edgeWeight(e.getEdgeID()).value));
                        } else {
                            queue.push(v.getValue(), computeNegativeGain(v));
                            marker.set(v.getValue(), 0, true);
                        }
                    }
                }
            }
        } while (blockWeights.get(V2.value).compareTo(pCtx.blockWeights.perfectlyBalanced(V2.value)) < 0);


        marker.reset();
        queue.clear();
    }

    private EdgeWeight computeNegativeGain(NodeID u) {
        EdgeWeight gain = new EdgeWeight(0);
        for (Edge e : graph.neighbors(u)) {
            NodeID v = graph.edgeTarget(e.getEdgeID());
            if (partition.get(u.getValue()).equals(partition.get(v.getValue()))) {
                gain = gain.add(graph.edgeWeight(e.getEdgeID()));
            } else {
                gain = gain.add(new EdgeWeight(-graph.edgeWeight(e.getEdgeID()).value));

            }
        }
        return gain;
    }
}
