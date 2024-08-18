package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.GraphUtils.Edge;
import org.alshar.common.datastructures.*;
import org.alshar.kaminpar_shm.Metrics;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.common.context.InitialRefinementContext;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BinaryMinHeap;
import org.alshar.common.datastructures.EdgeWeight;
import org.alshar.common.Math.Random_shm;
import org.alshar.kaminpar_shm.refinement.Marker;

import java.util.ArrayList;
import java.util.List;
public class InitialTwoWayFMRefiner<Q extends QueueSelectionPolicy, C extends CutAcceptancePolicy, S extends StoppingPolicy> extends InitialRefiner {
    private static final int CHUNK_SIZE = 64;
    private static final int NUMBER_OF_NODE_PERMUTATIONS = 32;

    private final PartitionContext pCtx;
    private final InitialRefinementContext rCtx;
    private BinaryMinHeap<EdgeWeight>[] queues;
    private Marker marker;
    private List<EdgeWeight> weightedDegrees;
    private MaxOverloadSelectionPolicy queueSelectionPolicy;
    private BalancedMinCutAcceptancePolicy cutAcceptancePolicy;
    private S stoppingPolicy;
    private Random_shm rand;
    private Random_shm.RandomPermutations<NodeID> permutations;
    private Graph graph;

    public InitialTwoWayFMRefiner(int n, PartitionContext pCtx, InitialRefinementContext rCtx, MemoryContext mCtx, S stoppingPolicy) {
        this.pCtx = pCtx;
        this.rCtx = rCtx;
        this.queues = mCtx.queues;
        this.marker = mCtx.marker;
        this.weightedDegrees = mCtx.weightedDegrees;
        this.stoppingPolicy = stoppingPolicy;
        this.rand = Random_shm.getInstance();
        // Initialize queue selection policy
        this.queueSelectionPolicy = new MaxOverloadSelectionPolicy();
        this.cutAcceptancePolicy = new BalancedMinCutAcceptancePolicy();
        // Initialize permutations
        List<NodeID> initialElements = new ArrayList<>();
        for (int i = 0; i < CHUNK_SIZE; i++) {
            initialElements.add(new NodeID(i));
        }
        this.permutations = new Random_shm.RandomPermutations<>(rand, initialElements, NUMBER_OF_NODE_PERMUTATIONS);

        if (queues[0].capacity() < n) {
            queues[0].resize(n);
        }
        if (queues[1].capacity() < n) {
            queues[1].resize(n);
        }
        if (marker.capacity() < n) {
            marker.resize(n);
        }
        if (weightedDegrees.size() < n) {
            for (int i = weightedDegrees.size(); i < n; i++) {
                weightedDegrees.add(new EdgeWeight(0));
            }
        }
    }

    @Override
    public void initialize(Graph graph) {
        this.graph = graph;
        stoppingPolicy.reset();
        stoppingPolicy.init(this.graph);
        initWeightedDegrees();
    }

    @Override
    public boolean refine(PartitionedGraph pGraph, PartitionContext pCtx) {
        if (pGraph.k().value != 2) {
            throw new IllegalArgumentException("2-way refiner cannot be used on a " + pGraph.k() + "-way partition");
        }

        EdgeWeight initialEdgeCut = Metrics.edgeCutSeq(pGraph);
        if (initialEdgeCut.value == 0) {
            return false;
        }

        EdgeWeight prevEdgeCut = initialEdgeCut;
        EdgeWeight curEdgeCut = prevEdgeCut;

        curEdgeCut = curEdgeCut.add(round(pGraph));
        for (int it = 1; curEdgeCut.value > 0 && it < rCtx.numIterations && !abort(prevEdgeCut, curEdgeCut); ++it) {
            prevEdgeCut = curEdgeCut;
            curEdgeCut = curEdgeCut.add(round(pGraph));
        }

        return curEdgeCut.value < initialEdgeCut.value;
    }

    @Override
    public MemoryContext free() {
        MemoryContext mCtx = new MemoryContext();
        mCtx.queues = queues;
        mCtx.marker = marker;
        mCtx.weightedDegrees = weightedDegrees;
        return mCtx;
    }

    private boolean abort(EdgeWeight prevEdgeWeight, EdgeWeight curEdgeWeight) {
        return (1.0 - 1.0 * curEdgeWeight.value / prevEdgeWeight.value) < rCtx.improvementAbortionThreshold;
    }

    private EdgeWeight round(PartitionedGraph pGraph) {
        stoppingPolicy.reset();
        initPQ(pGraph);

        List<NodeID> moves = new ArrayList<>();
        int active = 0;

        NodeWeight currentOverload = Metrics.totalOverload(pGraph, pCtx);
        NodeWeight acceptedOverload = currentOverload;

        EdgeWeight currentDelta = new EdgeWeight(0);
        EdgeWeight acceptedDelta = new EdgeWeight(0);

        while ((!queues[0].isEmpty() || !queues[1].isEmpty()) && !stoppingPolicy.shouldStop(rCtx)) {
            active = queueSelectionPolicy.select(pGraph, pCtx, queues, rand);
            if (queues[active].isEmpty()) {
                active = 1 - active;
            }
            BinaryMinHeap<EdgeWeight> queue = queues[active];

            NodeID u = new NodeID(queue.peekId());
            EdgeWeight delta = queue.peekKey();
            BlockID from = new BlockID(active);
            BlockID to = new BlockID(1 - active);

            marker.set(u.value, 0, false);
            queue.pop();

            pGraph.setBlock(u, to);
            currentDelta = currentDelta.add(delta);
            moves.add(u);

            stoppingPolicy.update(delta.negate());
            currentOverload = Metrics.totalOverload(pGraph, pCtx);

            updateNeighborsGain(pGraph, u, from, to);

            if (cutAcceptancePolicy.accept(pGraph, pCtx, acceptedOverload, currentOverload, acceptedDelta, currentDelta)) {
                acceptedDelta = currentDelta;
                acceptedOverload = currentOverload;
                moves.clear();
            }
        }

        rollbackToAcceptedCut(pGraph, moves);
        resetForNextRun();

        return acceptedDelta;
    }


    private void initPQ(PartitionedGraph pGraph) {
        for (int i = 0; i < 2; i++) {
            queues[i].clear();
        }

        List<Integer> chunks = new ArrayList<>();
        for (int i = 0; i < graph.n().value / CHUNK_SIZE + 1; i++) {
            chunks.add(i * CHUNK_SIZE);
        }
        rand.shuffle(chunks);

        for (int chunk : chunks) {
            for (NodeID i : permutations.get()) {
                NodeID u = new NodeID(chunk + i.value);
                if (u.value < graph.n().value) {
                    insertNode(pGraph, u);
                }
            }
        }
    }

    private void insertNode(PartitionedGraph pGraph, NodeID u) {
        EdgeWeight gain = computeGainFromScratch(pGraph, u);
        BlockID uBlock = pGraph.block(u);
        if (weightedDegrees.get(u.value).compareTo(gain) != 0) {
            queues[uBlock.value].push(u.value, gain);
        }
    }

    private EdgeWeight computeGainFromScratch(PartitionedGraph pGraph, NodeID u) {
        BlockID uBlock = pGraph.block(u);
        EdgeWeight weightedExternalDegree = new EdgeWeight(0);

        for (Edge e : pGraph.neighbors(u)) {
            NodeID v = pGraph.edgeTarget(e.getEdgeID());
            if (!pGraph.block(v).equals(uBlock)) {
                weightedExternalDegree = weightedExternalDegree.add(pGraph.edgeWeight(e.getEdgeID()));
            }
        }

        EdgeWeight weightedInternalDegree = weightedDegrees.get(u.value).subtract(weightedExternalDegree);
        return weightedInternalDegree.subtract(weightedExternalDegree);
    }

    private void initWeightedDegrees() {
        for (NodeID u : graph.nodes()) {
            EdgeWeight weightedDegree = new EdgeWeight(0);
            for (EdgeID e : graph.incidentEdges(u)) {
                weightedDegree = weightedDegree.add(graph.edgeWeight(e));
            }
            weightedDegrees.set(u.value, weightedDegree);
        }
    }

    private void updateNeighborsGain(PartitionedGraph pGraph, NodeID u, BlockID from, BlockID to) {
        for (Edge e : graph.neighbors(u)) {
            NodeID v = pGraph.edgeTarget(e.getEdgeID());
            if (!marker.get(v)) {
                EdgeWeight edgeWeight = graph.edgeWeight(e.getEdgeID());
                BlockID vBlock = pGraph.block(v);
                EdgeWeight lossDelta = new EdgeWeight(2 * edgeWeight.value * ((to.equals(vBlock)) ? 1 : -1));

                if (queues[vBlock.value].contains(v.value)) {
                    EdgeWeight newLoss = queues[vBlock.value].key(v.value).add(lossDelta);
                    boolean stillBoundaryNode = newLoss.compareTo(weightedDegrees.get(v.value)) < 0;

                    if (!stillBoundaryNode) {
                        queues[vBlock.value].remove(v.value);
                    } else {
                        queues[vBlock.value].changePriority(v.value, newLoss);
                    }
                } else {
                    queues[vBlock.value].push(v.value, weightedDegrees.get(v.value).add(lossDelta));
                }
            }
        }
    }

    private void rollbackToAcceptedCut(PartitionedGraph pGraph, List<NodeID> moves) {
        for (NodeID u : moves) {
            pGraph.setBlock(u, new BlockID(1 - pGraph.block(u).value));
        }
    }

    private void resetForNextRun() {
        for (int i = 0; i < 2; i++) {
            queues[i].clear();
        }
        marker.reset();
    }
}


