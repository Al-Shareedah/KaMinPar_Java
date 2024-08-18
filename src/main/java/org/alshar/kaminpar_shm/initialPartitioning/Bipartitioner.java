package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.Graph;
import org.alshar.common.Seq;
import org.alshar.common.context.InitialPartitioningContext;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.BlockWeight;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;
import org.alshar.common.datastructures.StaticArray;

import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.kaminpar_shm.kaminpar;


import java.util.Arrays;
public abstract class Bipartitioner {

    protected static final BlockID V1 = new BlockID(0);
    protected static final BlockID V2 = new BlockID(1);

    protected final Graph graph;
    protected final PartitionContext pCtx;
    protected final InitialPartitioningContext iCtx;

    protected StaticArray<BlockID> partition;
    protected BlockWeights blockWeights;

    public Bipartitioner(Graph graph, PartitionContext pCtx, InitialPartitioningContext iCtx) {
        this.graph = graph;
        this.pCtx = pCtx;
        this.iCtx = iCtx;

        if (pCtx.k.value != 2) {
            throw new IllegalArgumentException("Not a bipartition context");
        }

        this.blockWeights = new BlockWeights();
    }

    // Copy constructor is not allowed
    public Bipartitioner(Bipartitioner other) {
        throw new UnsupportedOperationException("Copy constructor is not allowed");
    }

    // Assignment operator is not allowed
    public Bipartitioner assign(Bipartitioner other) {
        throw new UnsupportedOperationException("Assignment operator is not allowed");
    }

    // Move constructor
    public Bipartitioner(Bipartitioner other, boolean move) {
        if (!move) {
            throw new UnsupportedOperationException("Move constructor is allowed only with 'move' flag set to true");
        }
        this.graph = other.graph;
        this.pCtx = other.pCtx;
        this.iCtx = other.iCtx;
        this.partition = other.partition;
        this.blockWeights = other.blockWeights;
    }

    // Move assignment operator
    public Bipartitioner assign(Bipartitioner other, boolean move) {
        if (!move) {
            throw new UnsupportedOperationException("Move assignment operator is allowed only with 'move' flag set to true");
        }
        return this;
    }

    public PartitionedGraph bipartition(StaticArray<BlockID> partition) {
        return new PartitionedGraph(new Seq(), graph, new BlockID(2), bipartitionRaw(partition));
    }


    public StaticArray<BlockID> bipartitionRaw(StaticArray<BlockID> partition) {
        if (graph.n().value == 0) {
            return new StaticArray<>(0);
        }
        this.partition = partition != null ? partition : new StaticArray<>(graph.n().value);

        if (this.partition.size() < graph.n().value) {
            this.partition.resize(graph.n().value);
        }

        Arrays.fill(this.partition.getArray(), kaminpar.kInvalidBlockID.value);

        blockWeights.reset();
        bipartitionImpl();

        return this.partition;
    }

    protected abstract void bipartitionImpl();

    protected void addToSmallerBlock(NodeID u) {
        NodeWeight delta1 = blockWeights.get(0).subtract(pCtx.blockWeights.perfectlyBalanced(0));
        NodeWeight delta2 = blockWeights.get(1).subtract(pCtx.blockWeights.perfectlyBalanced(1));
        BlockID block = delta1.compareTo(delta2) < 0 ? V1 : V2;
        setBlock(u, block);
    }

    protected void setBlock(NodeID u, BlockID b) {
        Object value = partition.get(u.value);

        BlockID currentBlockID;
        if (value instanceof BlockID) {
            currentBlockID = (BlockID) value;
        } else if (value instanceof Integer) {
            // If somehow an Integer is stored, convert it to BlockID
            currentBlockID = new BlockID((Integer) value);
        } else {
            throw new IllegalStateException("Unexpected value type in partition: " + value.getClass().getName());
        }


        //BlockID currentBlockID = new BlockID(partition.get(u.value).getValue());
        if (!currentBlockID.equals(kaminpar.kInvalidBlockID)) {
            throw new IllegalArgumentException("use updateBlock() instead");
        }
        partition.set(u.value, b);
        blockWeights.add(b.value, graph.nodeWeight(u));
    }

    protected void changeBlock(NodeID u, BlockID b) {
        if (partition.get(u.value).equals(BlockID.kInvalidBlockID)) {
            throw new IllegalArgumentException("only use setBlock() instead");
        }
        partition.set(u.value, b);

        NodeWeight uWeight = graph.nodeWeight(u);
        blockWeights.add(b.value, uWeight);
        blockWeights.subtract(otherBlock(b).value, uWeight);
    }

    protected BlockID otherBlock(BlockID b) {
        return new BlockID(1 - b.value);
    }

    // Nested class for BlockWeights
    public static class BlockWeights {
        private final BlockWeight[] weights = new BlockWeight[2];

        public BlockWeights() {
            weights[0] = new BlockWeight(0);
            weights[1] = new BlockWeight(0);
        }

        public BlockWeight get(int index) {
            return weights[index];
        }

        public void add(int index, NodeWeight value) {
            weights[index] = weights[index].add(value);
        }

        public void subtract(int index, NodeWeight value) {
            weights[index] = weights[index].subtract(value);
        }

        public void reset() {
            weights[0] = new BlockWeight(0);
            weights[1] = new BlockWeight(0);
        }
    }
}
