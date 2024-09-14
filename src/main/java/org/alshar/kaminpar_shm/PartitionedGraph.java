package org.alshar.kaminpar_shm;

import org.alshar.Graph;
import org.alshar.common.datastructures.*;
import org.alshar.common.Seq;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

public class PartitionedGraph extends GraphDelegate {
    private BlockID k;
    private StaticArray<BlockID> partition;
    private StaticArray<BlockWeight> blockWeights;

    // Parallel constructor
    public PartitionedGraph(Graph graph, BlockID k, StaticArray<BlockID> partition) {
        super(graph);
        this.k = k;
        this.partition = partition != null ? partition : new StaticArray<>(graph.n().value);
        this.blockWeights = new StaticArray<>(k.value);

        if (partition != null) {
            initBlockWeightsSeq();
        } else {
            initBlockWeightsSeq();
        }
    }

    // Sequential constructor
    public PartitionedGraph(Seq seqTag, Graph graph, BlockID k, StaticArray<BlockID> partition) {
        super(graph);
        this.k = k;
        this.partition = partition != null ? partition : new StaticArray<>(graph.n().value);
        this.blockWeights = new StaticArray<>(k.value);
        // Initialize block weights to 0
        for (int i = 0; i < k.value; ++i) {
            this.blockWeights.set(i, new BlockWeight(0));
        }
        if (partition != null) {
            initBlockWeightsSeq();
        } else {
            initBlockWeightsSeq();
        }
    }

    public boolean move(NodeID u, BlockID from, BlockID to, BlockWeight maxToWeight) {
        // Explicitly convert NodeWeight to BlockWeight
        BlockWeight delta = new BlockWeight(nodeWeight(u).value);
        if (moveBlockWeight(from, to, delta, maxToWeight)) {
            setBlock(u, to);
            return true;
        }
        return false;
    }

    public <T> void setBlock(NodeID u, BlockID to) {
        BlockWeight weight = new BlockWeight(nodeWeight(u).value);
        BlockID from = block(u);
        if (!from.isInvalid()) {
            decreaseBlockWeight(from, weight);
        }
        increaseBlockWeight(to, weight);
        partition.set(u.value, to);
    }

    public boolean moveBlockWeight(BlockID from, BlockID to, BlockWeight delta, BlockWeight maxToWeight) {
        // Convert StaticArray<BlockWeight> to AtomicLongArray
        AtomicLongArray atomicBlockWeights = new AtomicLongArray(blockWeights.size());
        for (int i = 0; i < blockWeights.size(); i++) {
            atomicBlockWeights.set(i, blockWeights.get(i).getValue());
        }

        while (true) {
            long newWeight = atomicBlockWeights.get(to.value);
            if (newWeight + delta.getValue() <= maxToWeight.getValue()) {
                if (atomicBlockWeights.compareAndSet(to.value, newWeight, newWeight + delta.getValue())) {
                    // Decrease block weight for 'from' and update the original StaticArray
                    decreaseBlockWeight(from, delta);

                    // Convert AtomicLongArray back to StaticArray<BlockWeight>
                    for (int i = 0; i < blockWeights.size(); i++) {
                        blockWeights.set(i, new BlockWeight(atomicBlockWeights.get(i)));
                    }

                    return true;
                }
            } else {
                return false;
            }
        }
    }
    public Iterable<BlockID> blocks() {
        return () -> new Iterator<BlockID>() {
            private int current = 0;

            @Override
            public boolean hasNext() {
                return current < k.value; // Iterate up to the total number of blocks (k)
            }

            @Override
            public BlockID next() {
                return new BlockID(current++);
            }
        };
    }


    public BlockWeight blockWeight(BlockID blockID) {
        return blockWeights.get(blockID.value);
    }

    public void setBlockWeight(BlockID b, BlockWeight weight) {
        blockWeights.set(b.value, weight);
    }

    public void increaseBlockWeight(BlockID b, BlockWeight by) {
        blockWeights.set(b.value, blockWeights.get(b.value).add(by));
    }

    private void decreaseBlockWeight(BlockID from, BlockWeight delta) {
        // Decrease block weight logic
        BlockWeight currentWeight = blockWeights.get(from.value);
        blockWeights.set(from.value, new BlockWeight(currentWeight.getValue() - delta.getValue()));
    }

    public BlockID k() {
        return k;
    }

    public BlockID block(NodeID u) {
        return partition.get(u.value);
    }

    public void initBlockWeightsPar() {
        ThreadLocal<StaticArray<BlockWeight>> blockWeightsTLS = ThreadLocal.withInitial(() -> new StaticArray<>(k.value));

        // Parallel processing of nodes
        IntStream.range(0, graph.n().value).parallel().forEach(u -> {
            BlockID blockId = block(new NodeID(u));
            if (!blockId.equals(kaminpar.kInvalidBlockID)) {
                BlockWeight weight = (BlockWeight) nodeWeight(new NodeID(u));
                StaticArray<BlockWeight> blockWeights = blockWeightsTLS.get();
                blockWeights.set(blockId.value, blockWeights.get(blockId.value).add(weight));
            }
        });

        // Reduce the block weights across threads
        AtomicLongArray aggregatedBlockWeights = new AtomicLongArray(k.value);
        IntStream.range(0, k.value).parallel().forEach(b -> {
            blockWeightsTLS.get().forEach(weight -> {
                aggregatedBlockWeights.addAndGet(b, weight.getValue());
            });
        });

        // Store the aggregated block weights in the class variable
        IntStream.range(0, k.value).forEach(b -> {
            blockWeights.set(b, new BlockWeight(aggregatedBlockWeights.get(b)));
        });
    }

    public void initBlockWeightsSeq() {
        for (int u = 0; u < graph.n().value; ++u) {
            BlockID blockId = block(new NodeID(u));
            if (!blockId.equals(kaminpar.kInvalidBlockID)) {
                NodeWeight nodeWeight= nodeWeight(new NodeID(u));
                BlockWeight blockWeight=new BlockWeight(nodeWeight.value); // Create a new BlockWeight from NodeWeight
                blockWeights.set(blockId.value, blockWeights.get(blockId.value).add(blockWeight));
            }
        }
    }
    public StaticArray<Integer> takeRawBlockWeights() {
        // Assuming BlockWeight wraps an integer in this context
        StaticArray<Integer> rawBlockWeights = new StaticArray<>(blockWeights.size());
        for (int i = 0; i < blockWeights.size(); i++) {
            rawBlockWeights.set(i, (int) blockWeights.get(i).value);
        }
        return rawBlockWeights;
    }
    public StaticArray<BlockID> takeRawPartition() {
        StaticArray<BlockID> temp = this.partition;
        this.partition = null;  // Clear the partition to signify it's been moved
        return temp;
    }



}
