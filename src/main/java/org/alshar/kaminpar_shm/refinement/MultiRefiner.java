package org.alshar.kaminpar_shm.refinement;
import org.alshar.common.datastructures.BlockWeight;
import org.alshar.common.datastructures.StaticArray;
import org.alshar.kaminpar_shm.PartitionedGraph;
import org.alshar.common.context.PartitionContext;
import org.alshar.common.enums.RefinementAlgorithm;

import java.util.*;

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
    private void reorderGraphBlockWeights(PartitionedGraph pGraph) {
        // Get the block weights as a StaticArray<BlockWeight>
        StaticArray<BlockWeight> blockWeights = pGraph.getBlockWeights();

        // Convert StaticArray<BlockWeight> to List<BlockWeight> for sorting
        List<BlockWeight> blockWeightList = new ArrayList<>();
        for (int i = 0; i < blockWeights.size(); i++) {
            blockWeightList.add(blockWeights.get(i));
        }

        // Sort the block weights in ascending order
        blockWeightList.sort(Comparator.comparingLong(bw -> bw.value));

        // Convert the sorted List<BlockWeight> back to StaticArray<BlockWeight>
        StaticArray<BlockWeight> sortedBlockWeights = new StaticArray<>(blockWeightList.size());
        for (int i = 0; i < blockWeightList.size(); i++) {
            sortedBlockWeights.set(i, blockWeightList.get(i));
        }

        // Set the sorted block weights back to pGraph
        pGraph.setBlockWeights(sortedBlockWeights);
    }
    private void reorderPartitionContextWeights(PartitionContext pCtx) {
        List<BlockWeight> perfectlyBalancedBlockWeights = pCtx.blockWeights.perfectlyBalancedBlockWeights;
        List<BlockWeight> maxBlockWeights = pCtx.blockWeights.maxBlockWeights;

        // Create a list of indices to sort
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < perfectlyBalancedBlockWeights.size(); i++) {
            indices.add(i);
        }

        // Sort the indices based on the values in perfectlyBalancedBlockWeights
        indices.sort(Comparator.comparingInt(i -> (int) perfectlyBalancedBlockWeights.get(i).value));

        // Reorder both perfectlyBalancedBlockWeights and maxBlockWeights using the sorted indices
        List<BlockWeight> sortedPerfectlyBalanced = new ArrayList<>();
        List<BlockWeight> sortedMaxWeights = new ArrayList<>();

        for (int index : indices) {
            sortedPerfectlyBalanced.add(perfectlyBalancedBlockWeights.get(index));
            sortedMaxWeights.add(maxBlockWeights.get(index));
        }

        // Update the PartitionContext with sorted values
        pCtx.blockWeights.perfectlyBalancedBlockWeights = sortedPerfectlyBalanced;
        pCtx.blockWeights.maxBlockWeights = sortedMaxWeights;
    }



}