package org.alshar.kaminpar_shm;

import org.alshar.common.Math.MathUtils;
import org.alshar.common.context.*;
import org.alshar.Graph;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.NodeWeight;

public class PartitionUtils {

    public static int computeFinalK(int block, int currentK, int inputK) {
        if (currentK == inputK) {
            return 1;
        }

        int level = (int) (Math.log(currentK) / Math.log(2));
        int base = inputK >> level;
        int numPlusOneBlocks = inputK & ((1 << level) - 1);

        int reversedBlock = reverseBits(block, level);

        return base + (reversedBlock < numPlusOneBlocks ? 1 : 0);
    }

    private static int reverseBits(int value, int bitCount) {
        int result = 0;
        for (int i = 0; i < bitCount; i++) {
            result = (result << 1) | (value & 1);
            value >>= 1;
        }
        return result;
    }

    public static double compute2WayAdaptiveEpsilon(
            long totalNodeWeight, int k, PartitionContext pCtx) {
        double base = (1.0 + pCtx.epsilon) * k * pCtx.totalNodeWeight.value / pCtx.k.value / totalNodeWeight;
        double exponent = 1.0 / MathUtils.ceilLog2(k);
        double epsilonPrime = Math.pow(base, exponent) - 1.0;
        return Math.max(epsilonPrime, 0.0001);
    }

    public static PartitionContext createBipartitionContext(
            Graph subgraph, BlockID k1, BlockID k2, PartitionContext kwayPCtx) {
        PartitionContext twowayPCtx = new PartitionContext();
        twowayPCtx.k = new BlockID(2);
        twowayPCtx.setup(subgraph);
        twowayPCtx.epsilon = compute2WayAdaptiveEpsilon(subgraph.totalNodeWeight().value, k1.value + k2.value, kwayPCtx);
        twowayPCtx.blockWeights.setup(twowayPCtx, k1.value + k2.value);
        return twowayPCtx;
    }
    public static PartitionContext createBipartitionContextWithQueue(
            Graph subgraph, BlockID k1, BlockID k2, PartitionContext kwayPCtx) {
        PartitionContext twowayPCtx = new PartitionContext();
        twowayPCtx.k = new BlockID(2);
        twowayPCtx.setup(subgraph);

        // Compute the epsilon value based on the weights
        twowayPCtx.epsilon = compute2WayAdaptiveEpsilon(subgraph.totalNodeWeight().value, k1.value + k2.value, kwayPCtx);

        // Access the blockConstraints queue from kwayPCtx and pass it to setup
        if (kwayPCtx.blockConstraints != null && !kwayPCtx.blockConstraints.isEmpty()) {
            twowayPCtx.blockWeights.setup(twowayPCtx, k1.value + k2.value, kwayPCtx.blockConstraints);
        } else {
            throw new IllegalStateException("Block constraints queue is empty or not initialized.");
        }

        return twowayPCtx;
    }


    // Implementation of computeMaxClusterWeight
    public static NodeWeight computeMaxClusterWeight(
            CoarseningContext cCtx, Graph graph, PartitionContext pCtx) {

        NodeID n = graph.n();
        NodeWeight totalNodeWeight = graph.totalNodeWeight();

        return computeMaxClusterWeight(cCtx, n, totalNodeWeight, pCtx);
    }

    public static NodeWeight computeMaxClusterWeight(
            CoarseningContext cCtx, NodeID n, NodeWeight totalNodeWeight, PartitionContext pCtx) {

        double maxClusterWeight = 0.0;

        switch (cCtx.clusterWeightLimit) {
            case EPSILON_BLOCK_WEIGHT:
                int divisor = Math.min(Math.max(2, n.value / cCtx.contractionLimit), pCtx.k.value);
                maxClusterWeight = (pCtx.epsilon * totalNodeWeight.value) / divisor;
                break;

            case BLOCK_WEIGHT:
                maxClusterWeight = (1.0 + pCtx.epsilon) * totalNodeWeight.value / pCtx.k.value;
                break;

            case ONE:
                maxClusterWeight = 1.0;
                break;

            case ZERO:
                maxClusterWeight = 0.0;
                break;
        }

        return new NodeWeight((long) (maxClusterWeight * cCtx.clusterWeightMultiplier));
    }
    private static int floorLog2(int value) {
        int log = 0;
        while (value > 1) {
            value >>= 1;
            log++;
        }
        return log;
    }
}

