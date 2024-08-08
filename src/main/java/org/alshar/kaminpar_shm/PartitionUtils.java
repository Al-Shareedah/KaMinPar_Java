package org.alshar.kaminpar_shm;

import org.alshar.Context;
import org.alshar.kaminpar_shm.kaminpar.BlockID;

import java.util.Arrays;

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
            long totalNodeWeight, int k, Context.PartitionContext pCtx) {
        double base = (1.0 + pCtx.epsilon) * k * pCtx.totalNodeWeight.value / pCtx.k / totalNodeWeight;
        double exponent = 1.0 / Math.ceil(Math.log(k) / Math.log(2));
        double epsilonPrime = Math.pow(base, exponent) - 1.0;
        return Math.max(epsilonPrime, 0.0001);
    }
}

