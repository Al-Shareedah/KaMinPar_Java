package org.alshar.common.Math;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
public class Random_shm {
    private static final int kPrecomputedBools = 1024;
    private static final Lock createMutex = new ReentrantLock();
    private static final List<Random_shm> instances = Collections.synchronizedList(new ArrayList<>());
    private static int seed = 0;

    private final java.util.Random generator;
    private final int[] randomBools = new int[kPrecomputedBools];
    private int nextRandomBool;

    public Random_shm() {
        this.generator = new java.util.Random(seed + Thread.currentThread().getId());
        this.nextRandomBool = 0;
        precomputeBools();
    }


    public static Random_shm getInstance() {
        return createInstance();
    }

    private static Random_shm createInstance() {
        createMutex.lock();
        try {
            Random_shm instance = new Random_shm();
            instances.add(instance);
            return instance;
        } finally {
            createMutex.unlock();
        }
    }

    public static void reseed(int newSeed) {
        seed = newSeed;
        instances.forEach(instance -> instance.reinit(newSeed));
    }

    public static int getSeed() {
        return seed;
    }

    private void reinit(int newSeed) {
        this.generator.setSeed(newSeed + Thread.currentThread().getId());
        this.nextRandomBool = 0;
        precomputeBools();
    }

    private void precomputeBools() {
        for (int i = 0; i < kPrecomputedBools; i++) {
            randomBools[i] = generator.nextBoolean() ? 1 : 0;
        }
    }

    public int randomIndex(int inclusiveLowerBound, int exclusiveUpperBound) {
        return inclusiveLowerBound + generator.nextInt(exclusiveUpperBound - inclusiveLowerBound);
    }

    public boolean randomBool() {
        return randomBools[nextRandomBool++ % kPrecomputedBools] == 1;
    }

    public boolean randomBool(double prob) {
        return generator.nextDouble() <= prob;
    }

    public <T> void shuffle(List<T> list) {
        Collections.shuffle(list, generator);
    }

    public static class RandomPermutations<ValueType> {
        private final List<List<ValueType>> permutations = new ArrayList<>();
        private final Random_shm rand;

        public RandomPermutations(Random_shm rand, List<ValueType> initialElements, int count) {
            this.rand = rand;
            initPermutations(initialElements, count);
        }
        public RandomPermutations(Random_shm rand, int size, int count) {
            this.rand = rand;
            initPermutations(size, count);
        }


        private void initPermutations(List<ValueType> initialElements, int count) {
            for (int i = 0; i < count; i++) {
                List<ValueType> permutation = new ArrayList<>(initialElements);
                rand.shuffle(permutation);
                permutations.add(permutation);
            }
        }
        private void initPermutations(int size, int count) {
            for (int i = 0; i < count; i++) {
                List<ValueType> permutation = new ArrayList<>(size);
                for (int j = 0; j < size; j++) {
                    permutation.add((ValueType) Integer.valueOf(j));  // Cast to ValueType
                }
                rand.shuffle(permutation);
                permutations.add(permutation);
            }
        }

        public List<ValueType> get() {
            return permutations.get(rand.randomIndex(0, permutations.size()));
        }
    }
}
