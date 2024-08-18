package org.alshar.common.GraphUtils;
import org.alshar.Context;
import org.alshar.Graph;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.StaticArray;
import org.alshar.common.datastructures.BlockID;
import org.alshar.common.context.PartitionContext;
import org.alshar.kaminpar_shm.PartitionedGraph;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
public class Permutator {

    public static class NodePermutations<T> {
        public StaticArray<T> oldToNew;
        public StaticArray<T> newToOld;

        public NodePermutations(StaticArray<T> oldToNew, StaticArray<T> newToOld) {
            this.oldToNew = oldToNew;
            this.newToOld = newToOld;
        }
    }

    public static NodePermutations<Integer> sortByDegreeBuckets(StaticArray<Integer> nodes) {
        int n = nodes.size() - 1;
        int cpus = Math.min(Runtime.getRuntime().availableProcessors(), n);

        StaticArray<Integer> permutation = new StaticArray<>(n);
        StaticArray<Integer> inversePermutation = new StaticArray<>(n);

        int numBuckets = getNumberOfDegreeBuckets();
        int[][] localBuckets = new int[cpus][numBuckets + 1];

        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                ForkJoinPool.commonPool().submit(() -> {
                    for (int cpu = 0; cpu < cpus; cpu++) {
                        int start = cpu * (n / cpus);
                        int end = (cpu == cpus - 1) ? n : start + (n / cpus);
                        for (int u = start; u < end; u++) {
                            int bucket = findBucket(nodes.get(u + 1) - nodes.get(u));
                            permutation.set(u, localBuckets[cpu][bucket]++);
                        }
                    }
                }).join();
            }
        });

        int[] globalBuckets = new int[numBuckets + 1];
        for (int i = 1; i < cpus; ++i) {
            for (int j = 0; j < numBuckets; ++j) {
                globalBuckets[j + 1] += localBuckets[i][j];
            }
        }
        StaticArray<Integer> globalBucketsArray = new StaticArray<>(globalBuckets.length);
        for (int i = 0; i < globalBuckets.length; i++) {
            globalBucketsArray.set(i, globalBuckets[i]);
        }

        prefixSum(globalBucketsArray);

        for (int i = 0; i < numBuckets; ++i) {
            for (int j = 0; j < cpus - 1; ++j) {
                localBuckets[j + 1][i] += localBuckets[j][i];
            }
        }

        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                ForkJoinPool.commonPool().submit(() -> {
                    for (int cpu = 0; cpu < cpus; cpu++) {
                        int start = cpu * (n / cpus);
                        int end = (cpu == cpus - 1) ? n : start + (n / cpus);
                        for (int u = start; u < end; u++) {
                            int bucket = findBucket(nodes.get(u + 1) - nodes.get(u));
                            permutation.set(u, permutation.get(u) + globalBuckets[bucket] + localBuckets[cpu][bucket]);
                        }
                    }
                }).join();
            }
        });

        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                ForkJoinPool.commonPool().submit(() -> {
                    for (int u = 1; u < nodes.size(); ++u) {
                        inversePermutation.set(permutation.get(u - 1), u - 1);
                    }
                }).join();
            }
        });

        return new NodePermutations<>(permutation, inversePermutation);
    }





    private static NodePermutations<StaticArray<Integer>> rearrangeGraph(PartitionContext pCtx, StaticArray<Integer> nodes, StaticArray<Integer> edges, StaticArray<Integer> nodeWeights, StaticArray<Integer> edgeWeights) {
        // Implement the logic to rearrange the graph.
        return null;
    }

    private static int getNumberOfDegreeBuckets() {
        // Implement logic to return number of degree buckets.
        return 0;
    }

    private static void prefixSum(StaticArray<Integer> array) {
        for (int i = 1; i < array.size(); ++i) {
            array.set(i, array.get(i) + array.get(i - 1));
        }
    }

    private static int findBucket(int degree) {
        // Implement the logic to find the bucket for the given degree.
        return 0;
    }
    public static NodeID integrateIsolatedNodes(Graph graph, double epsilon, Context ctx) {
        int numNonIsolatedNodes = graph.n().value; // This becomes the first isolated node
        graph.rawNodes().unrestrict();
        graph.rawNodeWeights().unrestrict();
        graph.updateTotalNodeWeight();
        int numIsolatedNodes = graph.n().value - numNonIsolatedNodes;

        // Note: max block weights should not change
        ctx.partition.epsilon = epsilon;
        ctx.setup(graph);

        return new NodeID(numIsolatedNodes);  // Return as NodeID
    }
    public static PartitionedGraph assignIsolatedNodes(
            PartitionedGraph pGraph, NodeID numIsolatedNodes, PartitionContext pCtx) {
        Graph graph = pGraph.getGraph();
        int numNonIsolatedNodes = graph.n().value - numIsolatedNodes.value;

        StaticArray<BlockID> partition = new StaticArray<>(graph.n().value);
        // Copy partition of non-isolated nodes
        ForkJoinPool.commonPool().invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                ForkJoinPool.commonPool().submit(() -> {
                    for (int u = 0; u < numNonIsolatedNodes; u++) {
                        partition.set(u, pGraph.block(new NodeID(u)));
                    }
                }).join();
            }
        });

        // Now append the isolated ones
        int k = pGraph.k().value;
        StaticArray<Integer> blockWeights = pGraph.takeRawBlockWeights();
        int b = 0;

        for (int u = numNonIsolatedNodes; u < numNonIsolatedNodes + numIsolatedNodes.value; ++u) {
            while (b + 1 < k && blockWeights.get(b) + graph.nodeWeight(new NodeID(u)).value > pCtx.blockWeights.max(b).value) {

                ++b;
            }
            partition.set(u, new BlockID(b));
            blockWeights.set(b, (int) (blockWeights.get(b) + graph.nodeWeight(new NodeID(u)).value));
        }

        return new PartitionedGraph(graph, new BlockID(k), partition);
    }


}