package org.alshar.kaminpar_shm.coarsening;

public class LabelPropagationConfig {
    public static final int kMinChunkSize = 1024;

    // Nodes per permutation unit: when iterating over nodes in a chunk, we divide
    // them into permutation units, iterate over permutation orders in random
    // order, and iterate over nodes inside a permutation unit in random order.
    public static final int kPermutationSize = 64;

    // When randomizing the node order inside a permutation unit, we pick a random
    // permutation from a pool of permutations. This constant determines the pool
    // size.
    public static final int kNumberOfNodePermutations = 64;

    // If true, we count the number of empty clusters
    public static final boolean kTrackClusterCount = true;

    // If true, match singleton clusters in 2-hop distance
    public static final boolean kUseTwoHopClustering = false;

    public static final boolean kUseActualGain = false;
    public static final boolean kStatistics = false;

    public static final boolean kUseActiveSetStrategy = true;
    public static final boolean kUseLocalActiveSetStrategy = false;

    // Placeholder for the Graph, RatingMap, ClusterID, and ClusterWeight types
    // These types will depend on your Java implementation of the graph and other related classes.
    // For example, you might have:
    // public static class Graph {...}
    // public static class RatingMap {...}
    // public static class ClusterID {...}
    // public static class ClusterWeight {...}
}
