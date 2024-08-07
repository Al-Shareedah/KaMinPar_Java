package org.alshar;

import org.alshar.common.StaticArray;
import org.alshar.kaminpar_shm.kaminpar.*;
import org.alshar.common.DegreeBuckets;
import java.util.*;
import java.util.stream.IntStream;

public class Graph {
    // Fields
    private StaticArray<EdgeID> nodes;
    private StaticArray<NodeID> edges;
    private StaticArray<NodeWeight> nodeWeights;
    private StaticArray<EdgeWeight> edgeWeights;
    private NodeWeight totalNodeWeight;
    private EdgeWeight totalEdgeWeight;
    private NodeWeight maxNodeWeight;
    private StaticArray<NodeID> permutation;
    private boolean sorted;
    private List<NodeID> buckets;
    private int numberOfBuckets;

    // Constructors
    public Graph() {
        this.nodes = new StaticArray<>(0);
        this.edges = new StaticArray<>(0);
        this.nodeWeights = new StaticArray<>(0);
        this.edgeWeights = new StaticArray<>(0);
        this.sorted = false;
        this.buckets = new ArrayList<>(Collections.nCopies(DegreeBuckets.kNumberOfDegreeBuckets, new NodeID(0)));
        this.numberOfBuckets = 0;
    }

    public Graph(StaticArray<EdgeID> nodes, StaticArray<NodeID> edges, StaticArray<NodeWeight> nodeWeights,
                 StaticArray<EdgeWeight> edgeWeights, boolean sorted) {
        this.nodes = nodes;
        this.edges = edges;
        this.nodeWeights = nodeWeights;
        this.edgeWeights = edgeWeights;
        this.sorted = sorted;

        if (nodeWeights.isEmpty()) {
            this.totalNodeWeight = new NodeWeight(nodes.size() - 1);
            this.maxNodeWeight = new NodeWeight(1);
        } else {
            this.totalNodeWeight = new NodeWeight(Arrays.stream(nodeWeights.getArray())
                    .mapToLong(nw -> ((NodeWeight) nw).value)
                    .sum());
            this.maxNodeWeight = new NodeWeight(Arrays.stream(nodeWeights.getArray())
                    .mapToLong(nw -> ((NodeWeight) nw).value)
                    .max().orElse(1));
        }

        if (edgeWeights.isEmpty()) {
            this.totalEdgeWeight = new EdgeWeight(edges.size());
        } else {
            this.totalEdgeWeight = new EdgeWeight(Arrays.stream(edgeWeights.getArray())
                    .mapToLong(ew -> ((EdgeWeight) ew).value)
                    .sum());
        }

        this.buckets = new ArrayList<>(Collections.nCopies(DegreeBuckets.kNumberOfDegreeBuckets, new NodeID(0)));
        initDegreeBuckets();
    }

    public StaticArray<EdgeID> rawNodes() {
        return nodes;
    }

    public StaticArray<NodeID> rawEdges() {
        return edges;
    }

    public StaticArray<NodeWeight> rawNodeWeights() {
        return nodeWeights;
    }

    public StaticArray<EdgeWeight> rawEdgeWeights() {
        return edgeWeights;
    }

    public StaticArray<EdgeID> takeRawNodes() {
        StaticArray<EdgeID> temp = nodes;
        nodes = new StaticArray<>(0);
        return temp;
    }

    public StaticArray<NodeID> takeRawEdges() {
        StaticArray<NodeID> temp = edges;
        edges = new StaticArray<>(0);
        return temp;
    }

    public StaticArray<NodeWeight> takeRawNodeWeights() {
        StaticArray<NodeWeight> temp = nodeWeights;
        nodeWeights = new StaticArray<>(0);
        return temp;
    }

    public StaticArray<EdgeWeight> takeRawEdgeWeights() {
        StaticArray<EdgeWeight> temp = edgeWeights;
        edgeWeights = new StaticArray<>(0);
        return temp;
    }

    public boolean nodeWeighted() {
        return nodes.size() - 1 != totalNodeWeight.value;
    }

    public NodeWeight totalNodeWeight() {
        return totalNodeWeight;
    }

    public NodeWeight maxNodeWeight() {
        return maxNodeWeight;
    }

    public NodeWeight nodeWeight(NodeID u) {
        return nodeWeighted() ? nodeWeights.get((int) u.value) : new NodeWeight(1);
    }

    public boolean edgeWeighted() {
        return edges.size() != totalEdgeWeight.value;
    }

    public EdgeWeight totalEdgeWeight() {
        return totalEdgeWeight;
    }

    public EdgeWeight edgeWeight(EdgeID e) {
        return edgeWeighted() ? edgeWeights.get((int) e.value) : new EdgeWeight(1);
    }

    public NodeID n() {
        return new NodeID(nodes.size() - 1);
    }

    public EdgeID m() {
        return new EdgeID(edges.size());
    }

    public NodeID edgeTarget(EdgeID e) {
        return edges.get((int) e.value);
    }

    public NodeID degree(NodeID u) {
        return new NodeID(nodes.get((int) u.value + 1).value - nodes.get((int) u.value).value);
    }

    public EdgeID firstEdge(NodeID u) {
        return nodes.get((int) u.value);
    }

    public EdgeID firstInvalidEdge(NodeID u) {
        return nodes.get((int) u.value + 1);
    }

    public void pforNodes(java.util.function.IntConsumer l) {
        IntStream.range(0, (int) n().value).parallel().forEach(l);
    }

    public void pforEdges(java.util.function.IntConsumer l) {
        IntStream.range(0, (int) m().value).parallel().forEach(l);
    }

    public List<NodeID> nodes() {
        List<NodeID> nodeList = new ArrayList<>();
        for (int i = 0; i < n().value; i++) {
            nodeList.add(new NodeID(i));
        }
        return nodeList;
    }

    public List<EdgeID> edges() {
        List<EdgeID> edgeList = new ArrayList<>();
        for (int i = 0; i < m().value; i++) {
            edgeList.add(new EdgeID(i));
        }
        return edgeList;
    }

    public List<EdgeID> incidentEdges(NodeID u) {
        List<EdgeID> edgeList = new ArrayList<>();
        for (int i = (int) nodes.get((int) u.value).value; i < (int) nodes.get((int) u.value + 1).value; i++) {
            edgeList.add(new EdgeID(i));
        }
        return edgeList;
    }

    public List<NodeID> adjacentNodes(NodeID u) {
        List<NodeID> nodeList = new ArrayList<>();
        for (int i = (int) nodes.get((int) u.value).value; i < (int) nodes.get((int) u.value + 1).value; i++) {
            nodeList.add(edges.get(i));
        }
        return nodeList;
    }

    public void setPermutation(StaticArray<NodeID> permutation) {
        this.permutation = permutation;
    }

    public boolean permuted() {
        return permutation != null && permutation.size() > 0;
    }

    public NodeID mapOriginalNode(NodeID u) {
        return permutation.get((int) u.value);
    }

    public StaticArray<NodeID> takeRawPermutation() {
        StaticArray<NodeID> temp = permutation;
        permutation = new StaticArray<>(0);
        return temp;
    }

    public NodeID bucketSize(int bucket) {
        return new NodeID(buckets.get(bucket + 1).value - buckets.get(bucket).value);
    }

    public NodeID firstNodeInBucket(int bucket) {
        return buckets.get(bucket);
    }

    public NodeID firstInvalidNodeInBucket(int bucket) {
        return firstNodeInBucket(bucket + 1);
    }

    public int numberOfBuckets() {
        return numberOfBuckets;
    }

    public boolean sorted() {
        return sorted;
    }

    private void initDegreeBuckets() {
        if (sorted) {
            for (NodeID u : nodes()) {
                int bucket = DegreeBuckets.degreeBucket(degree(u).value);
                buckets.set(bucket + 1, new NodeID(buckets.get(bucket + 1).value + 1));
            }
            numberOfBuckets = (int) IntStream.range(0, buckets.size())
                    .filter(i -> buckets.get(i).value > 0)
                    .count();
        } else {
            buckets.set(1, n());
            numberOfBuckets = 1;
        }
        IntStream.range(0, buckets.size() - 1).forEach(i -> buckets.set(i + 1, new NodeID(buckets.get(i + 1).value + buckets.get(i).value)));
    }


    public void updateTotalNodeWeight() {
        if (nodeWeights.isEmpty()) {
            totalNodeWeight = new NodeWeight(nodes.size() - 1);
            maxNodeWeight = new NodeWeight(1);
        } else {
            totalNodeWeight = new NodeWeight(Arrays.stream(nodeWeights.getArray())
                    .mapToLong(nw -> ((NodeWeight) nw).value)
                    .sum());
            maxNodeWeight = new NodeWeight(Arrays.stream(nodeWeights.getArray())
                    .mapToLong(nw -> ((NodeWeight) nw).value)
                    .max().orElse(1));
        }
    }

    public static class Debug {
        public static boolean validateGraph(Graph graph, boolean undirected, NodeID numPseudoNodes) {
            for (int u = 0; u < graph.n().value; u++) {
                if (graph.rawNodes().get(u).value > graph.rawNodes().get(u + 1).value) {
                    System.out.println("Bad node array at position " + u);
                    return false;
                }
            }

            for (NodeID u : graph.nodes()) {
                for (NodeID v : graph.adjacentNodes(u)) {
                    if (v.value >= graph.n().value) {
                        System.out.println("Neighbor " + v.value + " of " + u.value + " is out-of-graph");
                        return false;
                    }
                    if (u.value == v.value) {
                        System.out.println("Self-loop at " + u.value);
                        return false;
                    }

                    boolean foundReverse = false;
                    for (NodeID uPrime : graph.adjacentNodes(v)) {
                        if (uPrime.value >= graph.n().value) {
                            System.out.println("Neighbor " + uPrime.value + " of neighbor " + v.value + " of " + u.value + " is out-of-graph");
                            return false;
                        }
                        if (u.value == uPrime.value) {
                            if (!graph.edgeWeight(graph.firstEdge(u)).equals(graph.edgeWeight(graph.firstEdge(uPrime)))) {
                                System.out.println("Weight of edge " + u + " differs from the weight of its reverse edge " + uPrime);
                                return false;
                            }
                            foundReverse = true;
                            break;
                        }
                    }
                    if (undirected && v.value < graph.n().value - numPseudoNodes.value && !foundReverse) {
                        System.out.println("Edge " + u.value + " --> " + v.value + " exists, but the reverse edge does not exist");
                        return false;
                    }
                }
            }
            return true;
        }

        public static void printGraph(Graph graph) {
            for (NodeID u : graph.nodes()) {
                System.out.print("L" + u.value + " NW" + graph.nodeWeight(u).value + " | ");
                for (NodeID v : graph.adjacentNodes(u)) {
                    System.out.print("L" + v.value + " NW" + graph.nodeWeight(v).value + "  ");
                }
                System.out.println();
            }
        }

        public static Graph sortNeighbors(Graph graph) {
            StaticArray<EdgeID> nodes = graph.takeRawNodes();
            StaticArray<NodeID> edges = graph.takeRawEdges();
            StaticArray<NodeWeight> nodeWeights = graph.takeRawNodeWeights();
            StaticArray<EdgeWeight> edgeWeights = graph.takeRawEdgeWeights();

            boolean edgeWeighted = graph.edgeWeighted();
            if (edgeWeighted) {
                List<Pair<NodeID, EdgeWeight>> zipped = new ArrayList<>();
                for (int i = 0; i < edges.size(); i++) {
                    zipped.add(new Pair<>(edges.get(i), edgeWeights.get(i)));
                }
                zipped.parallelStream().forEachOrdered(z -> {
                    int start = (int) nodes.get((int) z.getKey().value).value;
                    int end = (int) nodes.get((int) z.getKey().value + 1).value;
                    zipped.subList(start, end).sort(Comparator.comparing(p -> p.getKey().value));
                });
                for (int i = 0; i < edges.size(); i++) {
                    edges.set(i, zipped.get(i).getKey());
                    edgeWeights.set(i, zipped.get(i).getValue());
                }
            } else {
                IntStream.range(0, nodes.size() - 1).parallel().forEach(u -> {
                    int start = (int) nodes.get(u).value;
                    int end = (int) nodes.get(u + 1).value;
                    Arrays.sort(edges.getArray(), start, end);
                });
            }

            Graph sortedGraph = new Graph(nodes, edges, nodeWeights, edgeWeights, graph.sorted());
            sortedGraph.setPermutation(graph.takeRawPermutation());
            return sortedGraph;
        }

        static class Pair<K, V> {
            private K key;
            private V value;

            public Pair(K key, V value) {
                this.key = key;
                this.value = value;
            }

            public K getKey() {
                return key;
            }

            public V getValue() {
                return value;
            }
        }
    }
}
