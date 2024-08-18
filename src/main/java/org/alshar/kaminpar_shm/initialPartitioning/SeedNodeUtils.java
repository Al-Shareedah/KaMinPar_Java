package org.alshar.kaminpar_shm.initialPartitioning;

import org.alshar.Graph;
import org.alshar.common.datastructures.NodeID;
import org.alshar.common.datastructures.Pair;
import org.alshar.kaminpar_shm.refinement.Marker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.alshar.common.datastructures.Queue;
import java.util.concurrent.ThreadLocalRandom;
public class SeedNodeUtils {

    public static Pair<NodeID, NodeID> findFarAwayNodes(Graph graph, int numIterations) {
        Queue<NodeID> queue = new Queue<>(graph.n().getValue());
        Marker marker = new Marker(graph.n().getValue(), 1); // Assuming one marker for simplicity

        NodeID bestDistance = new NodeID(0);
        Pair<NodeID, NodeID> bestPair = new Pair<>(new NodeID(0), new NodeID(0));

        for (int i = 0; i < numIterations; ++i) {
            NodeID u = new NodeID(ThreadLocalRandom.current().nextInt(0, graph.n().getValue()));
            Pair<NodeID, NodeID> result = findFurthestAwayNode(graph, u, queue, marker);

            if (result.getValue().getValue() > bestDistance.getValue() ||
                    (result.getValue().getValue() == bestDistance.getValue() && ThreadLocalRandom.current().nextBoolean())) {
                bestDistance = result.getValue();
                bestPair = new Pair<>(u, result.getKey());
            }
        }

        return bestPair;
    }

    public static Pair<NodeID, NodeID> findFurthestAwayNode(Graph graph, NodeID startNode, Queue<NodeID> queue, Marker marker) {
        queue.pushTail(startNode);
        marker.set(startNode.getValue(), 0, true);

        NodeID currentDistance = new NodeID(0);
        NodeID lastNode = startNode;
        int remainingNodesInLevel = 1;
        int nodesInNextLevel = 0;

        while (!queue.empty()) {
            NodeID u = queue.head();
            queue.popHead();
            lastNode = u;

            for (NodeID v : graph.adjacentNodes(u)) {
                if (marker.get(v)) {
                    continue;
                }
                queue.pushTail(v);
                marker.set(v.getValue(), 0, true);
                nodesInNextLevel++;
            }

            // Keep track of distance from startNode
            if (remainingNodesInLevel > 0) {
                remainingNodesInLevel--;
                if (remainingNodesInLevel == 0) {
                    currentDistance = new NodeID(currentDistance.getValue() + 1);
                    remainingNodesInLevel = nodesInNextLevel;
                    nodesInNextLevel = 0;
                }
            }
        }

        // BFS did not scan the whole graph, i.e., we have disconnected components
        if (marker.firstUnmarkedElement(0) < graph.n().getValue()) {
            lastNode = new NodeID(marker.firstUnmarkedElement(0));
            currentDistance = new NodeID(Integer.MAX_VALUE); // infinity
        }

        marker.reset();
        queue.clear();
        return new Pair<>(lastNode, currentDistance);
    }







}
