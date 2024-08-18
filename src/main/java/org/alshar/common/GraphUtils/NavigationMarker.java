package org.alshar.common.GraphUtils;

import java.util.ArrayList;
import java.util.List;

public class NavigationMarker<Key, Element> {
    private Key key;
    private int position;
    private NavigableLinkedList<Key, Element> localList;

    public NavigationMarker(Key key, int position, NavigableLinkedList<Key, Element> localList) {
        this.key = key;
        this.position = position;
        this.localList = localList;
    }

    public Key getKey() {
        return key;
    }

    public int getPosition() {
        return position;
    }

    public Element getElement() {
        return localList.get(position);
    }
    // Method to return all edges (or elements) associated with this node
    public List<Element> getEdges(int count) {
        List<Element> edges = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            edges.add(localList.get(position + i));
        }
        return edges;
    }
}

