package org.alshar.common.GraphUtils;

import java.util.ArrayList;
import java.util.List;

public class NavigableLinkedList<Key, Element> {
    private static final int CHUNK_SIZE = 1 << 15; // 32,768 elements per chunk
    private List<List<Element>> chunks = new ArrayList<>();
    private List<Element> currentChunk = new ArrayList<>(CHUNK_SIZE);
    private List<NavigationMarker<Key, Element>> markers = new ArrayList<>();

    public void mark(Key key) {
        markers.add(new NavigationMarker<>(key, position(), this));
    }

    public void addElement(Element element) {
        flushIfFull();
        currentChunk.add(element);
    }

    public Element get(int position) {
        int chunkIndex = position / CHUNK_SIZE;
        int elementIndex = position % CHUNK_SIZE;
        return chunks.get(chunkIndex).get(elementIndex);
    }

    public List<NavigationMarker<Key, Element>> getMarkers() {
        return markers;
    }

    public int position() {
        return chunks.size() * CHUNK_SIZE + currentChunk.size();
    }

    public void flush() {
        if (!currentChunk.isEmpty()) {
            chunks.add(new ArrayList<>(currentChunk));
            currentChunk.clear();
        }
    }

    private void flushIfFull() {
        if (currentChunk.size() == CHUNK_SIZE) {
            flush();
        }
    }
}
