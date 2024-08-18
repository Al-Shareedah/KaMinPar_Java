package org.alshar.common.GraphUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.atomic.AtomicInteger;

public class TsNavigableList {

    public static <Key, Element> List<NavigationMarker<Key, Element>> combine(
            List<NavigableLinkedList<Key, Element>> lists) {

        AtomicInteger globalPos = new AtomicInteger(0);
        int totalMarkers = lists.stream().mapToInt(list -> list.getMarkers().size()).sum();
        List<NavigationMarker<Key, Element>> globalMarkers = new ArrayList<>(totalMarkers);

        // Flush all the local lists sequentially
        for (NavigableLinkedList<Key, Element> list : lists) {
            list.flush();
        }

        // Collect markers from all lists sequentially
        for (NavigableLinkedList<Key, Element> list : lists) {
            int localPos = globalPos.getAndAdd(list.getMarkers().size());
            for (int i = 0; i < list.getMarkers().size(); i++) {
                globalMarkers.add(localPos + i, list.getMarkers().get(i));
            }
        }

        return globalMarkers;
    }


}

