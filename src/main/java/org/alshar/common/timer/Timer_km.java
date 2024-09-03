package org.alshar.common.timer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Timer_km {
    private static final boolean kDebug = false;

    private static final int kSpaceBetweenTimeAndRestarts = 1;
    private static final int kSpaceBetweenRestartsAndAnnotation = 1;
    private static final String kBranch = "|- ";
    private static final String kEdge = "|  ";
    private static final String kTailBranch = "`- ";
    private static final String kTailEdge = "   ";
    private static final String kNameDel = ": ";
    private static final char kPadding = '.';
    private static final String kSecondsUnit = " s";

    public static class TimerTreeNode {
        String name;
        String description = "";

        int restarts = 0;
        Duration elapsed = Duration.ZERO;
        Instant start;

        TimerTreeNode parent = null;
        Map<String, TimerTreeNode> childrenTbl = new HashMap<>();
        List<TimerTreeNode> children = new ArrayList<>();

        String annotation = "";

        public String buildDisplayNameMr() {
            StringBuilder sb = new StringBuilder();
            sb.append(name.replace(" ", "_").toLowerCase());
            if (!description.isEmpty()) {
                sb.append("[").append(description.replace(" ", "_").toLowerCase()).append("]");
            }
            return sb.toString();
        }

        public String buildDisplayNameHr() {
            StringBuilder sb = new StringBuilder();
            sb.append(name);
            if (description != null && !description.isEmpty()) {
                sb.append(" (").append(description).append(")");
            }
            return sb.toString();
        }

        public double seconds() {
            return elapsed.toMillis() / 1000.0;
        }
    }

    public static class TimerTree {
        TimerTreeNode root = new TimerTreeNode();
        TimerTreeNode current = root;
    }

    private static Timer_km globalTimer = new Timer_km("Global Timer");

    public static Timer_km global() {
        return globalTimer;
    }

    private String name;
    private String annotation = "";
    private TimerTree tree = new TimerTree();
    private Lock lock = new ReentrantLock();
    private int disabled = 0;

    private int hrTimeCol = 0;
    private int hrMaxTimeLen = 0;
    private int hrMaxRestartsLen = 0;

    public Timer_km(String name) {
        this.name = name;
        this.tree.root.name = name;
        this.tree.root.start = Instant.now();
    }

    public void startTimer(String name) {
        startTimer(name, "");
    }

    public void startTimer(String name, String description) {
        lock.lock();
        try {
            if (disabled > 0) {
                return;
            }

            TimerTreeNode currentNode = tree.current;

            if (!description.isEmpty() || !currentNode.childrenTbl.containsKey(name)) {
                TimerTreeNode child = new TimerTreeNode();
                currentNode.childrenTbl.put(name, child);
                child.parent = currentNode;
                child.name = name;
                child.description = description;
                currentNode.children.add(child);
                tree.current = child;
            } else {
                tree.current = currentNode.childrenTbl.get(name);
            }

            tree.current.restarts++;
            startTimerImpl();
        } finally {
            lock.unlock();
        }
    }

    public void stopTimer() {
        lock.lock();
        try {
            if (disabled > 0) {
                return;
            }

            stopTimerImpl();
            tree.current = tree.current.parent;
        } finally {
            lock.unlock();
        }
    }

    public ScopedTimer startScopedTimer(String name) {
        startTimer(name);
        return new ScopedTimer(this);
    }

    public ScopedTimer startScopedTimer(String name, String description) {
        startTimer(name, description);
        return new ScopedTimer(this);
    }

    public void enable() {
        lock.lock();
        try {
            disabled = Math.max(0, disabled - 1);
        } finally {
            lock.unlock();
        }
    }

    public void disable() {
        lock.lock();
        try {
            disabled++;
        } finally {
            lock.unlock();
        }
    }

    public void annotate(String annotation) {
        this.annotation = annotation;
    }

    public TimerTreeNode tree() {
        return tree.root;
    }

    public double elapsedSeconds() {
        return Duration.between(tree.root.start, Instant.now()).toMillis() / 1000.0;
    }

    public void printMachineReadable(Appendable out, int maxDepth) {
        try {
            for (TimerTreeNode node : tree.root.children) {
                printNodeMr(out, "", node, maxDepth);
            }
            out.append("\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printNodeMr(Appendable out, String prefix, TimerTreeNode node, int maxDepth) throws Exception {
        if (maxDepth < 0) {
            return;
        }

        String displayName = prefix + node.buildDisplayNameMr();
        out.append(displayName).append("=").append(String.format("%.3f", node.seconds())).append(" ");

        String childPrefix = displayName + ".";
        for (TimerTreeNode child : node.children) {
            printNodeMr(out, childPrefix, child, maxDepth - 1);
        }
    }

    public void printHumanReadable(Appendable out, int maxDepth) {
        if (maxDepth < 0) {
            return;
        }

        hrTimeCol = Math.max(name.length() + kNameDel.length(), computeTimeCol(0, tree.root));
        hrMaxTimeLen = computeTimeLen(tree.root);
        hrMaxRestartsLen = computeRestartsLen(tree.root);

        try {
            out.append(name);
            printPaddedTiming(out, name.length(), tree.root);
            if (!annotation.isEmpty()) {
                out.append(" ").append(annotation);
            }
            out.append("\n");

            printChildrenHr(out, "", tree.root, maxDepth - 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printChildrenHr(Appendable out, String basePrefix, TimerTreeNode node, int maxDepth) throws Exception {
        if (maxDepth < 0) {
            return;
        }

        String prefixMid = basePrefix + kBranch;
        String childPrefixMid = basePrefix + kEdge;
        String prefixEnd = basePrefix + kTailBranch;
        String childPrefixEnd = basePrefix + kTailEdge;

        for (TimerTreeNode child : node.children) {
            boolean isLast = (child == node.children.get(node.children.size() - 1));
            String prefix = isLast ? prefixEnd : prefixMid;
            String childPrefix = isLast ? childPrefixEnd : childPrefixMid;

            String displayName = child.buildDisplayNameHr();
            out.append(prefix).append(displayName);
            printPaddedTiming(out, prefix.length() + displayName.length(), child);

            if (child.annotation != null && !child.annotation.isEmpty()) {
                out.append(" ").append(child.annotation);
            }

            out.append("\n");
            printChildrenHr(out, childPrefix, child, maxDepth - 1);
        }
    }

    private void printPaddedTiming(Appendable out, int startCol, TimerTreeNode node) throws Exception {
        int timePaddingLen = hrTimeCol - startCol - kNameDel.length();

        // Ensure the padding length is at least 1 before formatting
        String timePadding = timePaddingLen > 1 ? String.format("%" + (timePaddingLen - 1) + "s ", kPadding) : "";

        double time = node.seconds();
        out.append(kNameDel).append(timePadding).append(String.format("%.3f", time)).append(kSecondsUnit);

        int timeLen = String.format("%.3f", time).length();
        int restartsPaddingLength = hrMaxTimeLen - timeLen + kSpaceBetweenTimeAndRestarts;
        int tailPaddingLength = Math.max(0, hrMaxRestartsLen - String.valueOf(node.restarts).length());

        out.append(String.format("%" + restartsPaddingLength + "s", ""));
        if (node.restarts > 1) {
            out.append("(").append(String.valueOf(node.restarts)).append(")");
            if (tailPaddingLength > 0) {
                out.append(String.format("%" + tailPaddingLength + "s", ""));
            }
        } else if (hrMaxRestartsLen > 0) {
            out.append(String.format("%" + (2 + hrMaxRestartsLen) + "s", ""));
        }
    }



    public boolean isEnabled() {
        return disabled == 0;
    }

    private int computeTimeCol(int parentPrefixLen, TimerTreeNode node) {
        int prefixLen = (node.parent == null) ? 0 : parentPrefixLen + kTailBranch.length();
        int col = prefixLen + node.buildDisplayNameHr().length() + kNameDel.length();
        for (TimerTreeNode child : node.children) {
            col = Math.max(col, computeTimeCol(prefixLen, child));
        }
        return col;
    }

    private int computeTimeLen(TimerTreeNode node) {
        int space = String.format("%.3f", node.seconds()).length();
        for (TimerTreeNode child : node.children) {
            space = Math.max(space, computeTimeLen(child));
        }
        return space;
    }

    private int computeRestartsLen(TimerTreeNode node) {
        int space = (node.restarts > 1) ? String.valueOf(node.restarts).length() : 0;
        for (TimerTreeNode child : node.children) {
            space = Math.max(space, computeRestartsLen(child));
        }
        return space;
    }

    private void startTimerImpl() {
        tree.current.start = Instant.now();
    }

    private void stopTimerImpl() {
        Instant end = Instant.now();
        tree.current.elapsed = tree.current.elapsed.plus(Duration.between(tree.current.start, end));
    }

    public void reset() {
        tree = new TimerTree();
        tree.current = tree.root;
        tree.root.start = Instant.now();
        disabled = 0;
    }

    public static class ScopedTimer implements AutoCloseable {
        private Timer_km timer;

        public ScopedTimer(Timer_km timer) {
            this.timer = timer;
        }

        @Override
        public void close() {
            timer.stopTimer();
        }
    }

    public static class TimedScope<T> {
        private Timer_km timer;
        private String name;
        private String description;

        public TimedScope(Timer_km timer, String name, String description) {
            this.timer = timer;
            this.name = name;
            this.description = description.isEmpty() ? "" : description;
        }

        public TimedScope(Timer_km timer, String name) {
            this(timer, name, "");
        }

        public <R> R run(java.util.function.Supplier<R> supplier) {
            ScopedTimer scopedTimer = timer.startScopedTimer(name, description);
            try {
                return supplier.get();
            } finally {
                scopedTimer.close();
            }
        }
    }
}
