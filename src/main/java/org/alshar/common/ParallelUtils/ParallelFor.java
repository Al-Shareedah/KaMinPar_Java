package org.alshar.common.ParallelUtils;

import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class ParallelFor {

    public static void parallelFor(int first, int last, int step, BiConsumer<Integer, Integer> function) {
        if (step <= 0) {
            throw new IllegalArgumentException("Step must be positive");
        }
        if (first < last) {
            ForkJoinPool forkJoinPool = new ForkJoinPool();

            try {
                forkJoinPool.invoke(new ParallelForTask(first, last, step, function));
            } finally {
                forkJoinPool.shutdown();
                try {
                    if (!forkJoinPool.awaitTermination(60, TimeUnit.SECONDS)) {
                        forkJoinPool.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    forkJoinPool.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static class ParallelForTask extends RecursiveAction {
        private final int first;
        private final int last;
        private final int step;
        private final BiConsumer<Integer, Integer> function;
        private static final int THRESHOLD = 1000; // threshold for splitting tasks

        public ParallelForTask(int first, int last, int step, BiConsumer<Integer, Integer> function) {
            this.first = first;
            this.last = last;
            this.step = step;
            this.function = function;
        }

        @Override
        protected void compute() {
            if (last - first <= THRESHOLD) {
                function.accept(first, last);
            } else {
                int mid = (first + last) >>> 1;
                ParallelForTask leftTask = new ParallelForTask(first, mid, step, function);
                ParallelForTask rightTask = new ParallelForTask(mid, last, step, function);
                invokeAll(leftTask, rightTask);
            }
        }
    }

    public static void main(String[] args) {
        parallelFor(0, 10000, 1, (start, end) -> {
            // Your operation here, for example:
            for (int i = start; i < end; i++) {
                System.out.println("Processing index " + i);
            }
        });
    }
}
