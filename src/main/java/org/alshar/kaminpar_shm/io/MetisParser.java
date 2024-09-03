package org.alshar.kaminpar_shm.io;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class MetisParser {

    public static MetisFormat parseHeader(MappedFileToker toker) {
        toker.skipSpaces();
        while (toker.current() == '%') {
            toker.skipLine();
            toker.skipSpaces();
        }

        long numberOfNodes = toker.scanUInt();
        long numberOfEdges = toker.scanUInt();
        long format = (toker.current() != '\n') ? toker.scanUInt() : 0;
        toker.consumeChar('\n');

        boolean hasNodeWeights = (format % 100) / 10 == 1;
        boolean hasEdgeWeights = format % 10 == 1;

        return new MetisFormat(numberOfNodes, numberOfEdges, hasNodeWeights, hasEdgeWeights);
    }

    public static void parse(
            String filename,
            Consumer<MetisFormat> formatCallback,
            Consumer<Long> nodeCallback,
            BiConsumer<Long, Long> edgeCallback
    ) throws IOException {
        MappedFileToker toker = new MappedFileToker(filename);
        MetisFormat format = parseHeader(toker);

        formatCallback.accept(format);

        for (long u = 0; u < format.numberOfNodes; ++u) {
            toker.skipSpaces();
            while (toker.current() == '%') {
                toker.skipLine();
                toker.skipSpaces();
            }

            long nodeWeight = format.hasNodeWeights ? toker.scanUInt() : 1;
            nodeCallback.accept(nodeWeight);

            while (toker.validPosition() && Character.isDigit(toker.current())) {
                long v = toker.scanUInt() - 1;
                long edgeWeight = format.hasEdgeWeights ? toker.scanUInt() : 1;
                edgeCallback.accept(edgeWeight, v);
            }

            if (toker.validPosition()) {
                toker.consumeChar('\n');
            }
        }
    }
}

