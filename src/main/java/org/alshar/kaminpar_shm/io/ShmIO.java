package org.alshar.kaminpar_shm.io;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.alshar.common.datastructures.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ShmIO {
    public static void writePartition(String filename, List<Integer> partition) throws IOException {
        try (FileWriter writer = new FileWriter(filename)) {
            for (int block : partition) {
                writer.write(block + "\n");
            }
        }
    }

    public static List<Integer> readPartition(String filename) throws IOException {
        MappedFileToker toker = new MappedFileToker(filename);
        List<Integer> partition = new ArrayList<>();

        while (toker.validPosition()) {
            partition.add((int) toker.scanUInt());
            toker.consumeChar('\n');
        }

        return partition;
    }
}


