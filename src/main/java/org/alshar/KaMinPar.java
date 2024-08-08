package org.alshar;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.alshar.common.StaticArray;
import org.alshar.kaminpar_shm.kaminpar;
import org.alshar.kaminpar_shm.kaminpar.*;
import static org.alshar.Presets.*;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class KaMinPar {
    public static class ApplicationContext {
        @Parameter(names = {"--dump-config"}, description = "Print the current configuration and exit.")
        boolean dumpConfig = false;

        @Parameter(names = {"-v", "--version"}, description = "Show version and exit.")
        boolean showVersion = false;

        @Parameter(names = {"-s", "--seed"}, description = "Seed for random number generation.")
        int seed = 0;

        @Parameter(names = {"-t", "--threads"}, description = "Number of threads to be used.")
        int numThreads = 1;

        @Parameter(names = {"--max-timer-depth"}, description = "Set maximum timer depth shown in result summary.")
        int maxTimerDepth = 3;

        @Parameter(names = {"-k", "--k"}, description = "Number of blocks in the partition.", required = true)
        int k = 0;

        @Parameter(names = {"-q", "--quiet"}, description = "Suppress all console output.")
        boolean quiet = false;

        @Parameter(names = {"-E", "--experiment"}, description = "Use an output format that is easier to parse.")
        boolean experiment = false;

        @Parameter(names = {"--validate"}, description = "Validate input parameters before partitioning.")
        boolean validate = false;

        @Parameter(names = {"-G", "--graph"}, description = "Input graph in METIS format.", required = true)
        String graphFilename = "";

        @Parameter(names = {"-o", "--output"}, description = "Output filename for the graph partition.")
        String partitionFilename = "";
    }
    public static void main(String[] args) {
        ApplicationContext app = new ApplicationContext();
        Context ctx = Presets.createDefaultContext();
        JCommander commander = JCommander.newBuilder().addObject(app).build();

        setupContext(commander, app, ctx);

        try {
            commander.parse(args);
        } catch (ParameterException ex) {
            System.err.println(ex.getMessage());
            commander.usage();
            System.exit(1);
        }

        if (app.dumpConfig) {
            commander.usage();
            System.exit(0);
        }

        if (app.showVersion) {
            System.out.println("KaMinPar version 1.0");
            System.exit(0);
        }

        // Allocate graph data structures and read graph file
        StaticArray<EdgeID> xadj = new StaticArray<>(0); // Initialize with size 0
        StaticArray<NodeID> adjncy = new StaticArray<>(0);
        StaticArray<NodeWeight> vwgt = new StaticArray<>(0);
        StaticArray<EdgeWeight> adjwgt = new StaticArray<>(0);

        try {
            if (app.validate) {
                ShmIO.Metis.read(app.graphFilename, xadj, adjncy, vwgt, adjwgt, true);
                GraphValidator.validateUndirectedGraph(xadj, adjncy, vwgt, adjwgt);
            } else {
                ShmIO.Metis.read(app.graphFilename, xadj, adjncy, vwgt, adjwgt, false);
            }
        } catch (IOException e) {
            System.err.println("Error reading graph file: " + e.getMessage());
            System.exit(1);
        }

        int n = xadj.size() - 1;
        BlockID[] partition = new BlockID[n];
        for (int i = 0; i < n; i++) {
            partition[i] = new BlockID(0); // Initialize with default values
        }

        // Compute graph partition
        kaminpar.KaMinPar partitioner = new kaminpar.KaMinPar(app.numThreads, ctx);
        partitioner.reseed(app.seed);

        if (app.quiet) {
            partitioner.setOutputLevel(OutputLevel.QUIET);
        } else if (app.experiment) {
            partitioner.setOutputLevel(OutputLevel.EXPERIMENT);
        }

        partitioner.setMaxTimerDepth(app.maxTimerDepth);
        partitioner.takeGraph(n, xadj, adjncy, vwgt, adjwgt);

        partitioner.computePartition(new BlockID(app.k), partition);

        // Save graph partition
        if (!app.partitionFilename.isEmpty()) {
            try {
                GraphIO.writePartition(app.partitionFilename, partition);
            } catch (IOException e) {
                System.err.println("Error writing partition file: " + e.getMessage());
            }
        }
    }
    public static void setupContext(JCommander commander, ApplicationContext app, Context ctx) {
        commander.addCommand("config", new Object() {
            @Parameter(names = {"-C", "--config"}, description = "Read parameters from a TOML configuration file.")
            String config = "";
        });

        commander.addCommand("preset", new Object() {
            @Parameter(names = {"-P", "--preset"}, description = "Use configuration preset.")
            String preset;

            public void setPreset(String preset) {
            }
        });

        KaminparArguments.createAllOptions(commander, ctx);
    }
}