package org.alshar;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.alshar.common.context.ContextWrapper;
import org.alshar.common.datastructures.*;
import org.alshar.common.enums.*;
import org.alshar.kaminpar_shm.io.MetisReader;
import org.alshar.kaminpar_shm.io.ShmIO;
import org.alshar.kaminpar_shm.kaminpar;

import static org.alshar.Presets.createDefaultContext;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class KaMinPar {
    public static class ApplicationContext {
        @Parameter(names = {"--dump-config"}, description = "Print the current configuration and exit.")
        boolean dumpConfig = false;

        @Parameter(names = {"-v", "--version"}, description = "Show version and exit.")
        boolean showVersion = false;

        @Parameter(names = {"-s", "--seed"}, description = "Seed for random number generation.")
        int seed = 0;  // Default value is set here

        @Parameter(names = {"-t", "--threads"}, description = "Number of threads to be used.")
        int numThreads = 1;

        @Parameter(names = {"--max-timer-depth"}, description = "Set maximum timer depth shown in result summary.")
        int maxTimerDepth = 4;

        @Parameter(names = {"-k", "--k"}, description = "Number of blocks in the partition.", required = true)
        int k = 4;

        @Parameter(names = {"-q", "--quiet"}, description = "Suppress all console output.")
        boolean quiet = false;

        @Parameter(names = {"-E", "--experiment"}, description = "Use an output format that is easier to parse.")
        boolean experiment = false;

        @Parameter(names = {"--validate"}, description = "Validate input parameters before partitioning.")
        boolean validate = false;

        @Parameter(names = {"-G", "--graph"}, description = "Input graph in METIS format.", required = true)
        String graphFilename = "4elt.graph";

        @Parameter(names = {"-o", "--output"}, description = "Output filename for the graph partition.")
        String partitionFilename = "";
    }

    public static class PresetCommand {
        @Parameter(names = {"-P", "--preset"}, description = "Use configuration preset.", required = true)
        String preset;

        @Parameter(names = {"--rearrange-by"}, description = "Criteria by which the graph is sorted and rearranged.")
        String rearrangeBy = "natural";

        @Parameter(names = {"--c-contraction-limit"}, description = "Upper limit for the number of nodes per block in the coarsest graph.")
        int contractionLimit = 113;

        private final ContextWrapper contextWrapper;

        public PresetCommand(ContextWrapper contextWrapper) {
            this.contextWrapper = contextWrapper;
        }

        public void setPreset(String preset) {
            contextWrapper.ctx = Presets.createContextByPresetName(preset);
        }
    }

    public static void main(String[] args) {
        // Main application arguments
        String[] defaultArgs = {
                "-G", "email.graph",
                "-k", "4",
                "-t", "1",
                "preset", "-P", "default"
        };

        args = defaultArgs;
        ApplicationContext app = new ApplicationContext();
        Context ctx = createDefaultContext();
        ctx.debug.graphName = app.graphFilename;
        ContextWrapper contextWrapper = new ContextWrapper(ctx);
        PresetCommand presetCmd = new PresetCommand(contextWrapper);

        JCommander commander = JCommander.newBuilder()
                .addObject(app)  // Register the main parameters
                .addCommand("preset", presetCmd)  // Register the command
                .build();

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

        // Allocate and read graph data
        StaticArray<EdgeID> xadj = new StaticArray<>(0);
        StaticArray<NodeID> adjncy = new StaticArray<>(0);
        StaticArray<NodeWeight> vwgt = new StaticArray<>(0);
        StaticArray<EdgeWeight> adjwgt = new StaticArray<>(0);

        try {
            if (app.validate) {
                MetisReader.read(app.graphFilename, xadj, adjncy, vwgt, adjwgt, false);
                GraphValidator.validateUndirectedGraph(xadj, adjncy, vwgt, adjwgt);
            } else {
                MetisReader.read(app.graphFilename, xadj, adjncy, vwgt, adjwgt, false);

            }
        } catch (IOException e) {
            System.err.println("Error reading graph file: " + e.getMessage());
            System.exit(1);
        }

        int n = xadj.size() - 1;
        BlockID[] partition = new BlockID[n];
        for (int i = 0; i < n; i++) {
            partition[i] = new BlockID(0);
        }

        // Set up the partitioner and perform partitioning
        kaminpar.KaMinPar partitioner = new kaminpar.KaMinPar(app.numThreads, contextWrapper.ctx);
        partitioner.reseed(app.seed);

        if (app.quiet) {
            partitioner.setOutputLevel(OutputLevel.QUIET);
        } else if (app.experiment) {
            partitioner.setOutputLevel(OutputLevel.EXPERIMENT);
        }

        partitioner.setMaxTimerDepth(app.maxTimerDepth);
        partitioner.takeGraph(n, xadj, adjncy, vwgt, adjwgt);

        partitioner.computePartition(new BlockID(app.k), partition);

        // Convert BlockID[] to List<Integer>
        List<Integer> partitionList = convertBlockIDArrayToList(partition);

        // Save the graph partition if an output file is specified
        if (!app.partitionFilename.isEmpty()) {
            try {
                ShmIO.writePartition(app.partitionFilename, partitionList);
            } catch (IOException e) {
                System.err.println("Error writing partition file: " + e.getMessage());
            }
        }
    }

    public static List<Integer> convertBlockIDArrayToList(BlockID[] partition) {
        List<Integer> partitionList = new ArrayList<>(partition.length);
        for (BlockID blockID : partition) {
            partitionList.add(blockID.value); // Assuming BlockID has a field 'value' of type int
        }
        return partitionList;
    }

    public static void setupContext(JCommander commander, ContextWrapper contextWrapper) {
        KaminparArguments.createAllOptions(commander, contextWrapper.ctx);
    }

}