package mrpolyonymous;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.google.cloud.ServiceOptions;

public class TableSizerApp {

    // Command line options
    private static final String OPT_SHORT_HELP = "h";
    private static final String OPT_LONG_HELP = "help";
    private static final String OPT_LONG_JOB_PROJECT = "jobProject";
    private static final String OPT_LONG_MAX_TABLES = "maxTables";
    private static final String OPT_LONG_MAX_COLUMNS = "maxColumns";
    private static final String OPT_LONG_PROJECT = "project";
    private static final String OPT_LONG_DATASETS = "datasets";
    private static final String OPT_LONG_DATASET_PREFIX = "datasetPrefix";
    private static final String OPT_LONG_TABLES = "tables";
    private static final String OPT_LONG_NO_OUTPUT = "noOutput";
    private static final String OPT_LONG_OUTPUT_PREFIX = "outputPrefix";
    private static final String OPT_LONG_SIZE_PARTITIONS = "sizePartitions";
    private static final String OPT_LONG_MIN_PARTITION_VALUE = "minPartitionTime";
    private static final String OPT_LONG_MAX_PARTITION_VALUE = "maxPartitionTime";
    private static final String OPT_LONG_MAX_THREADS = "maxThreads";
    private static final String OPT_LONG_IGNORE = "ignore";

    public static void main(String[] args) {
        // Process command line options
        Options options = getCommandLineOptions();
        CommandLine commandLine = parseCommandLine(options, args);

        TableSizerConfig config = getConfig(commandLine);
        if (config == null) {
            // There was a problem parsing the config, exit with non-zero status
            System.exit(-1);
        }

        TableSizerApp app = new TableSizerApp(config);
        try {
            app.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private final TableSizerConfig config;
    private final Instant runTime;
    private final String localRunTime;

    private TableSizerApp(TableSizerConfig config) {
        this.config = config;

        var nowLocal = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        // filename friendly version of now
        localRunTime = DateTimeFormatter.ofPattern("uuuu-MM-dd-HH-mm-ss").format(nowLocal);
        runTime = nowLocal.toInstant();
    }

    private void run() throws IOException, InterruptedException, ExecutionException {
        try (TableSizer tableSizer = new TableSizer(config)) {
            long start = System.currentTimeMillis();
            tableSizer.run();
            long end = System.currentTimeMillis();
            System.out.format("\nRetrieving sizes took %,dms\n", end - start);

            printResults(tableSizer);

            if (config.saveOutput()) {
                saveTableResults(tableSizer);
                saveColumnResults(tableSizer);
                if (config.sizeIndividualPartitions()) {
                    saveColumnPartitionResults(tableSizer);
                }
            }

            if (tableSizer.getErroredColumns().size() > 0) {
                System.out.println("Errors in columns");
                for (ErroredColumn errorColumn : tableSizer.getErroredColumns()) {
                    System.out.println(errorColumn);
                }
            }
        }
    }

    private void printResults(TableSizer tableSizer) {
        for (TableData tableData : tableSizer.getPerTableData().values()) {
            System.out.println("Sizes for table " + TableSizer.formatTableId(tableData));
            long totalSizeFromColumns = printColumnTable(tableData.getColumnSizes(), tableData.getNumRows());

            // Display both calculations as a sanity check
            System.out.format("Total size from columns: %,d bytes\n", totalSizeFromColumns);
            System.out.format("Total size from table metadata: %,d bytes\n", tableData.getLogicalSize());
            if (tableData.getLogicalSize() > 0) {
                // check if computed size value is within 2% of the real value
                double frac = (double) totalSizeFromColumns / (double) tableData.getLogicalSize();
                if (frac < 0.98 || frac > 1.02) {
                    // Could be an issue with the table, this code, or simply the config limited the number of columns
                    System.out.println("Estimated sizes differ significantly from total size");
                }
            }

            if (config.sizeIndividualPartitions()) {
                for (Map.Entry<String, TableData> mapEntry : tableSizer.getPerTableData().entrySet()) {
                    TreeMap<String, PartitionData> sortedPartitions = new TreeMap<>(
                            mapEntry.getValue().getPartitionDataMap());
                    for (Map.Entry<String, PartitionData> sortedEntry : sortedPartitions.entrySet()) {
                        String tableId = mapEntry.getKey();
                        String partitionDecorator = sortedEntry.getKey();
                        PartitionData partitionData = sortedEntry.getValue();
                        if (partitionData.isEmpty()) {
                            continue;
                        }

                        System.out.format("Table %s partition %s sizes\n", tableId, partitionDecorator);
                        long totalPartitionSizeFromColumns = printColumnTable(partitionData.getColumnSizes(), null);
                        System.out.format("Total partition size from columns: %,d\n", totalPartitionSizeFromColumns);
                        System.out.format("Total expected partition size:     %,d\n", partitionData.getPartitionSize());

                    }
                }
            }

            System.out.println();
        }
    }

    private long printColumnTable(Map<String, Long> columnSizes, Long numRows) {

        long totalSizeFromColumns = 0;
        TreeMap<String, Long> sizesSortedByName = new TreeMap<>(columnSizes);
        // First pass to get width
        int nameWidth = 1;
        int sizeWidth = 1;
        for (Map.Entry<String, Long> mapEntry : sizesSortedByName.entrySet()) {
            nameWidth = Math.max(nameWidth, mapEntry.getKey().length());
            sizeWidth = Math.max(sizeWidth, String.format("%,d", mapEntry.getValue()).length());
        }

        String format;
        if (numRows == null) {
            format = "Column %" + nameWidth + "s: %," + sizeWidth + "d bytes\n";
            numRows = 0L;
        } else {
            format = "Column %" + nameWidth + "s: %," + sizeWidth + "d bytes, avg %.1f bytes per row\n";
        }

        for (Map.Entry<String, Long> mapEntry : sizesSortedByName.entrySet()) {
            totalSizeFromColumns += mapEntry.getValue();
            System.out.format(format,
                    mapEntry.getKey(),
                    mapEntry.getValue(),
                    numRows == 0 ? 0.0 : mapEntry.getValue().doubleValue() / numRows);
        }
        return totalSizeFromColumns;
    }

    private void saveTableResults(TableSizer tableSizer) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        OutputStream resultStream = new GZIPOutputStream(new FileOutputStream(filenameFor("-table-analysis-")));
        ObjectWriter ow = objectMapper.writer().withRootValueSeparator("\n");
        SequenceWriter seq = ow.writeValues(resultStream);

        for (TableData tableData : tableSizer.getPerTableData().values()) {
            TableResult tableResult = new TableResult(runTime,
                    tableData.getTableId(),
                    tableData.getLogicalSize(),
                    tableData.getNumRows());
            seq.write(tableResult);
        }
        seq.close();
        resultStream.close();
    }

    private String filenameFor(String fileType) {
        return config.outputPrefix() + fileType + localRunTime + ".json.gz";
    }

    private void saveColumnResults(TableSizer tableSizer) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        OutputStream resultStream = new GZIPOutputStream(new FileOutputStream(filenameFor("-column-analysis-")));
        ObjectWriter ow = objectMapper.writer().withRootValueSeparator("\n");
        SequenceWriter seq = ow.writeValues(resultStream);

        for (TableData tableData : tableSizer.getPerTableData().values()) {
            for (Map.Entry<String, Long> entry : tableData.getColumnSizes().entrySet()) {
                ColumnResult result = new ColumnResult(runTime,
                        tableData.getTableId(),
                        entry.getKey(),
                        entry.getValue());
                seq.write(result);
            }
        }
        seq.close();
        resultStream.close();
    }

    private void saveColumnPartitionResults(TableSizer tableSizer) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(Include.ALWAYS);
        OutputStream resultStream = new GZIPOutputStream(new FileOutputStream(filenameFor("-partition-analysis-")));
        ObjectWriter ow = objectMapper.writer().withRootValueSeparator("\n");
        SequenceWriter seq = ow.writeValues(resultStream);

        for (TableData tableData : tableSizer.getPerTableData().values()) {
            // First, write the results for the whole table
            for (Map.Entry<String, Long> entry : tableData.getColumnSizes().entrySet()) {
                ColumnResult result = new ColumnResult(runTime,
                        tableData.getTableId(),
                        entry.getKey(),
                        entry.getValue());
                seq.write(result);
            }

            // Next, write the results for each partition that has data
            for (Map.Entry<String, PartitionData> partitionEntry : tableData.getPartitionDataMap().entrySet()) {
                for (Map.Entry<String, Long> columnSizeEntry : partitionEntry.getValue().getColumnSizes().entrySet()) {
                    ColumnResult result = new ColumnResult(runTime,
                            tableData.getTableId(),
                            columnSizeEntry.getKey(),
                            columnSizeEntry.getValue());
                    result.setPartitionDecorator(partitionEntry.getKey());
                    seq.write(result);
                }
            }

        }
        seq.close();
        resultStream.close();
    }

    /**
     * Return the command line options
     */
    private static Options getCommandLineOptions() {
        Options options = new Options();

        options.addOption(Option.builder(OPT_SHORT_HELP).longOpt(OPT_LONG_HELP).desc("Show help").build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_JOB_PROJECT)
                .hasArg()
                .required(false)
                .desc("GCP project in which to run size estimation query jobs")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_MAX_TABLES)
                .hasArg()
                .required(false)
                .desc("Maximum number of tables to size. Use this to speed up the sizing process while testing.")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_MAX_COLUMNS)
                .hasArg()
                .required(false)
                .desc("Maximum number of columns to retrieve per table. Use this to speed up the sizing process while testing.")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_PROJECT)
                .hasArg()
                .required(false)
                .desc("GCP project to scan for datasets and tables")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_DATASETS)
                .hasArg()
                .required(false)
                .desc("Comma-separated list of datasets to scan. Can either be of form `project`.`dataset` or `dataset`, in which case the scan project is used")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_DATASET_PREFIX)
                .hasArg()
                .required(false)
                .desc("Only scan tables in datasets with name beginning with this prefix")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_TABLES)
                .hasArg()
                .required(false)
                .desc("Comma-separated list of BigQuery table names to size. Can be fully qualified `project`.`dataset`.`table` format, `dataset`.`table` format, in which case project option is required, or just `table`, in which case all project.dataset combinations are scanned for that table name")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_NO_OUTPUT)
                .required(false)
                .desc("Do not savew output to files")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_OUTPUT_PREFIX)
                .hasArg()
                .required(false)
                .desc("Prefix to give to output files")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_SIZE_PARTITIONS)
                .hasArg(false)
                .desc("Find column sizes for individual partitions as well as the entire table")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_MIN_PARTITION_VALUE)
                .hasArg()
                .desc("When sizing individual partitions, the earliest partition to size (inclusive)")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_MAX_PARTITION_VALUE)
                .hasArg()
                .desc("When sizing individual partitions, the latest partition to size (exclusive)")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_MAX_THREADS)
                .hasArg()
                .desc("Maximum threads/simultaneous queries to run")
                .build());

        options.addOption(Option.builder()
                .longOpt(OPT_LONG_IGNORE)
                .hasArg()
                .desc("Ignore value. When you want to keep some value close by but don't want it used")
                .build());

        return options;
    }

    /**
     * Parse command line options and return the parsed info. If help is requested or there is an error
     * in the options the help is shown and the process will exit without returning from this method
     */
    private static CommandLine parseCommandLine(Options options, String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        int helpExitCode = 0;
        var showHelp = false;

        // First check to see if the user only requested help with no other options.
        // This allows arguments that would otherwise be required to be ignored.
        Options helpOptions = new Options();
        helpOptions.addOption(Option.builder(OPT_SHORT_HELP).longOpt(OPT_LONG_HELP).desc("Show help").build());
        try {
            commandLine = parser.parse(helpOptions, args, true);
            showHelp = commandLine.hasOption(OPT_SHORT_HELP);
        } catch (ParseException e) {
            // Ignore
        }

        if (!showHelp) {
            try {
                commandLine = parser.parse(options, args, false);
            } catch (ParseException e) {
                // Exiting due to an error, exit process with non-zero value
                helpExitCode = 1;
                commandLine = null;
            }
            if (commandLine != null && commandLine.hasOption(OPT_SHORT_HELP)) {
                showHelp = true;
            }
        }

        if (showHelp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(TableSizerApp.class
                    .getName(), "Estimate size of columns in BigQuery tables. Options:", options, "", true);
            System.exit(helpExitCode);
        }
        return commandLine;
    }

    /**
     * Turn parsed command line arguments into usable object, or return null if there is a problem
     * parsing the arguments
     * 
     */
    private static TableSizerConfig getConfig(CommandLine commandLine) {
        final String defaultProject = ServiceOptions.getDefaultProjectId();
        if (defaultProject == null) {
            throw new IllegalArgumentException(
                    "No default GCP project found. This program requires a default project and uses the default application credentials for your environment. Make sure the GCP SDK is installed.");
        }

        String jobProject = defaultProject;
        if (commandLine.hasOption(OPT_LONG_JOB_PROJECT)) {
            jobProject = commandLine.getOptionValue(OPT_LONG_JOB_PROJECT);
        }

        final int maxTables = getIntegerOption(commandLine, OPT_LONG_MAX_TABLES, "Max tables", 0);
        final int maxColumnsPerTable = getIntegerOption(commandLine, OPT_LONG_MAX_COLUMNS, "Max columns", 0);
        // Going too big on the thread count sometimes causes problems fetching results.
        final int maxThreads = getIntegerOption(commandLine,
                OPT_LONG_MAX_THREADS,
                "Max threads",
                Math.max(1, Runtime.getRuntime().availableProcessors()));

        String scanProject = defaultProject;
        if (commandLine.hasOption(OPT_LONG_PROJECT)) {
            scanProject = commandLine.getOptionValue(OPT_LONG_PROJECT);
        }

        List<String> datasetsToScan = Collections.emptyList();
        if (commandLine.hasOption(OPT_LONG_DATASETS)) {
            datasetsToScan = Arrays.asList(commandLine.getOptionValue(OPT_LONG_DATASETS).split(","));
        }

        String matchDatasetPrefix = "";
        if (commandLine.hasOption(OPT_LONG_DATASET_PREFIX)) {
            matchDatasetPrefix = commandLine.getOptionValue(OPT_LONG_DATASET_PREFIX);
        }

        List<String> tablesToScan = Collections.emptyList();
        if (commandLine.hasOption(OPT_LONG_TABLES)) {
            tablesToScan = Arrays.asList(commandLine.getOptionValue(OPT_LONG_TABLES).split(","));
        }

        String outputPrefix = "tablesize";
        if (commandLine.hasOption(OPT_LONG_OUTPUT_PREFIX)) {
            outputPrefix = commandLine.getOptionValue(OPT_LONG_OUTPUT_PREFIX);
        }

        boolean saveOutput = true;
        if (commandLine.hasOption(OPT_LONG_NO_OUTPUT)) {
            saveOutput = false;
        }

        boolean sizeIndividualPartitions = false;
        if (commandLine.hasOption(OPT_LONG_SIZE_PARTITIONS)) {
            sizeIndividualPartitions = true;
        }

        OffsetDateTime minPartitionValueInclusive = null;
        if (commandLine.hasOption(OPT_LONG_MIN_PARTITION_VALUE)) {
            minPartitionValueInclusive = parseTime(commandLine.getOptionValue(OPT_LONG_MIN_PARTITION_VALUE));
        }

        OffsetDateTime maxPartitionValueExclusive = null;
        if (commandLine.hasOption(OPT_LONG_MAX_PARTITION_VALUE)) {
            maxPartitionValueExclusive = parseTime(commandLine.getOptionValue(OPT_LONG_MAX_PARTITION_VALUE));
        }

        return new TableSizerConfig(jobProject,
                maxTables,
                maxColumnsPerTable,
                scanProject,
                datasetsToScan,
                matchDatasetPrefix,
                tablesToScan,
                saveOutput,
                outputPrefix,
                sizeIndividualPartitions,
                minPartitionValueInclusive,
                maxPartitionValueExclusive,
                maxThreads);
    }

    private static OffsetDateTime parseTime(String optionValue) {
        try {
            return Instant.parse(optionValue).atOffset(ZoneOffset.UTC);
        } catch (RuntimeException e) {
            try {
                return LocalDate.parse(optionValue).atStartOfDay().atOffset(ZoneOffset.UTC);
            } catch (RuntimeException e2) {
                throw new BadPartitionException("Invalid partition time bound " + optionValue);
            }
        }
    }

    private static int getIntegerOption(CommandLine commandLine, String longOpt, String typeString, int defaultValue) {
        int val = defaultValue;
        if (commandLine.hasOption(longOpt)) {
            try {
                val = Integer.parseInt(commandLine.getOptionValue(longOpt));
                if (val < 0) {
                    System.out.println(typeString + " must be >= 0; using default " + defaultValue);
                    val = defaultValue;
                }
            } catch (NumberFormatException e) {
                System.out.println(typeString + " must be an integer");
                throw e;
            }
        }
        return val;
    }

    static class BadPartitionException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public BadPartitionException(String val) {
            super("'" + val + "' is not a valid partition bound");
        }
    }
}
