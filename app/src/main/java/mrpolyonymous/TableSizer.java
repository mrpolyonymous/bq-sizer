package mrpolyonymous;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.JobStatistics.QueryStatistics;

/**
 * The main logic to find the size of each column in a talbe, and optionally the size of each column
 * within a partition
 */
public class TableSizer implements AutoCloseable {

    /** Virtual partition time column name */
    public static final String PARTITIONTIME = "_PARTITIONTIME";
    /** Special partition decorator when a row has no partition info */
    public static final String NULL_PARTITION = "__NULL__";
    /**
     * Special partition decorator when a row's partition value is outside the min/max partition bounds
     */
    public static final String UNPARTITIONED_PARTITION = "__UNPARTITIONED__";

    private static final OffsetDateTime EPOCH = Instant.EPOCH.atOffset(ZoneOffset.UTC);

    public static String formatDatasetId(DatasetId datasetId) {
        return datasetId.getProject() + "." + datasetId.getDataset();
    }

    public static String formatTableId(TableId tableId) {
        return tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable();
    }

    public static String formatTableId(TableData tableData) {
        return formatTableId(tableData.getTableId());
    }

    private final BigQuery bq;
    /**
     * Executor service for things that submit real work tasks and wait for them to finish. There are
     * two services so that the worker pool doesn't fill up with threads waiting on tasks to complete,
     * which cannot complete as there are no spare threads to run them.
     */
    private ExecutorService supervisorService;
    /** Executor service for things that do real work. */
    private ExecutorService workerService;
    private Map<String, TableData> perTableData;
    private List<ErroredColumn> erroredColumns;

    private final TableSizerConfig config;

    public TableSizer(TableSizerConfig config) {
        this.config = config;

        // use default application credentials
        bq = BigQueryOptions.getDefaultInstance().getService();

        perTableData = new HashMap<>();

        supervisorService = Executors.newWorkStealingPool();
        workerService = Executors.newFixedThreadPool(config.maxThreads());

        erroredColumns = new ArrayList<>();
    }

    Map<String, TableData> getPerTableData() {
        return perTableData;
    }

    List<ErroredColumn> getErroredColumns() {
        return erroredColumns;
    }

    TableSizerConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        workerService.shutdown();
        supervisorService.shutdown();
        try {
            workerService.awaitTermination(24, TimeUnit.HOURS);
            supervisorService.awaitTermination(24, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            // not really any graceful way to handle this,
            // dump it to stderr and hope the program terminates
            System.err.println("Interrupted while shutting down executor service");
            e.printStackTrace();
        }
    }

    private static record ColumnFuture(Future<Long> sizeFuture,
            TableId tableId,
            String partitionDecorator,
            String column) {
        String formattedDecorator() {
            if (partitionDecorator == null) {
                return "";
            }
            return "$" + partitionDecorator;
        }
    }

    /**
     * The main entry point to the column size estimation process
     */
    public void run() throws InterruptedException, ExecutionException, IOException {

        List<TableId> runOnTableIds = resolveTableIds();

        List<Future<TableData>> tableFutures = new ArrayList<>();
        for (TableId tableId : runOnTableIds) {
            Future<TableData> f = workerService.submit(new TableDataFetcher(this, tableId));
            tableFutures.add(f);
            if (config.maxTables() > 0 && tableFutures.size() >= config.maxTables()) {
                break;
            }
        }

        for (Future<TableData> f : tableFutures) {
            TableData tableData = f.get();
            if (tableData != null) {
                // can be null if the table is not found
                perTableData.put(formatTableId(tableData), tableData);
            }
        }

        List<Future<?>> supervisorFutures = new ArrayList<>();
        for (TableData tableData : perTableData.values()) {

            if (tableData.getLogicalSize() == 0) {
                // no need to size columns if the table is empty
                continue;
            }

            Future<?> f = asyncGetColumnSizes(tableData,
                    null,
                    null,
                    null,
                    columnName -> new ColumnSizeFetcher(tableData, columnName));
            supervisorFutures.add(f);
        }

        if (config.sizeIndividualPartitions()) {
            for (TableData tableData : perTableData.values()) {
                if (tableData.getLogicalSize() == 0) {
                    continue;
                }

                if (!tableData.isPartitioned()) {
                    System.out.format("Table %s is not partitioned, nothing to do\n", formatTableId(tableData));
                    continue;
                }

                refinePartitionBounds(tableData);

                if (!tableData.getPartitionColumn().equals(PARTITIONTIME)) {
                    supervisorFutures.add(asyncGetNullPartitionSize(tableData));
                    supervisorFutures.add(asyncGetUnpartitionedPartitionSize(tableData));
                }

                for (PartitionBounds bounds : tableData.getPartitionIterable()) {
                    Future<?> f = asyncGetColumnSizes(tableData,
                            bounds.partitionDecorator(),
                            () -> getColumnSizeWithBounds(tableData.getTableId(),
                                    "*",
                                    tableData.getPartitionColumn(),
                                    bounds),
                            () -> getColumnSizeWithBounds(tableData.getTableId(),
                                    tableData.getPartitionColumn(),
                                    tableData.getPartitionColumn(),
                                    bounds),
                            columnName -> new BoundedColumnSizeFetcher(tableData, columnName, bounds));
                    supervisorFutures.add(f);
                }
            }
        }

        // wait for all tasks to complete
        for (Future<?> future : supervisorFutures) {
            future.get();
        }
    }

    /**
     * Make a better guess at which partitions actually contain data
     */
    private void refinePartitionBounds(TableData tableData) {
        if (!(tableData.getPartitionIterable() instanceof TimeBoundsIterable)) {
            return;
        }

        TimeBoundsIterable timeBounds = (TimeBoundsIterable) tableData.getPartitionIterable();
        if (!timeBounds.isMinDateGuess()) {
            // If the lower bound was known due to partition expiry it's not likely this refinement process will be useful
            return;
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime min = timeBounds.getMinDate();
        if (!min.isBefore(now)) {
            return;
        }

        List<TimeInterval> refinedIntervals = findIntervalsWithData(tableData, timeBounds);
        tableData.setPartitionIterable(timeBounds.withRefinedIntervals(refinedIntervals));
    }

    private String getIntervalSizeQuery(TableData tableData,
            TimeBoundsIterable timeBounds,
            OffsetDateTime minDate,
            OffsetDateTime maxDate) {
        TableId tableId = tableData.getTableId();
        String sql = "SELECT * FROM `" + tableId.getProject()
                + "`.`"
                + tableId.getDataset()
                + "`.`"
                + tableId.getTable()
                + "` WHERE `"
                + tableData.getPartitionColumn()
                + "` >= "
                + timeBounds.formatForSql(minDate)
                + " AND `"
                + tableData.getPartitionColumn()
                + "` < "
                + timeBounds.formatForSql(maxDate);
        return sql;

    }

    private List<TimeInterval> findIntervalsWithData(TableData tableData, TimeBoundsIterable timeBounds) {
        List<TimeInterval> foundIntervals = new ArrayList<>();

        final OffsetDateTime nextPartitionAfterEpoch = EPOCH.plus(1, timeBounds.getGranularity());

        String sql;
        long size;

        // No point testing epoch stuff when using _PARTITIONTIME as BigQuery didn't exist back then
        if (!tableData.getPartitionColumn().equals(PARTITIONTIME)) {
            sql = getIntervalSizeQuery(tableData, timeBounds, TimeBoundsIterable.MIN_PARTITION_TIME, EPOCH);
            size = runQueryGetEstimatedBytes(sql);
            if (size <= 0) {
                System.out.format("Table %s: no data between 1960 and 1970\n", formatTableId(tableData));
            } else {
                int maxSplits = splitsRequired(
                        timeBounds.partitionsBetween(TimeBoundsIterable.MIN_PARTITION_TIME, EPOCH));
                findIntervalsWithData(tableData,
                        timeBounds,
                        foundIntervals,
                        TimeBoundsIterable.MIN_PARTITION_TIME,
                        EPOCH,
                        size,
                        size,
                        maxSplits);
            }

            // special testing around epoch. It's not uncommon for errors to result in a time field
            // being set to 0, or simply defaulting to 0, and being written to the epoch partition 
            sql = getIntervalSizeQuery(tableData, timeBounds, EPOCH, nextPartitionAfterEpoch);
            size = runQueryGetEstimatedBytes(sql);
            if (size <= 0) {
                System.out.format("Table %s: no data at epoch\n", formatTableId(tableData));
            } else {
                System.out.format("Table %s: data entered at epoch, possible mistake: %,d bytes\n",
                        formatTableId(tableData),
                        size);
                foundIntervals.add(new TimeInterval(EPOCH, nextPartitionAfterEpoch));
            }

        }

        sql = getIntervalSizeQuery(tableData, timeBounds, nextPartitionAfterEpoch, timeBounds.getMinDate());
        size = runQueryGetEstimatedBytes(sql);
        if (size <= 0) {
            System.out.format("Table %s: no data between epoch+1 %s and %s\n",
                    formatTableId(tableData),
                    timeBounds.getGranularity(),
                    timeBounds.getMinDate());
        } else {
            System.out.format("Table %s: no data between %s and %s: %,d bytes\n",
                    formatTableId(tableData),
                    nextPartitionAfterEpoch.toString(),
                    timeBounds.getMinDate().toString(),
                    size);
            int maxSplits = splitsRequired(
                    timeBounds.partitionsBetween(nextPartitionAfterEpoch, timeBounds.getMinDate()));
            findIntervalsWithData(tableData,
                    timeBounds,
                    foundIntervals,
                    nextPartitionAfterEpoch,
                    timeBounds.getMinDate(),
                    size,
                    size,
                    maxSplits);
        }

        int maxSplits = splitsRequired(timeBounds.partitionsBetween(timeBounds.getMinDate(), timeBounds.getMaxDate()));
        findIntervalsWithData(tableData,
                timeBounds,
                foundIntervals,
                timeBounds.getMinDate(),
                timeBounds.getMaxDate(),
                null,
                null,
                maxSplits);

        return foundIntervals;
    }

    private static int splitsRequired(long numPartitions) {
        if (numPartitions <= 1) {
            return 0;
        } else if (numPartitions == 2) {
            return 1;
        }

        int splitsRequired = (int) Math.ceil(Math.log(numPartitions) / Math.log(2.0));
        // Place upper bound of 10 splits, more than that can mean too many queries
        return Math.min(10, splitsRequired);
    }

    /**
     * 
     * @param tableData
     * @param timeBounds
     * @param foundIntervals
     * @param min
     * @param max
     * @param knownSize
     *        size of data between min and max, if known
     * @param parentIntervalSize
     *        size of data in parent interval, if known
     * @param maxSplits
     * @return
     */
    private final Long findIntervalsWithData(TableData tableData,
            TimeBoundsIterable timeBounds,
            List<TimeInterval> foundIntervals,
            OffsetDateTime min,
            OffsetDateTime max,
            Long knownSize,
            Long parentIntervalSize,
            int maxSplits) {
        if (!max.isAfter(min)) {
            return 0L;
        }

        System.out.format("Table %s: finding data between %s and %s\n",
                formatTableId(tableData),
                min.toString(),
                max.toString());

        long size;
        if (knownSize == null) {
            String sql = getIntervalSizeQuery(tableData, timeBounds, min, max);
            size = runQueryGetEstimatedBytes(sql);
        } else {
            size = knownSize;
        }
        if (parentIntervalSize == null) {
            parentIntervalSize = size;
        }

        if (size <= 0L) {
            return 0L;
        }

        OffsetDateTime mid = timeBounds.midPoint(min, max);
        if (maxSplits <= 0 || !mid.isAfter(min)) {
            TimeInterval bounds = new TimeInterval(min, max);
            foundIntervals.add(bounds);
            return size;
        }

        Long minSize = findIntervalsWithData(tableData,
                timeBounds,
                foundIntervals,
                min,
                mid,
                null,
                size,
                maxSplits - 1);
        long remainingSize = parentIntervalSize - minSize;
        if (minSize > 0L && remainingSize > 0L && (maxSplits < 3 || Duration.between(mid, max).toDays() < 7)) {
            // If there's data in the lower half there's likely to be diminishing returns in splitting the upper half, it's probably full
            TimeInterval bounds = new TimeInterval(mid, max);
            foundIntervals.add(bounds);
        } else {
            findIntervalsWithData(tableData, timeBounds, foundIntervals, mid, max, size - minSize, size, maxSplits - 1);
        }
        return size;
    }

    private Future<?> asyncGetNullPartitionSize(TableData tableData) throws InterruptedException {
        return asyncGetColumnSizes(tableData,
                NULL_PARTITION,
                () -> getNullPartitionColumnSize(tableData.getTableId(), "*", tableData.getPartitionColumn()),
                () -> getNullPartitionColumnSize(tableData.getTableId(),
                        tableData.getPartitionColumn(),
                        tableData.getPartitionColumn()),
                columnName -> new ColumnSizeFetcherNullPartition(tableData, columnName));
    }

    private Future<?> asyncGetUnpartitionedPartitionSize(TableData tableData) throws InterruptedException {
        return asyncGetColumnSizes(tableData,
                UNPARTITIONED_PARTITION,
                () -> getUnpartitionedPartitionColumnSize(tableData.getTableId(),
                        "*",
                        tableData.getPartitionColumn(),
                        tableData.getPartitionIterable()),
                () -> getUnpartitionedPartitionColumnSize(tableData.getTableId(),
                        tableData.getPartitionColumn(),
                        tableData.getPartitionColumn(),
                        tableData.getPartitionIterable()),
                columnName -> new ColumnSizeFetcherUnpartitionedPartition(tableData, columnName));
    }

    private Future<?> asyncGetColumnSizes(TableData tableData,
            String partitionDecorator,
            Supplier<Long> initPartitionSize,
            Supplier<Long> initPartitionColumnSize,
            Function<String, Callable<Long>> columnSizeFetcherSupplier) throws InterruptedException {
        Runnable task = () -> {
            Long partitionSize = null;
            if (initPartitionSize != null) {
                Objects.requireNonNull(initPartitionColumnSize);

                partitionSize = initPartitionSize.get();

                if (partitionSize <= 0L) {
                    System.out.format("Table %s: has no data in partition %s",
                            formatTableId(tableData),
                            partitionDecorator);
                    tableData.markEmpty(partitionDecorator);
                    return;
                }

                Long partitionColumnSize = initPartitionColumnSize.get();
                tableData.initPartitionData(partitionDecorator, partitionSize, partitionColumnSize);
            }

            List<ColumnFuture> colSizeFutures = new ArrayList<>();
            int colCount = 0;
            for (String columnName : tableData.getColumnNames()) {
                // Might be the case if the column is the partitioning column
                if (tableData.hasSizeForColumn(partitionDecorator, columnName)) {
                    continue;
                }
                // Sometimes columns have 0 size across the whole table, which means they'll have 0 size in a partition
                if (partitionDecorator != null) {
                    Long wholeSize = tableData.getColumnSizes().get(columnName);
                    // wholeSize can be null if fetching the column size for the whole table hasn't finished yet.
                    // We could wait for that to finish but it's complicated and doesn't really speed things up.
                    if (wholeSize != null && wholeSize.longValue() == 0L) {
                        System.out.format(
                                "Table %s: column %d is empty in whole table, skipping sizing it in partition",
                                formatTableId(tableData),
                                columnName);
                        tableData.addPartitionColumnSize(partitionDecorator, columnName, 0L, false);
                        continue;
                    }
                }

                Callable<Long> columnSizer = columnSizeFetcherSupplier.apply(columnName);
                Future<Long> colSizeFuture = workerService.submit(columnSizer);
                colSizeFutures
                        .add(new ColumnFuture(colSizeFuture, tableData.getTableId(), partitionDecorator, columnName));

                ++colCount;
                if (config.maxColumnsPerTable() > 0 && colCount >= config.maxColumnsPerTable()) {
                    break;
                }
            }
            for (ColumnFuture colFuture : colSizeFutures) {
                try {
                    System.out.format("Getting result for %s%s:%s\n",
                            formatTableId(colFuture.tableId()),
                            colFuture.formattedDecorator(),
                            colFuture.column());
                    Future<Long> f = colFuture.sizeFuture();
                    Long columnSize;
                    try {
                        columnSize = f.get();
                    } catch (InterruptedException e) {
                        // Can't throw checked exceptions inside a runnable
                        throw new RuntimeException(e);
                    }
                    tableData.addPartitionColumnSize(partitionDecorator,
                            colFuture.column(),
                            columnSize,
                            tableData.getPartitionColumn() != null);
                } catch (ExecutionException e) {
                    System.err.format("Error fetching column %s%s:%s\n",
                            formatTableId(colFuture.tableId()),
                            colFuture.formattedDecorator(),
                            colFuture.column());
                    e.printStackTrace();
                    erroredColumns.add(new ErroredColumn(colFuture.tableId(), colFuture.column()));
                }
            }
        };
        return supervisorService.submit(task);
    }

    /**
     * Turn configuration options into an explicit list of tables to be sized.
     */
    private List<TableId> resolveTableIds() {
        List<TableId> tableIds = new ArrayList<>();

        List<String> toResolveTables = new ArrayList<>();
        for (String idString : config.tablesToScan()) {
            idString = idString.trim();
            if (idString.isEmpty()) {
                continue;
            }

            String[] idComponents = idString.split("\\.");
            if (idComponents.length == 3) {
                tableIds.add(TableId.of(idComponents[0], idComponents[1], idComponents[2]));
            } else if (idComponents.length == 2) {
                tableIds.add(TableId.of(config.scanProject(), idComponents[0], idComponents[1]));
            } else if (idComponents.length == 1) {
                toResolveTables.add(idString);
            } else {
                throw new IllegalArgumentException("Invalid table " + idString);
            }

            if (config.maxTables() > 0 && tableIds.size() >= config.maxTables()) {
                break;
            }
        }

        if (tableIds.isEmpty() || !toResolveTables.isEmpty()) {
            List<DatasetId> datasetIds = parseDatasets();

            if (datasetIds.isEmpty()) {
                System.out.println("No datasets specified, listing all datasets in " + config.scanProject());
                Page<Dataset> datasets = bq.listDatasets(config.scanProject());
                for (Dataset ds : datasets.iterateAll()) {
                    if (ds.getDatasetId().getDataset().startsWith(config.matchDatasetPrefix())) {
                        datasetIds.add(ds.getDatasetId());
                    }
                }
            }
            for (DatasetId datasetId : datasetIds) {
                if (toResolveTables.isEmpty()) {
                    // find all tables in dataset
                    System.out.println(
                            "No tables specified, listing all tables in dataset " + formatDatasetId(datasetId));

                    Page<Table> tables = bq.listTables(datasetId);
                    for (Table table : tables.iterateAll()) {
                        tableIds.add(table.getTableId());
                        if (config.maxTables() > 0 && tableIds.size() >= config.maxTables()) {
                            break;
                        }
                    }
                } else {
                    for (String table : toResolveTables) {
                        tableIds.add(TableId.of(datasetId.getProject(), datasetId.getDataset(), table));
                        if (config.maxTables() > 0 && tableIds.size() >= config.maxTables()) {
                            break;
                        }
                    }
                }
                if (config.maxTables() > 0 && tableIds.size() >= config.maxTables()) {
                    break;
                }
            }
        }
        return tableIds;
    }

    private List<DatasetId> parseDatasets() {
        List<DatasetId> datasetIds = new ArrayList<>();
        for (String idString : config.datasetsToScan()) {
            idString = idString.trim();
            if (idString.isEmpty()) {
                continue;
            }

            String[] idComponents = idString.split("\\.");
            if (idComponents.length == 2) {
                datasetIds.add(DatasetId.of(idComponents[0], idComponents[1]));
            } else if (idComponents.length == 1) {
                datasetIds.add(DatasetId.of(config.scanProject(), idComponents[0]));
            } else {
                throw new IllegalArgumentException("Invalid dataset " + idString);
            }
        }

        return datasetIds;
    }

    class ColumnSizeFetcher implements Callable<Long> {
        private final TableData tableData;
        private final String column;

        public ColumnSizeFetcher(TableData tableData, String column) {
            this.tableData = tableData;
            this.column = column;
        }

        @Override
        public Long call() throws Exception {
            Long colSize = getColumnSize(tableData.getTableId(), column, tableData.getPartitionColumn());
            return colSize;
        }
    }

    class BoundedColumnSizeFetcher implements Callable<Long> {
        private final TableData tableData;
        private final String column;
        private final PartitionBounds bounds;

        public BoundedColumnSizeFetcher(TableData tableData, String column, PartitionBounds bounds) {
            this.tableData = tableData;
            this.column = column;
            this.bounds = bounds;
        }

        @Override
        public Long call() throws Exception {
            Long colSize = getColumnSizeWithBounds(tableData.getTableId(),
                    column,
                    tableData.getPartitionColumn(),
                    bounds);
            return colSize;
        }
    }

    class ColumnSizeFetcherNullPartition implements Callable<Long> {
        private final TableData tableData;
        private final String column;

        public ColumnSizeFetcherNullPartition(TableData tableData, String column) {
            this.tableData = tableData;
            this.column = column;
        }

        @Override
        public Long call() throws Exception {
            Long colSize = getNullPartitionColumnSize(tableData.getTableId(), column, tableData.getPartitionColumn());
            return colSize;
        }
    }

    class ColumnSizeFetcherUnpartitionedPartition implements Callable<Long> {
        private final TableData tableData;
        private final String column;

        public ColumnSizeFetcherUnpartitionedPartition(TableData tableData, String column) {
            this.tableData = tableData;
            this.column = column;
        }

        @Override
        public Long call() throws Exception {
            Long colSize = getUnpartitionedPartitionColumnSize(tableData.getTableId(),
                    column,
                    tableData.getPartitionColumn(),
                    tableData.getPartitionIterable());
            return colSize;
        }
    }

    long getColumnSize(TableId tableId, String columnName, String partitionColumn) {
        String sql = "SELECT " + safeForQuery(columnName)
                + " FROM `"
                + tableId.getProject()
                + "`.`"
                + tableId.getDataset()
                + "`.`"
                + tableId.getTable()
                + "`";
        if (partitionColumn != null) {
            // An odd-looking query, but null partition values get their own dedicated partition,
            // so you can't always get all values in a column just by doing column >= value
            sql = sql + " WHERE (`" + partitionColumn + "` IS NULL OR `" + partitionColumn + "` IS NOT NULL)";
        }
        // LIMIT 1 is useless as the queries are only ever dry run,
        // but add it just in case someone decided to run the query
        sql = sql + " LIMIT 1";

        return runQueryGetEstimatedBytes(sql);
    }

    long getNullPartitionColumnSize(TableId tableId, String columnName, String partitionColumn) {
        String sql = "SELECT " + safeForQuery(columnName)
                + " FROM `"
                + tableId.getProject()
                + "`.`"
                + tableId.getDataset()
                + "`.`"
                + tableId.getTable()
                + "` WHERE `"
                + partitionColumn
                + "` IS NULL"
                + " LIMIT 1";

        return runQueryGetEstimatedBytes(sql);
    }

    long getUnpartitionedPartitionColumnSize(TableId tableId,
            String columnName,
            String partitionColumn,
            PartitionBoundsIterable bounds) {
        String sql = "SELECT " + safeForQuery(columnName)
                + " FROM `"
                + tableId.getProject()
                + "`.`"
                + tableId.getDataset()
                + "`.`"
                + tableId.getTable()
                + "` WHERE `"
                + partitionColumn
                + "` < "
                + bounds.getMinUnpartitionedBound()
                + " OR `"
                + partitionColumn
                + "` >= "
                + bounds.getMaxUnpartitionedBound()
                + " LIMIT 1";

        return runQueryGetEstimatedBytes(sql);
    }

    long getColumnSizeWithBounds(TableId tableId, String columnName, String partitionColumn, PartitionBounds bounds) {
        String sql = "SELECT " + safeForQuery(columnName)
                + " FROM `"
                + tableId.getProject()
                + "`.`"
                + tableId.getDataset()
                + "`.`"
                + tableId.getTable()
                + "` WHERE `"
                + partitionColumn
                + "` >= "
                + bounds.minValueSql()
                + " AND `"
                + partitionColumn
                + "` < "
                + bounds.maxValueSql()
                + " LIMIT 1";

        return runQueryGetEstimatedBytes(sql);
    }

    private static String safeForQuery(String columnName) {
        if (columnName.equals("*")) {
            return columnName;
        }
        // Can't do SELECT `parent.child`, it has to be SELECT `parent`.`child`
        return "`" + columnName.replace(".", "`.`") + "`";
    }

    private long runQueryGetEstimatedBytes(String sql) {
        //  setMaximumBytesBilled should not be required, but added just in case dry runs suddenly start billing
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
                .setDryRun(true)
                .setMaximumBytesBilled(10_000_000L)
                .build();
        JobId jobId = JobId.newBuilder().setJob("bqsizer-" + UUID.randomUUID()).setProject(config.jobProject()).build();

        Job job;
        try {
            job = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            QueryStatistics queryStatistics = job.getStatistics();

            Long bytesProcessed = queryStatistics.getTotalBytesProcessed();
            if (bytesProcessed == null) {
                // should never happen, but just in case, treat null as 0
                return 0L;
            } else {
                return bytesProcessed;
            }

        } catch (BigQueryException e) {
            System.err.println("Problem with dry run of query " + sql);
            e.printStackTrace();
            throw e;
        }

    }

    Table getTable(TableId tableId) {
        return bq.getTable(tableId);
    }

}
