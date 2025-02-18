package mrpolyonymous;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.cloud.bigquery.TableId;

/**
 * Statistics discovered about a table and the columns within the table
 */
public class TableData {
    /** Special, invented identifier for "all partitions". Not usable with BigQuery. */
    private static final String ALL_PARTITIONS = "__ALL__";

    private final TableId tableId;

    private final long numRows;
    private final long logicalSize;

    private final List<String> columnNames;

    private final String partitionColumn;
    private final long partitionColumnSize;
    private final boolean partitionFilterRequired;

    private PartitionBoundsIterable partitionIterable;
    private final PartitionData combinedData;
    private final ConcurrentHashMap<String, PartitionData> partitionDataMap;

    public TableData(TableId tableId,
            long numRows,
            long logicalSize,
            String partitionColumn,
            long partitionColumnSize,
            boolean partitionFilterRequired,
            PartitionBoundsIterable partitionIterable,
            List<String> columnNames) {
        this.tableId = tableId;
        this.numRows = numRows;
        this.logicalSize = logicalSize;
        this.partitionColumn = partitionColumn;
        this.partitionColumnSize = partitionColumnSize;
        this.partitionFilterRequired = partitionFilterRequired;
        this.partitionIterable = partitionIterable;
        this.columnNames = columnNames;

        this.combinedData = new PartitionData(ALL_PARTITIONS, partitionColumn, logicalSize, partitionColumnSize);
        if (logicalSize == 0) {
            this.combinedData.setEmpty(true);
        }

        this.partitionDataMap = new ConcurrentHashMap<>();
    }

    public PartitionBoundsIterable getPartitionIterable() {
        return partitionIterable;
    }

    public void setPartitionIterable(PartitionBoundsIterable partitionIterable) {
        this.partitionIterable = partitionIterable;
    }

    public TableId getTableId() {
        return tableId;
    }

    public long getNumRows() {
        return numRows;
    }

    public long getLogicalSize() {
        return logicalSize;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public boolean isPartitioned() {
        return partitionColumn != null;
    }

    public long getPartitionColumnSize() {
        return partitionColumnSize;
    }

    public boolean isPartitionFilterRequired() {
        return partitionFilterRequired;
    }
    
    public Map<String, PartitionData> getPartitionDataMap() {
        return partitionDataMap;
    }

    public void initPartitionData(String partitionDecorator, long partitionSize, long partitionColumnSize) {
        if (partitionColumn == null) {
            throw new IllegalStateException("Table is not partitioned");
        }

        PartitionData partitionData = new PartitionData(partitionDecorator,
                partitionColumn,
                partitionSize,
                partitionColumnSize);
        partitionDataMap.put(partitionDecorator, partitionData);
    }

    public void addPartitionColumnSize(String partitionDecorator,
            String columnName,
            long columnSize,
            boolean sizeIncludesPartitionColumn) {
        PartitionData partitionData;
        if (partitionDecorator == null) {
            partitionData = combinedData;
        } else {
            partitionData = partitionDataMap.get(partitionDecorator);
        }
        if (partitionData == null) {
            throw new IllegalStateException("Partition " + partitionDecorator + " has not been initialized");
        }
        partitionData.addColumnSize(columnName, columnSize, sizeIncludesPartitionColumn);
    }

    public Map<String, Long> getColumnSizes() {
        return combinedData.getColumnSizes();
    }

    public boolean hasSizeForColumn(String columnName) {
        return combinedData.hasSizeForColumn(columnName);
    }

    public boolean hasSizeForColumn(String partitionDecorator, String columnName) {
        if (partitionDecorator == null) {
            return combinedData.hasSizeForColumn(columnName);
        }
        if (!partitionDataMap.containsKey(partitionDecorator)) {
            return false;
        }
        return partitionDataMap.get(partitionDecorator).hasSizeForColumn(columnName);
    }

    public void markEmpty(String partitionDecorator) {
        PartitionData partitionData = partitionDataMap.computeIfAbsent(partitionDecorator,
                k -> new PartitionData(partitionDecorator, partitionColumn, 0L, 0L));
        partitionData.setEmpty(true);
    }
}
