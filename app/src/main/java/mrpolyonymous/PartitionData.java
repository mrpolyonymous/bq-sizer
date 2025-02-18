package mrpolyonymous;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Size info discovered about a partition. Can also be about the whole table when a table is not
 * partitioned.
 */
public class PartitionData {
    private final String partitionDecorator;
    private final String partitionColumn;
    private final long partitionSize;
    private final long partitionColumnSize;
    /**
     * Size of columns within partition
     */
    private final ConcurrentHashMap<String, Long> columnSizes;

    /**
     * Convenience flag to make output report more succinct
     */
    private boolean isEmpty;

    public PartitionData(String partitionDecorator,
            String partitionColumn,
            long partitionSize,
            long partitionColumnSize) {
        this.partitionDecorator = partitionDecorator;
        this.partitionColumn = partitionColumn;
        this.partitionSize = partitionSize;
        this.partitionColumnSize = partitionColumnSize;
        this.columnSizes = new ConcurrentHashMap<>();
        if (partitionColumn != null && !partitionColumn.equals(TableSizer.PARTITIONTIME)) {
            // track partition column size in map automatically if it's not a synthetic column
            columnSizes.put(partitionColumn, partitionColumnSize);
        }
    }

    public void addColumnSize(String columnName, long columnSize, boolean sizeIncludesPartitionColumn) {
        long correctedColumnSize = columnSize;
        if (sizeIncludesPartitionColumn) {
            correctedColumnSize = correctedColumnSize - partitionColumnSize;
        }
        if (correctedColumnSize < 0) {
            // Have only seen this during dev/debuggin but it proved useful
            System.out.format(
                    "WARNING: partition %s column %s has negative size after subtracting partition column %s size\n",
                    partitionDecorator,
                    columnName,
                    partitionColumn);
            correctedColumnSize = 0;
        }
        columnSizes.put(columnName, correctedColumnSize);
    }

    public long getPartitionSize() {
        return partitionSize;
    }

    public Map<String, Long> getColumnSizes() {
        return columnSizes;
    }

    public boolean hasSizeForColumn(String columnName) {
        return columnSizes.containsKey(columnName);
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public void setEmpty(boolean isEmpty) {
        this.isEmpty = isEmpty;
    }
}
