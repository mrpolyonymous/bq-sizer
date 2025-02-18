package mrpolyonymous;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;

class TableDataFetcher implements Callable<TableData> {

    private final TableSizer tableSizer;
    private final TableId tableId;

    public TableDataFetcher(TableSizer tableSizer, TableId tableId) {
        this.tableSizer = tableSizer;
        this.tableId = tableId;
    }

    @Override
    public TableData call() throws Exception {
        Table table = tableSizer.getTable(tableId);
        if (table == null) {
            System.err.println("Table " + TableSizer.formatTableId(tableId) + " not found");
            return null;
        }
        
        if (table.getDefinition().getType() != TableDefinition.Type.TABLE) {
            System.err.println(TableSizer.formatTableId(tableId) + " cannot be estimated. Size estimating only works for regular tables, not for views or external tables");
            return null;
        }
        
        StandardTableDefinition tableDefinition = table.getDefinition();
        
        long numBytes = table.getNumTotalLogicalBytes();
        // It's weird that rows is stored as a BigInteger, but bytes is stored as a Long,
        // does that mean tables can have more rows than bytes?
        long numRows = table.getNumRows().longValue();
        long avgSize = (numRows == 0) ? 0 : numBytes / numRows;
        
        System.out.format("%s: %,d bytes over %,d rows, %,d bytes/row\n",
                TableSizer.formatTableId(tableId),
                numBytes,
                numRows,
                avgSize
                );
        
        String partitionColumn = getPartitionColumn(tableDefinition);
        Long partitionColumnSize;
        if (numBytes > 0) {
            partitionColumnSize = getPartitionColumnSize(tableDefinition);
        } else {
            partitionColumnSize = 0L;
        }
        PartitionBoundsIterable partitionBounds = getPartitionIterable(tableDefinition);
        boolean partitionFilterRequired = isPartitionFilterRequired(tableDefinition);
        
        TableData tableData = new TableData(tableId,
                numRows,
                numBytes,
                partitionColumn,
                partitionColumnSize,
                partitionFilterRequired,
                partitionBounds,
                computeColumnNames(tableDefinition.getSchema()));
        return tableData;
    }

    String getPartitionColumn(StandardTableDefinition tableDefinition) {
        RangePartitioning rangePartitioning = tableDefinition.getRangePartitioning();
        if (rangePartitioning != null) {
            return rangePartitioning.getField();
        }
        
        TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
        if (timePartitioning != null) {
            String partitionColumn = timePartitioning.getField();
            if (partitionColumn == null) {
                // partitioned by virtual _PARTITIONTIME column which occupies 0 bytes
                return TableSizer.PARTITIONTIME;
            }
            return partitionColumn;
        }
        
        return null;
    }
    
    long getPartitionColumnSize(StandardTableDefinition tableDefinition) {
        RangePartitioning rangePartitioning = tableDefinition.getRangePartitioning();
        if (rangePartitioning != null) {
            // There doesn't seem to be a way to tell if partition filters are
            // required when using range partitioning. At least, the Java objects don't seem to
            // have such a field. Assume a filter is always required.
            String partitionColumn = rangePartitioning.getField();
            return tableSizer.getColumnSize(tableId, partitionColumn, partitionColumn);
        }
        
        TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
        if (timePartitioning != null) {
            String partitionColumn = timePartitioning.getField();
            if (partitionColumn == null) {
                // partitioned by virtual _PARTITIONTIME column which occupies 0 bytes
                return 0L;
            }
            
            String filterColumn = null;
            if (Boolean.TRUE.equals(timePartitioning.getRequirePartitionFilter())) {
                filterColumn = partitionColumn;
            }
            long partitionColumnSize = tableSizer.getColumnSize(tableId, partitionColumn, filterColumn);
            return partitionColumnSize;
        }
        
        return 0L;
        
    }

    Boolean isPartitionFilterRequired(StandardTableDefinition tableDefinition) {
        RangePartitioning rangePartitioning = tableDefinition.getRangePartitioning();
        if (rangePartitioning != null) {
            // There doesn't seem to be a way to tell if partition filters are
            // required when using range partitioning. At least, the Java objects don't seem to
            // have such a field. Assume a filter is always required.
            return true;
        }
        
        TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
        if (timePartitioning != null) {
            return Boolean.TRUE.equals(timePartitioning.getRequirePartitionFilter());
        }
        
        return false;
    }

    PartitionBoundsIterable getPartitionIterable(StandardTableDefinition tableDefinition) {
        RangePartitioning rangePartitioning = tableDefinition.getRangePartitioning();
        if (rangePartitioning != null) {
            return RangeBoundsIterable.from(rangePartitioning);
        }
        
        TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
        if (timePartitioning != null) {
            Field partitionField = getPartitionField(tableDefinition, timePartitioning.getField());
            
            return TimeBoundsIterable.from(timePartitioning, partitionField, tableSizer.getConfig());
        }
        
        return null;
    }
    
    private Field getPartitionField(TableDefinition tableDefinition, String fieldName) {
        if (fieldName == null) {
            return null;
        }
            
        FieldList fieldList = tableDefinition.getSchema().getFields();
        for (Field field : fieldList) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                return field;
            }
        }
        throw new IllegalStateException("Cannot find field " + fieldName + " in table");
    }

    private static List<String> computeColumnNames(Schema schema) {
        List<String> columnNames = new ArrayList<>();
        computeColumnNames(schema.getFields(), "", columnNames);
        return Collections.unmodifiableList(columnNames);
    }

    private static List<String> computeColumnNames(FieldList fieldList, String parentColumn, List<String> columnNames) {
        String parentPrefix = parentColumn.isEmpty() ? parentColumn : parentColumn + ".";
        for (Field field : fieldList) {
            // Cannot refer to a column as parent.child when column is REPEATED RECORD (aka array of struct)
            if (field.getType() == LegacySQLTypeName.RECORD && field.getMode() != Mode.REPEATED) {
                computeColumnNames(field.getSubFields(), parentPrefix + field.getName(), columnNames);
            } else {
                columnNames.add(parentPrefix + field.getName());
            }
        }
        return columnNames;
    }
}
