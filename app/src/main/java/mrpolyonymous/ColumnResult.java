package mrpolyonymous;

import java.time.Instant;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.cloud.bigquery.TableId;

/**
 * Info about a column within a table, for serialization purposes
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ColumnResult {

    /** ISO 8601 date time string */
    private String resultTime;

    private String project;
    private String dataset;
    private String table;
    private String columnName;
    // String, not long, in case of JSON big integer issues
    private String columnSize;

    /**
     * null for size across entire table, non-null for individual partitions
     */
    private String partitionDecorator;

    public ColumnResult() {
        // default values
    }

    public ColumnResult(Instant resultTime, TableId tableId, String columnName, Long columnSize) {
        this(resultTime.toString(),
                tableId.getProject(),
                tableId.getDataset(),
                tableId.getTable(),
                columnName,
                columnSize,
                null);
    }

    public ColumnResult(String resultTime,
            String project,
            String dataset,
            String table,
            String columnName,
            Long columnSize,
            String partitionDecorator) {
        this.resultTime = resultTime;
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.columnName = columnName;
        this.columnSize = columnSize.toString();
        this.partitionDecorator = partitionDecorator;
    }

    public String getResultTime() {
        return resultTime;
    }

    public void setResultTime(String resultTime) {
        this.resultTime = resultTime;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(String columnSize) {
        this.columnSize = columnSize;
    }

    public String getPartitionDecorator() {
        return partitionDecorator;
    }

    public void setPartitionDecorator(String partitionDecorator) {
        this.partitionDecorator = partitionDecorator;
    }

}
