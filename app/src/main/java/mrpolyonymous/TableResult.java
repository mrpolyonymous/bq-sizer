package mrpolyonymous;

import java.time.Instant;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.cloud.bigquery.TableId;

/**
 * Info about a table as a whole, for serialization purposes
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TableResult {

    /** ISO 8601 date time string */
    private String resultTime;
    private String project;
    private String dataset;
    private String table;
    // String, not long, in case of JSON big integer issues
    private String tableSize;
    private String numRows;

    public TableResult() {
    }

    public TableResult(Instant resultTime, TableId tableId, long tableSize, long numRows) {
        this(resultTime.toString(), tableId.getProject(), tableId.getDataset(), tableId.getTable(), tableSize, numRows);
    }

    public TableResult(String resultTime, String project, String dataset, String table, long tableSize, long numRows) {
        this.resultTime = resultTime;
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.tableSize = String.valueOf(tableSize);
        this.numRows = String.valueOf(numRows);
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

    public String getTableSize() {
        return tableSize;
    }

    public void setTableSize(String tableSize) {
        this.tableSize = tableSize;
    }

    public String getNumRows() {
        return numRows;
    }

    public void setNumRows(String numRows) {
        this.numRows = numRows;
    }

}
