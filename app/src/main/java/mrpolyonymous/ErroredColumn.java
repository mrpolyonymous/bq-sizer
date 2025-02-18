package mrpolyonymous;

import com.google.cloud.bigquery.TableId;

/**
 * In case there was an error fetching data about a column
 */
record ErroredColumn(TableId tableId, String column) {
}
