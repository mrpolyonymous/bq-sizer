package mrpolyonymous;

/**
 * <pre>
 * gsutil cp tablesize-partition-analysis-2023-12-06.json.gz gs://my-bucket/
 * 
 * CREATE TABLE myproject.mydataset.column_data (
 * result_time TIMESTAMP,
 * project STRING,
 * dataset STRING,
 * table STRING,
 * column_name STRING,
 * column_size INT64,
 * partition_decorator STRING
 * );
 * 
 * LOAD DATA INTO myproject.mydataset.column_data(
 * result_time TIMESTAMP,
 * project STRING,
 * dataset STRING,
 * table STRING,
 * column_name STRING,
 * column_size INT64,
 * partition_decorator STRING
 * )
 * FROM FILES (
 *   format='JSON',
 *   compression='GZIP',
 *   uris = ['gs://my-bucket/tablesize-partition-analysis-2023-12-06.json.gz']
 * 
 * </pre>
 */
public class LoadCommands {

}
