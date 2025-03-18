# BigQuery Table and Column Size Estimator

A program to estimate the number of bytes stored in a BigQuery table,
estimating the logical size of each column in the table and optionally
estimating the logical size of each column in each partition in partitioned tables.

## Why?

BigQuery's table metadata will tell you how big a table is in terms of number of bytes stored,
but it won't tell you how many logical bytes a column occupies. This data is also not currently
captured in any `INFORMATION_SCHEMA` view. Sometimes you want to know how much data is
in each column to see how your data is distributed.

## How it works

**The estimator never reads actual data from BigQuery**. Instead, it uses,
or perhaps abuses, the query estimation available from a dry run.

At the most basic level, the program gets the table schema, executes a
dry run query against every column in the table, taking the estimated
bytes processed from the result to make its estimate:

```sql
SELECT column_name FROM myproject.mydataset.mytable
```

It's more complicated for partitioned tables but the general idea is the same.

## Running

A default GCP project and application default credentials must be available.
If running outside of GCP this typically means having the Google Cloud SDK/gcloud CLI installed,
and set the default credentials by running

```sh
gcloud auth login --update-adc
```

A Java Development Kit (JDK) version 21 or later is required to compile and run
the program. These can be installed by your operating system's package manager,
or free versions can be downloaded from [Adoptium](https://adoptium.net/).

The gradle wrapper is used to build the application and install it. The instructions
below show a default installation but the [gradle application plugin](https://docs.gradle.org/current/userguide/application_plugin.html)
has many more options that can be used to install the application in a more
convenient location.

```sh
# only required if you downloaded a JDK and unzipped it without installing via a package manager
export JAVA_HOME=/path/to/jdk

# Run the gradle wrapper to build the application
./gradlew clean build installDist

# Run the application
app/build/install/app/bin/app --tables=bigquery-public-data.samples.shakespeare
```

```text
bigquery-public-data.samples.shakespeare: 6,432,064 bytes over 164,656 rows, 39 bytes/row
Getting result for bigquery-public-data.samples.shakespeare:word
Getting result for bigquery-public-data.samples.shakespeare:word_count
Getting result for bigquery-public-data.samples.shakespeare:corpus
Getting result for bigquery-public-data.samples.shakespeare:corpus_date

Retrieving sizes took 1,533ms
Sizes for table bigquery-public-data.samples.shakespeare
Column      corpus: 2,464,625 bytes, avg 15.0 bytes/row
Column corpus_date: 1,317,248 bytes, avg 8.0 bytes/row
Column        word: 1,332,943 bytes, avg 8.1 bytes/row
Column  word_count: 1,317,248 bytes, avg 8.0 bytes/row
Total size from columns: 6,432,064 bytes
Total size from metadata: 6,432,064 bytes
```

### Command line options

All command line options are optional. If no arguments are specifed all tables
in all datasets in the default project are scanned.

| Option | Description |
| ------ | ----------- |
| --jobProject=arg | GCP project in which to run size estimation query jobs. If not specified, jobs are run the the default project set for the gcloud CLI. |
| --project=arg | Project to scan for datasets and tables |
| --datasets=arg | Comma-separated list of datasets to scan. Can either be of form `project.dataset`, or `dataset` in which case the scan project (`--project` option) is used to identify the project |
| --datasetPrefix=arg | Only scan tables in datasets with name beginning with this prefix |
| --tables=arg |  Comma-separated list of BigQuery tables to size. Can be fully-qualifed `project.dataset.table` format, `dataset.table` format in which case the `--project` option is required, or just `table`, in which case all datasets are scanned for that table name |
| --sizePartitions | Find column sizes for individual partitions as well as the entire table |
| --minPartitionTime=arg | When sizing individual time-based partitions, the earliest partition (inclusive) to size. Can be an ISO 8601 date and time like `2023-12-31T00:00:00Z` or just the date part like `2023-12-31`. Times that do not match the partition boundary exactly are truncated to the appropriate boundary. |
| --maxPartitionTime=arg | When sizing individual time-based partitions, the latest partition (exclusive) to size. Same syntax as `--minPartitionTime`. |
| --maxTables=arg | Maximum number of tables to size. Use this to speed up the sizing process when testing. Default is to size all tables found |
| --maxColumns=arg | Maximum number of columns to size per table. Use this to speed up the sizing process when testing. Default is to size all columns |
| --maxThreads=arg | Maximum threads/simultaneous queries to run. Defaults to number of available CPU cores. More threads is typically faster but too many threads may run into issues with query quota limits. |
| --noOutput | Skip saving output to files |
| --outputPrefix=arg | Prefix to give to output files. Default is `tablesize` |
| -h,--help | Show help |
| --ignore=arg | Ignore value. When you want to keep arguments around for later but don't want them used. E.g. `--ignore=project=bigquery-public-data` |

### Example usage

Example usage: get size of all tables and columns in the project `myproject`

```sh
app/build/install/app/bin/app --project=myproject
```

Example usage: get size of all tables and columns in the datasets `dataset1` and `dataset2` in the default project 

```sh
app/build/install/app/bin/app --datasets=dataset1,dataset2
```

Example usage: get size of all tables named `importantdata` found in the datasets with name starting `customer`
in the project `production`, limiting to the first 5 tables found and first 10 columns in each table to make sure the
correct tables are being scanned before committing to a lengthy scan process

```sh
app/build/install/app/bin/app --project=production --datasetPrefix=customer --tables=importantdata --maxTables=5 --maxColumns=10
```

Example usage: get size of all columns in the table named `myproject.mydataset.orders` in each partition between
January 1 2023 (inclusive) and January 1 2024 (exclusive), and set the output file prefix to `orders`.

```sh
app/build/install/app/bin/app --tables=myproject.mydataset.orders --sizePartitions --minPartitionTime=2023-01-01 --maxPartitionTime=2024-01-01 --outputPrefix=orders
```

## Results Output

Unless the `--noOutput` option is specified the results of the sizing process are saved to files in newline-delimited JSON, and gzip compressed. The files will have the following names:

1. `tablesize`-table-analysis-`timestamp`.json.gz
2. `tablesize`-column-analysis-`timestamp`.json.gz
2. `tablesize`-partitions-analysis-`timestamp`.json.gz, only created when the `--sizePartitions` option is specified

The `timestamp` will be of the form year-month-day-hour-minute-second. The `tablesize` prefix in the name can be changed 
by specifying the `--outputPrefix` option

### Output schema

The output files conform to a schema that can easily be imported back into BigQuery for analysis and visualization.

```sql
-- change project, dataset and table name as appropriate
CREATE TABLE IF NOT EXISTS myproject.mydataset.column_data (
  result_time TIMESTAMP OPTIONS(description="Time at which results were discovered"),
  project STRING OPTIONS(description="GCP project ID that contained the BigQuery dataset"),
  dataset STRING OPTIONS(description="BigQuery dataset that contained the table"),
  `table` STRING OPTIONS(description="Name of table in which column resides"),
  column_name STRING OPTIONS(description="The name of the column"),
  column_size INT64 OPTIONS(description="The size of the column within the given partition"),
  partition_decorator STRING OPTIONS(description="Partition decorator. NULL if the size is for the entire table and not a partition. Could also be the special values '__NULL__' if the partition column's value is NULL or '__UNPARTITIONED__' for data that falls outside valid partition bounds")
) OPTIONS(description="Estimated size of columns in tables, both partitioned and otherwise");

-- optionally create a view for data that really does fall into a partition.
-- view definition assumes partitions are by date, the definition will need to be
-- changed if you have different partitioning
CREATE VIEW IF NOT EXISTS myproject.mydataset.partition_size_view AS
SELECT
  PARSE_DATE("%Y%m%d", partition_decorator) partition_date,
  column_name,
  column_size,
  project,
  dataset,
  `table`
FROM myproject.mydataset.column_data
WHERE partition_decorator IS NOT NULL AND partition_decorator != "__NULL__" AND partition_decorator != "__UNPARTITIONED__"
```

Upload output data file to a Cloud Storage bucket via a shell:

```sh
# change bucket and file names as appropriate
gcloud storage cp tablesize-partition-analysis-2025-01-31-12-00-00.json.gz gs://my-bucket/
```

Load the data file into the table in BigQuery via SQL commands:

```sql
-- change project, dataset, table and bucket names as appropriate
LOAD DATA INTO myproject.mydataset.column_data(
 result_time TIMESTAMP,
 project STRING,
 dataset STRING,
 `table` STRING,
 column_name STRING,
 column_size INT64,
 partition_decorator STRING
 )
 FROM FILES (
   format='JSON',
   compression='GZIP',
   uris = ['gs://my-bucket/tablesize-partition-analysis-2025-01-31-12-00-00.json.gz']
 )
```

## Accuracy of results

In a perfect world the sum of all column sizes estimated in a table would
add up to the reported number of logical bytes occupied by the table. It's not
that simple, of course, and size estimates may not match up to the exact size
of the table, or report data existing in partitions where no data really exists
for different reasons:

* Streaming inserts keep data in a streaming buffer which isn't yet counted
against the table's logical size but would be measured in dry run queries
* Data can be loaded, inserted, updated or deleted while the size estimation
is being performed. The size estimation can take a while on tables with many
columns or partitions and change size significantly while measurements are happening
* Specifying the `--maxColumns` option can cause columns to be missed in the sizing process
* Some tables are just like that. For example, `bigquery-public-data.crypto_bitcoin.blocks`
is partitioned by day but only has data in partitions for the first day of each month. A query like
`SELECT timestamp_month FROM bigquery-public-data.crypto_bitcoin.blocks WHERE timestamp_month="2023-12-03"` estimates 312 bytes will be processed, yet running the query will return zero results.
Without actually reading data from tables the size estimation process cannot know
for sure if data really exists.

## GCP Permissions required

The ability to get table metadata and the ability to run SQL queries against each
table being sized is always required. Generally speaking this means `BigQuery Data Viewer`
role (or more powerful) is required at the project, dataset or table level.

The queries are always executed against the same GCP project, which is either the
default project or can be set via the `--jobProject` command line option.
`BigQuery Job User` (or better) role is required in this project.

Some of the command line options let the estimator discover tables to run against.
When using these options, permissions to list datasets and permission to list tables
in a dataset will be required.

## Costs

Running the size estimator should not incur any costs in GCP. At the time of writing, in 2025,
performing a dry run is free. No data is ever actually read from the BigQuery tables so there
will be no measurable network egress charges.

Using command line options that result in listing datasets or tables will count against GCP
quotas for these operations.

## Unsupported

* Does not work on views, external tables, and so on; only works against standard BigQuery native tables
* Columns that are arrays of structs, or `RECORD REPEATED`, are sized in their entirety and not broken
down by the individual fields in the structs.
* Does not support time bounds to size multiple partitions at once; always sizes each partition individually
