package mrpolyonymous;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Generate SQL to create test tables with different partitioning schemes and add some test data to
 * them
 */
public class GenTestTables {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");
    static String PROJECT = "MYPROJECT";
    static String DATASET = "MYDATASET";

    public static void main(String... args) {
        genCreateTable("_PARTITIONDATE", "_PARTITIONDATE", true, null, null);
        genCreateTable("date_PARTITIONTIME", "DATE(_PARTITIONTIME)", true, null, null);
        genCreateTable("date_PARTITIONTIME_exp", "DATE(_PARTITIONTIME)", true, 10L, ChronoUnit.DAYS);
        genCreateTable("date_timestamp", "DATE(timestamp_col)", true, null, null);
        genCreateTable("date_timestamp_exp", "DATE(timestamp_col)", true, 10L, ChronoUnit.DAYS);
        genCreateTable("date_datetime", "DATE(datetime_col)", true, null, null);
        genCreateTable("date_datetime_exp", "DATE(datetime_col)", true, 100L, ChronoUnit.DAYS);
        genCreateTable("date_datetime_trunc", "DATETIME_TRUNC(datetime_col, DAY)", true, null, null);
        genCreateTable("timestamp_trunc_partitiontime", "TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)", true, null, null);
        genCreateTable("timestamp_trunc_hour", "TIMESTAMP_TRUNC(timestamp_col, HOUR)", false, null, null);
        genCreateTable("timestamp_trunc_hour_exp", "TIMESTAMP_TRUNC(timestamp_col, HOUR)", false, 36L, ChronoUnit.HOURS);
        genCreateTable("timestamp_trunc_month", "TIMESTAMP_TRUNC(timestamp_col, MONTH)", false, null, null);
        genCreateTable("timestamp_trunc_month_exp", "TIMESTAMP_TRUNC(timestamp_col, MONTH)", false, 1L, ChronoUnit.MONTHS);
        genCreateTable("date", "date_col", true, null, null);
        genCreateTable("date_trunc", "DATE_TRUNC(date_col, YEAR)", true, null, null);
        genCreateTable("date_trunc_exp", "DATE_TRUNC(date_col, YEAR)", true, 5L, ChronoUnit.YEARS);
        genCreateTable("range", "RANGE_BUCKET(range_col,, GENERATE_ARRAY(0, 100, 10)", true, null, null);
        genCreateTable("range2", "RANGE_BUCKET(range_col,, GENERATE_ARRAY(0, 100, 9)", false, null, null);
    }

    private static void genCreateTable(String byTableName,
            String partitionBy,
            boolean requireFilter,
            Long partitionExpiry,
            ChronoUnit partitionUnits) {

        String tableName = PROJECT + "." + DATASET + ".by_" + byTableName;
        String sql;

        sql = "DROP TABLE IF EXISTS " + tableName + ";";
        System.out.println(sql);

        String partitionExpiryOption = "";
        if (partitionExpiry != null) {
            OffsetDateTime now = OffsetDateTime.now();
            long expirationSeconds = Duration.between(now.minus(partitionExpiry, partitionUnits), now).toSeconds();
            long expirationDays = (expirationSeconds + 86400L - 1L) / 86400L;
            expirationDays = Math.max(1, expirationDays);
            partitionExpiryOption = ", partition_expiry_days=" + expirationDays;
        }

        sql = "CREATE TABLE " + tableName
                + " (id_col STRING, date_col DATE, datetime_col DATETIME, timestamp_col TIMESTAMP, range_col INT64)"
                + " PARTITION BY " + partitionBy + " OPTIONS (require_partition__filter=" + requireFilter
                + partitionExpiryOption + ");";
        System.out.println(sql);

        OffsetDateTime almostNow = OffsetDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        StringBuilder sqlBuf = new StringBuilder("INSERT INTO " + tableName
                + " (id_col, date_col, datetime_col, timestamp_col, range_col)\n" + " VALUES\n");
        addRow("aaa", almostNow, 10, sqlBuf);
        if (partitionExpiry != null) {
            addRow("abab", almostNow.minus(partitionExpiry - 1, partitionUnits), 10, sqlBuf);
            addRow("acac", almostNow.minus(partitionExpiry, partitionUnits), 10, sqlBuf);
            addRow("adad", almostNow.minus(partitionExpiry + 1, partitionUnits), 10, sqlBuf);
        }

        // More values to push the range bounds
        sqlBuf.append(" ('bbbb', '1970-01-01', '1970-01-01T01:01:01', '1970-01-01T01:01:01Z', 0),\n");
        sqlBuf.append(" ('cccc', '1965-01-01', '1965-01-01T01:01:01', '1965-01-01T01:01:01Z', 0),\n");
        sqlBuf.append(" ('dddd', '1960-01-01', '1960-01-01T01:01:01', '1960-01-01T01:01:01Z', 0),\n");
        sqlBuf.append(" ('eeee', '1959-12-31', '1959-12-31T23:59:59', '1959-12-31T23:59:59Z', -1),\n");
        sqlBuf.append(" ('ffff', '2159-12-31', '2159-12-31T23:59:59', '2159-12-31T23:59:59Z', 99),\n");
        sqlBuf.append(" ('gggg', '2160-01-01', '2160-01-01T01:01:01', '2160-01-01T01:01:01Z', 100),\n");
        sqlBuf.append(" ('hhhh', '3160-01-01', '3160-01-01T01:01:01', '3160-01-01T01:01:01Z', 1000),\n");
        sqlBuf.append(" ('nnnn', NULL, NULL, NULL, NULL)\n");
        sqlBuf.append(";\n");
        System.out.println(sqlBuf);
    }

    static void addRow(String id, OffsetDateTime odt, int rangeVal, StringBuilder sql) {
        sql.append(" ('")
                .append(id)
                .append("', '")
                .append(odt.toLocalDate())
                .append("', '")
                .append(dateTimeFormatter.format(odt))
                .append("', '")
                .append(odt.toInstant())
                .append("', ")
                .append(rangeVal)
                .append("),\n");;
    }
}
