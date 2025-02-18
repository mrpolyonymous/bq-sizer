package mrpolyonymous;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.annotations.VisibleForTesting;

public class TimeBoundsIterable implements PartitionBoundsIterable {

    // Yes, it really is 1960, not 1970
    static final OffsetDateTime MIN_PARTITION_TIME = OffsetDateTime.of(1960, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    private static final OffsetDateTime MAX_PARTITION_TIME = OffsetDateTime.of(2160, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");

    public static TimeBoundsIterable from(TimePartitioning timePartitioning,
            Field partitionField,
            TableSizerConfig config) {
        boolean isMinDateGuess = false;
        OffsetDateTime minTime;
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        if (config.minPartitionTime() != null) {
            minTime = config.minPartitionTime();
        } else if (timePartitioning.getExpirationMs() == null) {
            // Assume some plausible default lower bound
            minTime = now.minus(4, ChronoUnit.YEARS);
            isMinDateGuess = true;
        } else {
            minTime = now.minus(timePartitioning.getExpirationMs(), ChronoUnit.MILLIS);
        }

        // Put some reasonable upper bound on max time, assuming dates aren't very far in the future
        OffsetDateTime maxTime;
        ChronoUnit partitionUnit;
        switch (timePartitioning.getType()) {
            case HOUR:
                // BigQuery default maximum is 4000 partitions. 4000 hours is about 5.5 months,
                // assume partitions older than that are unlikely to have any data
                if (isMinDateGuess) {
                    minTime = now.minus(4000, ChronoUnit.HOURS);
                }
                partitionUnit = ChronoUnit.HOURS;
                // Not uncommon to see times in the future due to badly configured time zones
                maxTime = now.plus(24, partitionUnit);
                break;
            case MONTH:
                partitionUnit = ChronoUnit.MONTHS;
                maxTime = now.plus(1, partitionUnit);
                break;
            case YEAR:
                partitionUnit = ChronoUnit.YEARS;
                maxTime = now.plus(1, partitionUnit);
                break;
            default:
                partitionUnit = ChronoUnit.DAYS;
                maxTime = now.plus(1, partitionUnit);
                break;
        }
        if (config.maxPartitionTime() != null) {
            // override previous guess at max time
            maxTime = config.maxPartitionTime();
        }

        // This even truncates the time if it was user-specified, which is probably okay
        minTime = truncateTime(minTime, partitionUnit);
        maxTime = truncateTime(maxTime, partitionUnit);

        // partitionField will be null when partitioning by _PARTITIONTIME
        LegacySQLTypeName fieldType = partitionField == null ? LegacySQLTypeName.TIMESTAMP : partitionField.getType();

        return new TimeBoundsIterable(isMinDateGuess, minTime, maxTime, partitionUnit, fieldType);

    }

    /**
     * Truncate a time in a way that will work as exact partition bounds in SQL queries
     */
    private static OffsetDateTime truncateTime(OffsetDateTime odt, ChronoUnit timeUnit) {
        switch (timeUnit) {
            case HOURS:
            case DAYS:
                return odt.truncatedTo(timeUnit);
            case MONTHS:
                return OffsetDateTime
                        .of(LocalDate.of(odt.getYear(), odt.getMonth(), 1), LocalTime.MIDNIGHT, ZoneOffset.UTC);
            case YEARS:
                return OffsetDateTime.of(LocalDate.of(odt.getYear(), 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private final boolean isMinDateGuess;
    private final List<TimeInterval> timeIntervals;
    private final ChronoUnit granularity;
    private final LegacySQLTypeName fieldType;

    @VisibleForTesting
    TimeBoundsIterable(boolean isMinDateGuess,
            OffsetDateTime minDate,
            OffsetDateTime maxDate,
            ChronoUnit granularity,
            LegacySQLTypeName fieldType) {
        this(isMinDateGuess, Collections.singletonList(new TimeInterval(minDate, maxDate)), granularity, fieldType);
    }

    private TimeBoundsIterable(boolean isMinDateGuess,
            List<TimeInterval> timeIntervals,
            ChronoUnit granularity,
            LegacySQLTypeName fieldType) {
        this.isMinDateGuess = isMinDateGuess;
        this.timeIntervals = timeIntervals;
        this.granularity = granularity;
        this.fieldType = fieldType;
    }

    @Override
    public Iterator<PartitionBounds> iterator() {
        return new TimeBoundsIterator();
    }

    class TimeBoundsIterator implements Iterator<PartitionBounds> {
        private Iterator<TimeInterval> intervalIterator;
        private TimeInterval currentInterval;
        private OffsetDateTime currentOffsetInInterval;

        TimeBoundsIterator() {
            intervalIterator = timeIntervals.iterator();
            currentInterval = intervalIterator.next();
            currentOffsetInInterval = currentInterval.min();
        }

        @Override
        public boolean hasNext() {
            return currentOffsetInInterval.isBefore(currentInterval.max()) || intervalIterator.hasNext();
        }

        @Override
        public PartitionBounds next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            OffsetDateTime nextMax = currentOffsetInInterval.plus(1, granularity);
            if (nextMax.isAfter(currentInterval.max())) {
                currentInterval = intervalIterator.next();
                currentOffsetInInterval = currentInterval.min();
                nextMax = currentOffsetInInterval.plus(1, granularity);
            }

            PartitionBounds bounds = new PartitionBounds(formatForSql(currentOffsetInInterval),
                    formatForSql(nextMax),
                    formatPartitionDecorator(currentOffsetInInterval));
            currentOffsetInInterval = nextMax;
            return bounds;
        }
    }

    String formatForSql(OffsetDateTime odt) {
        if (fieldType.equals(LegacySQLTypeName.TIMESTAMP)) {
            return "'" + odt.toInstant() + "'";
        } else if (fieldType.equals(LegacySQLTypeName.DATETIME)) {
            return "'" + DATETIME_FORMATTER.format(odt) + "'";
        } else {
            return "'" + odt.toLocalDate() + "'";
        }
    }

    String formatPartitionDecorator(OffsetDateTime odt) {
        if (granularity == ChronoUnit.HOURS) {
            return String
                    .format("%d%02d%02d%02d", odt.getYear(), odt.getMonthValue(), odt.getDayOfMonth(), odt.getHour());
        } else if (granularity == ChronoUnit.DAYS) {
            return String.format("%d%02d%02d", odt.getYear(), odt.getMonthValue(), odt.getDayOfMonth());
        } else if (granularity == ChronoUnit.MONTHS) {
            return String.format("%d%02d", odt.getYear(), odt.getMonthValue());
        } else if (granularity == ChronoUnit.YEARS) {
            return String.format("%d", odt.getYear());
        }
        return null;
    }

    @Override
    public String getMinUnpartitionedBound() {
        return formatForSql(MIN_PARTITION_TIME);
    }

    @Override
    public String getMaxUnpartitionedBound() {
        return formatForSql(MAX_PARTITION_TIME);
    }

    public OffsetDateTime getMinDate() {
        return timeIntervals.get(0).min();
    }

    public OffsetDateTime getMaxDate() {
        return timeIntervals.get(timeIntervals.size() - 1).max();
    }

    public ChronoUnit getGranularity() {
        return granularity;
    }

    public LegacySQLTypeName getFieldType() {
        return fieldType;
    }

    public boolean isMinDateGuess() {
        return isMinDateGuess;
    }

    public OffsetDateTime midPoint(OffsetDateTime min, OffsetDateTime max) {
        OffsetDateTime mid = min.plusHours(Duration.between(min, max).toHours() / 2);
        return truncateTime(mid, granularity);
    }

    /**
     * Return a new instance over the same field as this instance but with a more accurate list of time
     * intervals that contain data
     */
    public TimeBoundsIterable withRefinedIntervals(List<TimeInterval> refinedIntervals) {
        // TODO: validate that the intervals don't overlap and are increasing
        return new TimeBoundsIterable(false, refinedIntervals, granularity, fieldType);
    }

    public long partitionsBetween(OffsetDateTime min, OffsetDateTime max) {
        if (granularity == ChronoUnit.HOURS) {
            Duration duration = Duration.between(min, max);
            return duration.toHours();
        } else if (granularity == ChronoUnit.DAYS) {
            Duration duration = Duration.between(min, max);
            return duration.toDays();
        } else if (granularity == ChronoUnit.MONTHS) {
            return ChronoUnit.MONTHS.between(YearMonth.from(min.toLocalDate()), YearMonth.from(max.toLocalDate()));
        } else if (granularity == ChronoUnit.YEARS) {
            return ChronoUnit.YEARS.between(Year.from(min.toLocalDate()), Year.from(max.toLocalDate()));
        }
        return 0L;
    }
}
