package mrpolyonymous;

import static org.junit.jupiter.api.Assertions.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.LegacySQLTypeName;

public class TimePartitionBoundsTest {

    @Test
    public void testBasicDate() {
        OffsetDateTime minDate = OffsetDateTime.of(2020,  1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime maxDate = minDate.plus(1, ChronoUnit.YEARS);
        TimeBoundsIterable bounds = new TimeBoundsIterable(false, minDate, maxDate, ChronoUnit.DAYS, LegacySQLTypeName.DATE);
        
        Iterator<PartitionBounds> it = bounds.iterator();
        assertNotNull(it);
        
        PartitionBounds first = it.next();
        assertEquals("'2020-01-01'", first.minValueSql());
        assertEquals("'2020-01-02'", first.maxValueSql());
        assertEquals("20200101", first.partitionDecorator());
        
        int count = 1;
        PartitionBounds last = first;
        while (it.hasNext()) {
            last = it.next();
            ++count;
        }
        assertEquals(366, count); // 2020 was a leap year
        assertEquals("'2020-12-31'", last.minValueSql());
        assertEquals("'2021-01-01'", last.maxValueSql());
        assertEquals("20201231", last.partitionDecorator());
    }

    @Test
    public void testBasicMonth() {
        OffsetDateTime minDate = OffsetDateTime.of(2020,  1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime maxDate = minDate.plus(10, ChronoUnit.YEARS);
        TimeBoundsIterable bounds = new TimeBoundsIterable(false, minDate, maxDate, ChronoUnit.MONTHS, LegacySQLTypeName.DATE);
        
        Iterator<PartitionBounds> it = bounds.iterator();
        assertNotNull(it);
        
        PartitionBounds first = it.next();
        assertEquals("'2020-01-01'", first.minValueSql());
        assertEquals("'2020-02-01'", first.maxValueSql());
        assertEquals("202001", first.partitionDecorator());
        
        int count = 1;
        PartitionBounds last = first;
        while (it.hasNext()) {
            last = it.next();
            ++count;
        }
        assertEquals(10*12, count);
        assertEquals("'2029-12-01'", last.minValueSql());
        assertEquals("'2030-01-01'", last.maxValueSql());
        assertEquals("202912", last.partitionDecorator());
    }

    @Test
    public void testWithRefinedIntervals() {
        OffsetDateTime minDate = OffsetDateTime.of(2020,  1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime maxDate = minDate.plus(10, ChronoUnit.YEARS);
        TimeBoundsIterable bounds = new TimeBoundsIterable(false, minDate, maxDate, ChronoUnit.DAYS, LegacySQLTypeName.TIMESTAMP);
        
        TimeInterval i1 = new TimeInterval(minDate, minDate.plusDays(1));
        TimeInterval i2 = new TimeInterval(minDate.plusDays(10), minDate.plusDays(20));
        bounds = bounds.withRefinedIntervals(Arrays.asList(i1, i2));
        
        Iterator<PartitionBounds> it = bounds.iterator();
        assertNotNull(it);
        
        PartitionBounds first = it.next();
        assertEquals("'2020-01-01T00:00:00Z'", first.minValueSql());
        assertEquals("'2020-01-02T00:00:00Z'", first.maxValueSql());
        assertEquals("20200101", first.partitionDecorator());
        
        int count = 1;
        PartitionBounds last = first;
        while (it.hasNext()) {
            last = it.next();
            ++count;
        }
        assertEquals(1+10, count);
        assertEquals("'2020-01-20T00:00:00Z'", last.minValueSql());
        assertEquals("'2020-01-21T00:00:00Z'", last.maxValueSql());
        assertEquals("20200120", last.partitionDecorator());
    }

}
