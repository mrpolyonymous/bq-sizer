package mrpolyonymous;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.RangePartitioning.Range;

public class RangeBoundsIterable implements PartitionBoundsIterable {

    static RangeBoundsIterable from(RangePartitioning rangePartitioning) {
        return new RangeBoundsIterable(rangePartitioning.getRange());
    }

    private final Range range;

    public RangeBoundsIterable(Range range) {
        this.range = range;
    }

    @Override
    public Iterator<PartitionBounds> iterator() {
        return new RangeBoundsIterator();
    }

    class RangeBoundsIterator implements Iterator<PartitionBounds> {
        private Long currentValue;

        RangeBoundsIterator() {
            currentValue = range.getStart();
        }

        @Override
        public boolean hasNext() {
            return currentValue < range.getEnd();
        }

        @Override
        public PartitionBounds next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Long nextValue = currentValue + range.getInterval();

            PartitionBounds bounds = new PartitionBounds(String.valueOf(currentValue),
                    String.valueOf(nextValue),
                    String.valueOf(currentValue));
            currentValue = nextValue;
            return bounds;
        }
    }

    @Override
    public String getMinUnpartitionedBound() {
        return String.valueOf(range.getStart());
    }

    @Override
    public String getMaxUnpartitionedBound() {
        return String.valueOf(range.getEnd());
    }
}
