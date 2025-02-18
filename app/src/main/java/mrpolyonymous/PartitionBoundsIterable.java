package mrpolyonymous;

/**
 * Iterable over partition bounds, with additional methods to define where the overall bounds begin
 * and ends
 */
interface PartitionBoundsIterable extends Iterable<PartitionBounds> {

    /**
     * Return a String that can be used in SQL, values less than this will go into the special
     * __UNPARTITIONED__ partition
     */
    String getMinUnpartitionedBound();

    /**
     * Return a String that can be used in SQL, values greater than or equal to this will go into the
     * special __UNPARTITIONED__ partition
     */
    String getMaxUnpartitionedBound();
}
