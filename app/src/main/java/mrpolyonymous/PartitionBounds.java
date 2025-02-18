package mrpolyonymous;

/**
 * Info about a partition for use with SQL queries.
 * 
 * @param minValueSql
 *        the lower bound (inclusive) on the partition, suitable for insertion in a SQL comparison
 * @param maxValueSql
 *        the upper bound (exclusive) on the partition, suitable for insertion in a SQL comparison
 * @param partitionDecorator
 *        the partition decorator (Google's term) that identifies the partition
 */
public record PartitionBounds(String minValueSql, String maxValueSql, String partitionDecorator) {

}
