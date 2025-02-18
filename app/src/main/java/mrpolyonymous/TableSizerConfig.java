package mrpolyonymous;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Config settings controlling how the size estimating process will run
 */
record TableSizerConfig(String jobProject,
        int maxTables,
        int maxColumnsPerTable,
        String scanProject,
        List<String> datasetsToScan,
        String matchDatasetPrefix,
        List<String> tablesToScan,
        boolean saveOutput,
        String outputPrefix,
        boolean sizeIndividualPartitions,
        OffsetDateTime minPartitionTime,
        OffsetDateTime maxPartitionTime,
        int maxThreads) {

}
