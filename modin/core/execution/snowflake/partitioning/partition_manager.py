
from modin.core.execution.snowflake.partitioning.partition import SnowflakePartition


class SnowflakePartitionManager():
    _partition_class = SnowflakePartition