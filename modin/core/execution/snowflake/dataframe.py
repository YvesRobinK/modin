"""Module houses class that implements ``PandasDataframe``."""

from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowFrame

from modin.core.execution.snowflake.partitioning.partition import SnowflakePartition
#from modin.core.storage_formats.snowflake.query_compiler import SnowflakeQueryCompiler
from modin.core.execution.snowflake.partitioning.partition_manager import SnowflakePartitionManager
class SnowflakeDataframe():

    _partition_mgr_cls = SnowflakePartitionManager
    _partition_cls = SnowflakePartition
    #_query_compiler_cls = SnowflakeQueryCompiler

    def __init__(self,
        sf_session: Session,
        sf_frame: SnowFrame
                 ):
        self._partition = self._partition_cls(sf_frame, sf_session)
