

from snowflake.snowpark import Session
from snowflake.snowpark.types import LongType
from snowflake.snowpark.functions import mean as sf_mean
from snowflake.snowpark import DataFrame as sf_dataframe
import snowflake

class SnowflakePartition():

    def __init__(self,
                 _sf_session: Session,
                 _sf_table: sf_dataframe):
        self._sf_connection = _sf_session
        self._table_partition = sf_dataframe
