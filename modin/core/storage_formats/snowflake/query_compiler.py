

import abc
from modin.core.storage_formats.base import BaseQueryCompiler
from modin.core.execution.snowflake.dataframe import SnowflakeDataframe

from snowflake.snowpark import Session


import os


class SnowflakeQueryCompiler(BaseQueryCompiler, abc.ABC):
    """
    Class that handles queries to Snowflake dataframes
    """

    def __init__(self, snowflake_frame: SnowflakeDataframe):
        self._snowflake_frame = snowflake_frame

    def from_table(self,
                   table_name: str,
                   database_name:str
                   ):
        _snowflake_session = Session.builder.configs(os.environ['SNOWFLAKE_CON_DICT']).create()
        _snowflake_session.use_database(database_name)
        _snowflake_frame = _snowflake_session.table(table_name)

        self._snowflake_frame = SnowflakeDataframe(_snowflake_frame, _snowflake_session)




