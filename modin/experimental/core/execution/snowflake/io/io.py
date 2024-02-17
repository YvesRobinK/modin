import os
from snowflake.snowpark import Session

from modin.experimental.core.execution.snowflake.dataframe.dataframe import SnowflakeDataframe
from modin.experimental.core.storage_formats.hdk import DFAlgQueryCompiler


from modin.config.envvars import SnowFlakeConnectionParameters, SnowFlakeDatabaseName


class SnowflakeIO():
    frame_cls = SnowflakeDataframe
    query_compiler_cls = DFAlgQueryCompiler
    def make_connection(
            self
    ) -> Session:

        try:
            if SnowFlakeConnectionParameters.get() == None:
                raise Exception("Missing Snowflake connection dict")
            if SnowFlakeDatabaseName.get() == None:
                raise Exception("Missing Snowflake table name")
        except:
            if SnowFlakeConnectionParameters.get() == None:
                raise Exception("Missing Snowflake connection dict")
            if SnowFlakeDatabaseName.get() == None:
                raise Exception("Missing Snowflake table name")

        self.con_dict = SnowFlakeConnectionParameters.get()
        self.session = Session.builder.configs(self.con_dict).create()
        self.tablename = SnowFlakeDatabaseName.get()
        self.session.use_database(self.tablename)

    def from_sf_table(
            self,
            tablename: str
    ) -> DFAlgQueryCompiler:

        self.make_connection(self)

        snowflake_table = self.session.table(tablename)
        snowflake_frame = SnowflakeDataframe(snowflake_table)

        return self.query_compiler_cls(snowflake_frame, shape_hint=None)