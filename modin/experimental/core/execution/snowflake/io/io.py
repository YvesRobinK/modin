import os
import os
from snowflake.snowpark import Session

from modin.experimental.core.execution.snowflake.dataframe.dataframe import SnowflakeDataframe
from modin.experimental.core.storage_formats.hdk import DFAlgQueryCompiler


from modin.config.envvars import SnowFlakeConnectionParameters, SnowFlakeDatabaseName, SnowFlakeWarehouseName


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
            if SnowFlakeWarehouseName.get() == None:
                raise Exception("Missing Warehouse name")
        except:
            if SnowFlakeConnectionParameters.get() == None:
                raise Exception("Missing Snowflake connection dict")
            if SnowFlakeDatabaseName.get() == None:
                raise Exception("Missing Snowflake table name")
            if SnowFlakeWarehouseName.get() == None:
                raise Exception("Missing Warehouse name")

        self.con_dict = SnowFlakeConnectionParameters.get()
        self.session = Session.builder.configs(self.con_dict).create()
        self.session.use_warehouse(SnowFlakeWarehouseName.get())
        self.tablename = SnowFlakeDatabaseName.get()
        self._warehouse_name = SnowFlakeWarehouseName.get()
        self.session.use_database(self.tablename)

    def from_sf_table(
            self,
            tablename: str
    ) -> DFAlgQueryCompiler:

        self.make_connection(self)
        snowflake_table = self.session.table(tablename)
        snowflake_frame = SnowflakeDataframe(sf_table=snowflake_table, sf_base=snowflake_table)
        return self.query_compiler_cls(snowflake_frame, shape_hint=None)