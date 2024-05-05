import os
import os
from snowflake.snowpark import Session

from modin.experimental.core.execution.snowflake.dataframe.dataframe import SnowflakeDataframe
from modin.experimental.core.storage_formats.hdk import DFAlgQueryCompiler

from modin.experimental.core.execution.snowflake.io.singleton import SessionWraper

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
        self.session = SessionWraper(self.con_dict).session
        #self.session = Session.builder.configs(self.con_dict).create()
        self.session.sql("ALTER SESSION SET USE_CACHED_RESULT = FALSE").collect()
        self.session.sql("ALTER WAREHOUSE {} SUSPEND".format(SnowFlakeWarehouseName.get()))
        self.session.sql('ALTER WAREHOUSE {} RESUME IF SUSPENDED;'.format(SnowFlakeWarehouseName.get()))
        self.session.use_warehouse(SnowFlakeWarehouseName.get())

        self._warehouse_name = SnowFlakeWarehouseName.get()
        self.session.use_database(SnowFlakeDatabaseName.get())

    def from_sf_table(
            self,
            tablename: str,
            session: Session = None
    ) -> DFAlgQueryCompiler:

        if session is None:
            self.make_connection(self)
        else:
            self.session = session
            self.session.use_warehouse(SnowFlakeWarehouseName.get())
            self._warehouse_name = SnowFlakeWarehouseName.get()
            self.session.use_database(SnowFlakeDatabaseName.get())
        snowflake_table = self.session.table(tablename)
        snowflake_frame = SnowflakeDataframe(frame=snowflake_table, sf_session= self.session)
        return self.query_compiler_cls(snowflake_frame, shape_hint=None)