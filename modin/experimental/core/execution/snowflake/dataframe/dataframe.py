from typing import Optional, List, Hashable

import numpy
import pandas
import snowflake.snowpark.types

import modin.pandas.io
from modin.core.dataframe.pandas.dataframe.dataframe import PandasDataframe
from modin.core.dataframe.pandas.metadata import ModinDtypes
from modin.experimental.core.execution.native.implementations.hdk_on_native.df_algebra import GroupbyAggNode, DFAlgNode

from modin.experimental.core.storage_formats.hdk import DFAlgQueryCompiler


from snowflake.snowpark import table
from snowflake.snowpark.types import LongType, StringType, DecimalType, _NumericType


from pandas import Series

def _map_to_dtypes(
        sf_type
):
    if isinstance(sf_type, LongType):
        #return numpy.uint64
        #return numpy.dtypes.Int64DType
        return numpy.dtype('int64')
    elif isinstance(sf_type, StringType):
        return numpy.dtype('O')
    elif isinstance(sf_type, DecimalType):
        return numpy.dtype('float64')


NON_NUMERIC_DTYPES = [numpy.dtype('O')]

#class SnowflakeDataframe(PandasDataframe):
class SnowflakeDataframe():

    _query_compiler_cls = DFAlgQueryCompiler


    def __init__(
            self,
            sf_table: table = None,
            op: DFAlgNode = None
    ):
        self._partitions: table = sf_table
        print("Type of _partitions: ", str(type(sf_table)))
        self._op = op

        self._shape_hint = "row"
        self.columns = sf_table.columns

        self._sf_types = []
        self.schema =sf_table.schema
        for col in self.schema:
            self._sf_types.append(col.datatype)
        print("Sf_types: ", str(self._sf_types))
        dtypes = [_map_to_dtypes(x) for x in self._sf_types]

        self.dtypes = Series([x for x in dtypes if x not in NON_NUMERIC_DTYPES ])
        print("Series dtype: ", str(dtypes))
        self.index = self.columns

    """
    def __new__(cls,
                sf_table: table = None,
                op: DFAlgNode = None,
                *args,
                **kwargs):
        cls._partitions: table = sf_table
        print("Type of _partitions: ", str(type(sf_table)))
        cls._op = op

        cls._shape_hint = "column"
        cls.columns = sf_table.columns

        cls._sf_types = []
        for col in sf_table.schema:
            cls._sf_types.append(col.datatype)
        print("Sf_types: ", str(cls._sf_types))
        dtypes = [_map_to_dtypes(x) for x in cls._sf_types]

        cls.dtypes = Series([x for x in dtypes if x not in NON_NUMERIC_DTYPES])
        print("Series dtype: ", str(dtypes))
    """



    def groupby_agg(
            self,
            by: DFAlgQueryCompiler,
            axis: int,
            agg: dict,
            groupby_args: dict,
            **kwargs: dict[dict]
    ):
        pass

    def agg(
            self,
            agg: str
    ):
        if agg == "mean":
            print('reached mean')

        agg_dict = {}
        schema = self._partitions.schema
        command_dict = {}
        for col in schema:
            if isinstance(col.datatype, _NumericType):
                command_dict[col.name] = agg
        new_partitions = self._partitions.agg(command_dict)

        return SnowflakeDataframe(
            sf_table=new_partitions
        )
        """
        return self.__constructor__(
            sf_table=new_partitions,
            op=GroupbyAggNode(self, [], {"sort": False})
        )
        """

    @property
    def _has_unsupported_data(
            self
    ):
        return False

    """
    def to_pandas(
            self
    ): return self._partitions.to_pandas()
    """
    def __constructor__(self):
        """
        Create a new instance of this object.

        Returns
        -------
        PandasDataframe
        """
        return type(self)

    def _set_index(self):
        pass


    def take_2d_labels_or_positional(
            self,
            key= None,
            row_labels: Optional[List[Hashable]] = None,
            row_positions: Optional[List[int]] = None,
            col_labels: Optional[List[Hashable]] = None,
            col_positions: Optional[List[int]] = None
    ):
        print("col_labels:   ", str(col_labels))
        print("row_positions:   ", str(row_positions))
        if not (col_labels is None):
            return SnowflakeDataframe(sf_table=self._partitions.select(col_labels))

        if not (row_positions is None):
            #self._partitions.with_columne_renamed(row_positions._query_compiler._modin_frame._partitions.col("P_SIZE < 20"), "index")
            print("We going here")
            print("Row_position DF :", str(row_positions._query_compiler._modin_frame._partitions.columns[0]))
            #return SnowflakeDataframe(sf_table=self._partitions.select(row_positions._query_compiler._modin_frame._partitions.col("P_SIZE < 20")))
            res = eval('self._partitions.filter("P_SIZE < 20")')
            #return SnowflakeDataframe(sf_table=self._partitions.filter(str(row_positions._query_compiler._modin_frame._partitions.columns[0])))
            return SnowflakeDataframe(sf_table=res)
        return self


    def to_pandas(
            self
    ):
        from modin.pandas.dataframe import DataFrame
        return modin.pandas.io.from_pandas(self._partitions.to_pandas())

    def filter(
            self,
            key
    ):
        print("This is the key: ", str(key), " This is the type:", str(type(key)))
        self._partitions.filter(key)


    def copy(
            self
    ):

        return SnowflakeDataframe(sf_table= self._partitions)


    def bin_op(
            self,
            other,
            op_name,
            **kwargs
    ):
        print("op_name: ", str(op_name))
        print("Columns:", str(self.columns))
        if op_name == "lt":
            expression = self._expr_build(self.columns, "lt", other)
            expression = 'self._partitions.select_expr("P_SIZE < 20")'
            print("Expression:  " , str(expression))
            new_table = eval(str(expression))
            return SnowflakeDataframe(sf_table=new_table)
        return None

    def has_multiindex(
            self
    ):
        return True if len(self.columns) > 1 else False

    def _expr_build(
            self,
            columns,
            op,
            other
    ):
        if op == "lt":
            res = []
            for item in columns:
                res.append("'" + item + " < " + str(other) + "'")
            return res
        return None