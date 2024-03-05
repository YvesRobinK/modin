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
from snowflake.snowpark.types import LongType, StringType, DecimalType, _NumericType, DataType
from snowflake.snowpark.functions import col

from pandas import Series

OPERATORS = ['*', '-', '+', '/']

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
    elif isinstance(sf_type, DataType):
        return numpy.dtype('float64')


NON_NUMERIC_DTYPES = [numpy.dtype('O')]

#class SnowflakeDataframe(PandasDataframe):
class SnowflakeDataframe():

    _query_compiler_cls = DFAlgQueryCompiler


    def __init__(
            self,
            sf_table: table = None,
            sf_base: table = None,
            op: DFAlgNode = None
    ):
        self._partitions: table = sf_table
        self._op = op
        self._base_partition = sf_base
        self._shape_hint = "row"
        self.columns = sf_table.columns

        self._sf_types = []
        self.schema = sf_table.schema
        for col in self.schema:
            self._sf_types.append(col.datatype)

        dtypes = [_map_to_dtypes(x) for x in self._sf_types]

        self.dtypes = Series([x for x in dtypes if x not in NON_NUMERIC_DTYPES ])

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

        schema = self._partitions.schema
        command_dict = {}
        for col in schema:
            if isinstance(col.datatype, _NumericType):
                command_dict[col.name] = agg
        new_partitions = self._partitions.agg(command_dict)

        return SnowflakeDataframe(
            sf_table=new_partitions,
            sf_base=self._base_partition
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
        if not (col_labels is None):
            row_op_list = []
            for item in col_labels:
                for op in OPERATORS:
                    if op in item:
                        row_op_list.append((item.split(' ')[0], item.split(' ')[1] ,item.split(' ')[-1]))

            command_string = 'self._partitions.select_expr('
            for item in col_labels:
                command_string += '"' + str(item) + '", '
            command_string = command_string[:-2] + ')'
            new_frame = eval(command_string)
            return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition)

        if not (row_positions is None):
            #self._partitions.with_columne_renamed(row_positions._query_compiler._modin_frame._partitions.col("P_SIZE < 20"), "index")
            #return SnowflakeDataframe(sf_table=self._partitions.select(row_positions._query_compiler._modin_frame._partitions.col("P_SIZE < 20")))
            #print("Row positions columns:", str(row_positions._query_compiler._modin_frame.columns))
            col_name = row_positions._query_compiler._modin_frame.columns[0]
            com_string = 'self._partitions.filter(' + col_name + ')'
            res = eval(str(com_string))
            #return SnowflakeDataframe(sf_table=self._partitions.filter(str(row_positions._query_compiler._modin_frame._partitions.columns[0])))
            return SnowflakeDataframe(sf_table=res,sf_base=self._partitions)
        return self


    def to_pandas(
            self
    ):
        from modin.pandas.dataframe import DataFrame
        return modin.pandas.io.from_pandas(self._partitions.to_pandas())

    def copy(
            self
    ):
        return SnowflakeDataframe(sf_table=self._partitions, sf_base=self._base_partition)


    def bin_op(
            self,
            other,
            op_name,
            **kwargs
    ):
        print("OPNAME:  ", str(op_name))
        if op_name == "eq":
            column_name = self.columns[0]
            expr_string = 'self._partitions.select_expr("' + column_name + ' == ' + str(other) + '")'

            new_table = eval(str(expr_string))
            new_table = self._partitions.select(col(column_name) == str(other))

            return SnowflakeDataframe(sf_table=new_table, sf_base=self._base_partition)

        if op_name == "lt":
            column_name = self.columns[0]

            expr_string = 'self._partitions.select_expr("' + column_name + ' < ' + str(other) + '")'
            new_table = eval(str(expr_string))
            return SnowflakeDataframe(sf_table=new_table, sf_base=self._base_partition)

        if op_name == "gt":
            column_name = self.columns[0]

            expr_string = 'self._partitions.select_expr("' + column_name + ' > ' + str(other) + '")'
            new_table = eval(str(expr_string))
            return SnowflakeDataframe(sf_table=new_table, sf_base=self._base_partition)

        if op_name == "mul":

            col_name_self = self.columns[0]
            col_name_other = other.columns[0]
            command_string = col_name_self + " * " + col_name_other
            temp_frame = self._base_partition.select(col(col_name_self) * col(col_name_other))
            return SnowflakeDataframe(sf_table=temp_frame, sf_base=self._base_partition)
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

    def _set_columns(
            self,
            new_columns
    ):
        rename_dict = {}
        for index in range(len(self.columns)):
            rename_dict[self.columns[index]] = new_columns[index]
        self._partitions.rename(rename_dict)
        return self

    def join(
            self,
            other,
            on
    ):
        left_key = "self._partitions." + on.split(' ')[0]
        right_key = "other._modin_frame._partitions." + on.split(' ')[len(on.split(' '))-1]
        eval_str = 'self._partitions.join(other._modin_frame._partitions, ' + left_key + ' == ' + right_key + ")"
        joined_frame = eval(eval_str)
        return SnowflakeDataframe(sf_table=joined_frame, sf_base=joined_frame)

    def get_index_names(self):
        #return self.columns
        return ["just_a_dummy_in_order_to_skip_machanism_inb_upper_layers"]


    def groupby_agg(
            self,
            by,
            axis,
            op,
            groupby_args,
            **kwargs
    ):
        aggregator = ""
        if op == "sum":
            aggregator = "sum"

        diff = list(set(self.columns) - set(by.columns))
        print("DIFF: <<<<<<<", str(diff))
        new_frame = self._partitions.group_by(by.columns).function(aggregator)(diff[0])
        return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition)

    def sort_rows(
            self,
            columns,
            ascending,
            ignore_index,
            na_position

    ):
        print("COLUMNS +++++++++", str(columns))
        command_string = 'self._partitions.sort('
        for item in columns:
            for item2 in self.columns:
                if item in item2:
                    command_string += 'col(' + str(item2) + '),'
        command_string = command_string[:-1] + ')'
        print("COMAND STRING: ", str(command_string))
        new_frame = eval(command_string)
        return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition)
