from typing import Optional, List, Hashable

import numpy
import pandas
import snowflake.snowpark.types
from functools import wraps

import modin.pandas.io
from modin.core.dataframe.pandas.dataframe.dataframe import PandasDataframe
from modin.core.dataframe.pandas.metadata import ModinDtypes
import copy

from modin.experimental.core.storage_formats.hdk import DFAlgQueryCompiler


from snowflake.snowpark import table
from snowflake.snowpark.types import LongType, StringType, DecimalType, _NumericType, DataType
from snowflake.snowpark.functions import col

from pandas import Series

from modin.experimental.core.execution.snowflake.dataframe.Frame import Frame
from modin.experimental.core.execution.snowflake.dataframe.operaterNodes import \
    Node, ConstructionNode, SelectionNode, ComparisonNode, VirtualFrame

OPERATORS = ['*', '-', '+', '/']
def track(func):
    @wraps(func)
    def with_logging(*args, **kwargs):
        print(func.__name__ + " was called")
        return func(*args, **kwargs)
    return with_logging

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


class SnowflakeDataframe:
    _query_compiler_cls = DFAlgQueryCompiler

    def __init__(
            self,
            sf_table: table = None,
            sf_base: table = None,
            op= None,
            or_statement: str = None,
            and_statement: str = None,
            sf_session= None,
            virtual_frame= None
    ):
        self._frame: table = Frame(sf_table)
        self._partitions = sf_table
        self._op = op

        self._base_partition = sf_base
        self._shape_hint = "row"
        self.columns = sf_table.columns
        self._sf_session = sf_session
        self._sf_types = []
        self.schema = sf_table.schema
        for col in self.schema:
            self._sf_types.append(col.datatype)

        dtypes = [_map_to_dtypes(x) for x in self._sf_types]

        self.dtypes = Series([x for x in dtypes if x not in NON_NUMERIC_DTYPES ])
        self.or_statement = or_statement
        self.and_statement = and_statement
        self.index = self.columns

        if virtual_frame is None:
            self.virt_frame = VirtualFrame([ConstructionNode(colname) for colname in self.columns])
        else:
            self.virt_frame = virtual_frame

    @track
    def groupby_agg(
            self,
            by: DFAlgQueryCompiler,
            axis: int,
            agg: dict,
            groupby_args: dict,
            **kwargs: dict[dict]
    ):
        pass
    @track
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


    @property
    def _has_unsupported_data(
            self
    ):
        return False

    @track
    def __constructor__(self):
        """
        Create a new instance of this object.

        Returns
        -------
        PandasDataframe
        """
        return type(self)

    @track
    def _set_index(self):
        pass

    @track
    def take_2d_labels_or_positional(
            self,
            key= None,
            row_labels: Optional[List[Hashable]] = None,
            row_positions: Optional[List[int]] = None,
            col_labels: Optional[List[Hashable]] = None,
            col_positions: Optional[List[int]] = None
    ):
        if not (col_labels is None):
            print("COL_ABELS: ", str(col_labels), "COL_LABELS_TYPE_ ", str(type(col_labels)))
            row_op_list = []
            for item in col_labels:
                for op in OPERATORS:
                    if op in item:
                        row_op_list.append((item.split(' ')[0], item.split(' ')[1], item.split(' ')[-1]))
            if len(row_op_list) > 0:
                command_string = 'self._partitions.select_expr('
            else:
                command_string = 'self._partitions.select('
            for item in col_labels:
                command_string += '"' + str(item) + '", '
            command_string = command_string[:-2] + ')'

            new_frame = eval(command_string)
            new_virt_frame = self.virt_frame.from_col_names(col_labels)
            new_virt_frame = new_virt_frame.extend_nodes(SelectionNode)
            return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition, virtual_frame=new_virt_frame)

        if not (row_positions is None):
            print("We here ? ")
            if not (row_positions._query_compiler._modin_frame.or_statement is None):
                print("Hoila")
                com_string = 'self._partitions.filter('+row_positions._query_compiler._modin_frame.or_statement
                res = eval(com_string)
                return SnowflakeDataframe(sf_table=res, sf_base=self._partitions)


            value = row_positions._query_compiler._modin_frame.virt_frame.node_list[0].value
            operator = row_positions._query_compiler._modin_frame.virt_frame.node_list[0].operator
            colname = row_positions._query_compiler._modin_frame.virt_frame.node_list[0].colname

            com_string = f"self._partitions.filter('{colname} {operator} {value}')"
            print(com_string)
            res = eval(str(com_string))

            return SnowflakeDataframe(sf_table=res, sf_base=self._partitions)
        return self

    @track
    def to_pandas(
            self
    ):
        from modin.pandas.dataframe import DataFrame
        return modin.pandas.io.from_pandas(self._partitions.to_pandas())

    @track
    def copy(
            self
    ):
        return SnowflakeDataframe(sf_table=self._partitions, sf_base=self._base_partition)

    @track
    def bin_op(
            self,
            other,
            op_name,
            **kwargs
    ):
        comp_dict ={
            "le": "<=",
            "ge": ">=",
            "eq": "=",
            "lt": "<",
            "gt": ">"
        }
        operator_dict={

        }

        if op_name in comp_dict.keys() and not isinstance(other, self.__class__):
            column_name = self.columns[0]
            new_table = self._partitions.select(col(column_name) == str(other))
            print("New_table", new_table.columns)
            new_virt_frame = copy.deepcopy(self.virt_frame)
            new_virt_frame.node_list = [ComparisonNode(node.colname, comp_dict[op_name], other, node) for node in new_virt_frame.node_list]
            return SnowflakeDataframe(
                sf_table=new_table,
                sf_base=self._base_partition,
                virtual_frame=new_virt_frame
            )

        if op_name in comp_dict.keys():
            col_name_self = self.columns[0]
            col_name_other = other.columns[0]
            command_string = col_name_self + f" {comp_dict[op_name]} " + col_name_other
            temp_frame = self._base_partition.select(col(col_name_self) * col(col_name_other))
            return SnowflakeDataframe(sf_table=temp_frame, sf_base=self._base_partition)
        return None

    @track
    def has_multiindex(
            self
    ):
        return True if len(self.columns) > 1 else False

    @track
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

    @track
    def _set_columns(
            self,
            new_columns
    ):
        rename_dict = {}
        for index in range(len(self.columns)):
            rename_dict[self.columns[index]] = new_columns[index]
        self._partitions.rename(rename_dict)
        return self

    @track
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

    @track
    def get_index_names(self):
        return ["just_a_dummy_in_order_to_skip_mechanism_in_upper_layers"]

    @track
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

        new_frame = self._partitions.group_by(by.columns).function(aggregator)(diff[0])
        return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition)

    @track
    def sort_rows(
            self,
            columns,
            ascending,
            ignore_index,
            na_position

    ):
        command_string = 'self._partitions.sort('
        for item in columns:
            for item2 in self.columns:
                if item in item2:
                    command_string += 'col("' + str(item) + '")'
                    if ascending[0] == False :
                        command_string += ".desc(),"
                    else:
                        command_string += ".asc(),"
                    ascending = ascending[1:]
        command_string = command_string[:-1] + ')'
        new_frame = eval(command_string)
        return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition)

    @track
    def _execute(
            self
    ):
        return self

    @track
    def _or(
            self,
            other
    ):
        left_expr = self.columns[0].replace('"', '').replace("'", "")[1:-1].split(' ')
        right_expr = other.columns[0].replace('"', '').replace("'", "")[1:-1].split(' ')
        if left_expr[1] == "=":
            left_expr[1] = "=="
            right_expr[1] = "=="
        command_string = 'self._partitions.filter((col("' + left_expr[0] + '") ' + str(left_expr[1]) + ' "' + " ".join(left_expr[2:]) + '")'
        command_string += " | "
        command_string = command_string + '(col("' + right_expr[0] + '") ' + str(right_expr[1]) + ' "' + " ".join(right_expr[2:]) + '"))'


        new_frame = eval(command_string)

        or_statement = '(col("' + left_expr[0] + '") ' + str(left_expr[1]) + ' "' + " ".join(left_expr[2:]) + '")'
        or_statement += " | "
        or_statement = or_statement + '(col("' + right_expr[0] + '") ' + str(right_expr[1]) + ' "' + " ".join(
            right_expr[2:]) + '"))'
        return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition, or_statement=or_statement)

    @track
    def _and(
            self,
            other
    ):

        left_expr = self.columns[0].replace('"', '').replace("'", "")[1:-1].split(' ')
        right_expr = other.columns[0].replace('"', '').replace("'", "")[1:-1].split(' ')
        if left_expr[1] == "=":
            left_expr[1] = "=="
            right_expr[1] = "=="
        command_string = 'self._partitions.filter((col("' + left_expr[0] + '") ' + str(left_expr[1]) + ' "' + " ".join(
            left_expr[2:]) + '")'
        command_string += " | "
        command_string = command_string + '(col("' + right_expr[0] + '") ' + str(right_expr[1]) + ' "' + " ".join(
            right_expr[2:]) + '"))'


        new_frame = eval(command_string)

        or_statement = '(col("' + left_expr[0] + '") ' + str(left_expr[1]) + ' "' + " ".join(left_expr[2:]) + '")'
        or_statement += " & "
        or_statement = or_statement + '(col("' + right_expr[0] + '") ' + str(right_expr[1]) + ' "' + " ".join(
            right_expr[2:]) + '"))'
        return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition, or_statement=or_statement)

    @track
    def rename(
            self,
            columns: {str: str} = None
    ):
        command_dict = {}
        for key in columns.keys():
            for df_col in self.columns:
                print("HERE: ", key, "/", df_col)
                if key in df_col:
                    command_dict[col(df_col)] = columns[key]
        new_frame = self._partitions.rename(command_dict)
        return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition)

    @track
    def setitem(self, axis, key, value):
        print("Axis type: ", type(axis))
        print("Key tpye: ", type(key), " , Key: ", key)
        print("Value type: ", type(value._modin_frame._partitions), " , " )
        
        res = self._partitions.join(value._modin_frame._partitions)
        print(res.to_pandas())

        return self
