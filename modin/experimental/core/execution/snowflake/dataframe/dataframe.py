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
    Node, ConstructionNode, SelectionNode, ComparisonNode, VirtualFrame, JoinNode, SetIndexNode, FilterNode, RenameNode, \
    LogicalNode, BinOpNode

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
            sf_table=None,
            op_tree=None,
            sf_session=None,
            key_column=None,
            join_index=None
    ):
        if isinstance(sf_table, Frame):
            self._frame = sf_table
        else:
            self._frame = Frame(sf_table)
        self.key_column = key_column
        self._shape_hint = "row"
        self.columns = self._frame._frame.columns
        self._sf_session = sf_session
        self._sf_types = []
        self.schema = self._frame._frame.schema
        self._join_index = join_index
        self.index = self.columns

        if op_tree is None:

            self.op_tree = ConstructionNode(colnames=self.columns)
        else:
            self.op_tree = op_tree
        for col in self.schema:
            self._sf_types.append(col.datatype)

        dtypes = [_map_to_dtypes(x) for x in self._sf_types]
        self.dtypes = Series([x for x in dtypes if x not in NON_NUMERIC_DTYPES])

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
    def set_index(self,
                  index: None
                  ):
        return SnowflakeDataframe(
            sf_table=self._frame,
            sf_session=self._sf_session,
            op_tree=SetIndexNode(
                index=index,
                colnames=self.columns,
                prev=self.op_tree,
                frame= self.op_tree.frame
            ),
            key_column=self.key_column,
            join_index=index
        )

    @track
    def take_2d_labels_or_positional(
            self,
            key=None,
            row_labels: Optional[List[Hashable]] = None,
            row_positions: Optional[List[int]] = None,
            col_labels: Optional[List[Hashable]] = None,
            col_positions: Optional[List[int]] = None
    ):
        if not (col_labels is None):
            #col_labels is a list of strings representing the columne names for a simple selection
            new_frame = self._frame.col_selection(
                col_labels=col_labels
            )
            return SnowflakeDataframe(sf_table=new_frame,
                                      sf_session=self._sf_session,
                                      key_column=self.key_column,
                                      join_index=self._join_index,
                                      op_tree=SelectionNode(
                                          colnames=col_labels,
                                          prev=self.op_tree,
                                          frame= new_frame
                                      ))

            return SnowflakeDataframe(sf_table=new_frame, sf_base=self._base_partition, virtual_frame=new_virt_frame)

        if not (row_positions is None):
            print("row_positions type", type(row_positions))
            print("Node at row_positions", type(row_positions._query_compiler._modin_frame.op_tree))
            if isinstance(row_positions, modin.pandas.series.Series) and \
                    isinstance(row_positions._query_compiler._modin_frame.op_tree, ComparisonNode):
                new_frame = self._frame.filter(row_positions._query_compiler._modin_frame.op_tree)
                return SnowflakeDataframe(sf_table=new_frame,
                                          sf_session=self._sf_session,
                                          key_column=self.key_column,
                                          join_index=self._join_index,
                                          op_tree=FilterNode(
                                              colnames=self.columns,
                                              prev=self.op_tree,
                                              frame=new_frame
                                          ))
            if isinstance(row_positions, modin.pandas.series.Series) and \
                    isinstance(row_positions._query_compiler._modin_frame.op_tree, LogicalNode):
                new_frame = self._frame.filter(row_positions._query_compiler._modin_frame.op_tree)
                return SnowflakeDataframe(sf_table=new_frame,
                                          sf_session=self._sf_session,
                                          key_column=self.key_column,
                                          join_index=self._join_index,
                                          op_tree=FilterNode(
                                              colnames=self.columns,
                                              prev=self.op_tree,
                                              frame=new_frame
                                          ))

            return self
        return self

    @track
    def to_pandas(
            self
    ):
        from modin.pandas.dataframe import DataFrame
        return modin.pandas.io.from_pandas(self._frame._frame.to_pandas())

    @track
    def copy(
            self
    ):
        return SnowflakeDataframe(sf_table=self._frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=self.op_tree)

    @track
    def bin_op(
            self,
            other,
            op_name,
            **kwargs
    ):
        comp_dict = {
            "le": "<=",
            "ge": ">=",
            "eq": "=",
            "lt": "<",
            "gt": ">"
        }
        operator_dict = {
            "mul": "*"
        }

        if op_name in comp_dict.keys() and not isinstance(other, self.__class__):
            assert len(self.columns) == 1, "Comparisons can only be performed on one column"
            new_frame = self._frame.bin_comp(
                column=self.columns[0],
                operator=comp_dict[op_name],
                other=other
            )
            return SnowflakeDataframe(sf_table=new_frame,
                                      sf_session=self._sf_session,
                                      key_column=self.key_column,
                                      join_index=self._join_index,
                                      op_tree=ComparisonNode(
                                          colnames=self.columns,
                                          operator=comp_dict[op_name],
                                          value=other,
                                          prev=self.op_tree,
                                          comp_column=self.columns[0],
                                          frame=new_frame
                                      ))

        if op_name in operator_dict.keys() and isinstance(other, self.__class__):
            print("We in herererer")
            assert len(self.columns) == 1, "Series operation can only be performed on one column"
            assert len(other.columns) == 1, "Series operation can only be performed on one column"
            left_column = self.op_tree.prev.colnames[0]
            right_column = other.op_tree.prev.colnames[0]
            curr_node = self.op_tree
            while curr_node != None:
                print(curr_node.name)
                print("Left check ", left_column, "----", str(left_column in curr_node.colnames))
                print("Right check ", right_column, "----", str(right_column in curr_node.colnames))
                if left_column in curr_node.colnames and \
                        right_column in curr_node.colnames:
                    break
                curr_node = curr_node.prev
            print("Frame type: ", type(curr_node.frame))
            new_frame = self._frame.bin_op(
                left_column=left_column,
                right_column=right_column,
                operator=operator_dict[op_name],
                frame=curr_node.frame._frame
            )
            return SnowflakeDataframe(sf_table=new_frame,
                                      sf_session=self._sf_session,
                                      key_column=self.key_column,
                                      join_index=self._join_index,
                                      op_tree=BinOpNode(
                                          colnames=f"{left_column} {operator_dict[op_name]} {right_column}",
                                          operator=operator_dict[op_name],
                                          other=other.op_tree,
                                          prev=self.op_tree,
                                          frame=new_frame
                                      ))
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
        new_frame = self._frame.rename(rename_dict=rename_dict)
        return SnowflakeDataframe(sf_table=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=RenameNode(
                                      old_colnames=self.columns,
                                      new_colnames=new_columns,
                                      prev=self.op_tree,
                                      frame=new_frame
                                  ))

    @track
    def join(
            self,
            other,
            on
    ):
        new_frame = None

        new_frame = self._frame.join(
            other_frame=other._query_compiler._modin_frame._frame._frame,
            own_index=self._join_index,
            other_index=other._query_compiler._modin_frame._join_index
        )
        try:
            new_frame = self._frame.join(
                other_frame=other._modin_frame._frame._frame,
                own_index=self._join_index,
                other_index=other._modin_frame._join_index
            )
        except:
            ReferenceError("Index needs to be set for join")

        return SnowflakeDataframe(sf_table=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=JoinNode(
                                      self_colnames=self.columns,
                                      other_colnames=other._query_compiler._modin_frame.columns,
                                      other_tree=other._query_compiler._modin_frame.op_tree,
                                      prev=self.op_tree,
                                      frame=new_frame
                                  ))

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
                    if ascending[0] == False:
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
    def logic_op(self,
                 other,
                 logic_operator: str = None
                 ):

        self_last_comp = self.op_tree.prev
        other_last_comp = other._modin_frame.op_tree.prev
        new_frame = self._frame.logical_expression(left_comp=self_last_comp,
                                                   right_comp=other_last_comp,
                                                   logical_operator=logic_operator)
        return SnowflakeDataframe(sf_table=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=LogicalNode(
                                      left_comp=self_last_comp,
                                      right_comp=other_last_comp,
                                      logical_operator=logic_operator,
                                      frame=new_frame
                                  ))

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
        print("Value type: ", type(value._modin_frame._partitions), " , ")

        res = self._partitions.join(value._modin_frame._partitions)
        print(res.to_pandas())

        return self
