from typing import Optional, List, Hashable

import numpy
from functools import wraps
import modin.pandas.io

from modin.core.dataframe.pandas.dataframe.dataframe import PandasDataframe
from modin.experimental.core.storage_formats.hdk import DFAlgQueryCompiler


from snowflake.snowpark.types import LongType, StringType, DecimalType, _NumericType, DataType
from pandas import Series

from modin.experimental.core.execution.snowflake.dataframe.frame import Frame
from modin.experimental.core.execution.snowflake.dataframe.operaterNodes import \
    Node, ConstructionNode, SelectionNode, ComparisonNode, VirtualFrame, JoinNode, SetIndexNode, FilterNode, RenameNode, \
    LogicalNode, BinOpNode, AggNode, GroupByNode, SortNode, AssignmentNode

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
    """
        Lazy dataframe based on Snowflake dataframe

        While operations are eagerly performed on the Snowflake dataframe, the
        implementation makes sure the data is only computed once a function invokes
        the transformation into a local Pandas dataframe. At this point the normal
        operation on the now ModinDataFrame can be performed.

        Parameters
        ----------
        partitions : np.ndarray, optional
            Partitions of the frame.
        frame: modin.experimental.core.execution.snowflake.dataframe.frame.Frame
            Represents the table held by the dataframe
        op_tree: modin.experimental.core.execution.snowflake.dataframe.operatorNodes
            Representation of the operations performed on the dataframe, acts as a
            way to reconstruct operations performed on other dataframe
        sf_session: snowflake.snowpark.session
            The snowflake session used by this dataframe
        key_columne: str
            Could eventually be used as a mechanism to join frames where all join fields
            habe been lost
        join_index: str
            The columne used for joins on this table

        Attributes
        ----------
        key_columne : str
            Not currently used
        shape_hint : str
            Not currently used
        columns : [str]
            The column names of this dataframe
        _sf_session : snowflake.snowpark.session
            The snowflake session used by this dataframe
        _sf_types: [snowflake.snowpark.types]
            List of the snowpark types in the dataframe
        schema: snowflake.snowpark.dataframe.schema
            Schema of the snowpark datafrane
        _join_index: str
            Name of the column used for joins
        index: [str]
            Columne names
        op_tree: modin.experimental.core.execution.snowflake.dataframe.operatorNodes
            Represent the operations performed on this dataframe, is used to reconstruct
            execution when this dataframe is used as a parameter in a function that operater
            on another dataframe.
        dtype: [numpy.dtypes]
            Numpy dtype equivalent of snowflake types

        """
    _query_compiler_cls = DFAlgQueryCompiler

    def __init__(
            self,
            frame=None,
            op_tree=None,
            sf_session=None,
            key_column=None,
            join_index=None
    ):
        if isinstance(frame, Frame):
            self._frame = frame
        else:
            self._frame = Frame(frame)
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
    def agg(
            self,
            agg: str
    ):
        """
        Perform specified aggregation along columns.
        Parameters
        ----------
        agg : str
            Name of the aggregation function to perform.
        Returns
        -------
        SnowflakeDataframe
            New frame containing the result of aggregation.
        """
        schema = self._frame._frame.schema
        command_dict = {}
        for col in schema:
            if isinstance(col.datatype, _NumericType):
                command_dict[col.name] = agg
        new_frame = self._frame.agg(
            agg_dict=command_dict
        )
        return SnowflakeDataframe(frame=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=AggNode(
                                      colnames=command_dict.values(),
                                      prev=self.op_tree,
                                      frame=new_frame
                                  ))

    @property
    def _has_unsupported_data(
            self
    ):
        """
        Dummy to skip operation in upper layers

        Returns
        -------
        Bool
        """
        return False

    @track
    def __constructor__(self):
        """
        Create a new instance of this object.

        Returns
        -------
        SnowflakeDataframe
        """
        return type(self)

    @track
    def set_index(self,
                  index: None
                  ):
        """
        Sets self._join_index

        Returns
        -------
        SnowflakeDataframe
        """
        return SnowflakeDataframe(
            frame=self._frame,
            sf_session=self._sf_session,
            op_tree=SetIndexNode(
                index=index,
                colnames=self.columns,
                prev=self.op_tree,
                frame=self.op_tree.frame
            ),
            key_column=self.key_column,
            join_index=index
        )

    @track
    def take_2d_labels_or_positional(
            self,
            key=None,
            row_labels= None,
            row_positions = None,
            col_labels = None,
            col_positions = None
    ):
        """
        Performs filter and selection depending on what kind of class the
        parameters are

        Returns
        -------
        SnowflakeDataframe
        """
        if not (col_labels is None):
            #col_labels is a list of strings representing the columne names for a simple selection
            new_frame = self._frame.col_selection(
                col_labels=col_labels
            )
            return SnowflakeDataframe(frame=new_frame,
                                      sf_session=self._sf_session,
                                      key_column=self.key_column,
                                      join_index=self._join_index,
                                      op_tree=SelectionNode(
                                          colnames=col_labels,
                                          prev=self.op_tree,
                                          frame=new_frame
                                      ))


        if not (row_positions is None):
            if isinstance(row_positions, modin.pandas.series.Series) and \
                    isinstance(row_positions._query_compiler._modin_frame.op_tree, ComparisonNode):
                new_frame = self._frame.filter(row_positions._query_compiler._modin_frame.op_tree)
                return SnowflakeDataframe(frame=new_frame,
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
                return SnowflakeDataframe(frame=new_frame,
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
        """
        Materializes the lazy snowflake dataframe as a local modin dataframe

        Returns
        -------
        ModinDataframe
        """
        from modin.pandas.dataframe import DataFrame
        return modin.pandas.io.from_pandas(self._frame._frame.to_pandas())

    @track
    def copy(
            self
    ):
        """
        Makes a copy of the SnowflakeDataframe

        Returns
        -------
        SnowflakeDataframe
        """
        return SnowflakeDataframe(frame=self._frame,
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
        """
        Performs binary operations and comparisons on the dataframe

        Returns
        -------
        SnowflakeDataframe
        """
        comp_dict = {
            "le": "<=",
            "ge": ">=",
            "eq": "=",
            "lt": "<",
            "gt": ">"
        }
        operator_dict = {
            "mul": "*",
            "sub": "-",
            "div": "/",
            "add": "+"
        }

        if op_name in comp_dict.keys() and not isinstance(other, self.__class__):
            assert len(self.columns) == 1, "Comparisons can only be performed on one column"
            new_frame = self._frame.bin_comp(
                column=self.columns[0],
                operator=comp_dict[op_name],
                other=other
            )
            return SnowflakeDataframe(frame=new_frame,
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
            assert len(self.columns) == 1, "Series operation can only be performed on one column"
            assert len(other.columns) == 1, "Series operation can only be performed on one column"
            left_column = self.op_tree.prev.colnames[0]
            right_column = other.op_tree.prev.colnames[0]
            curr_node = self.op_tree
            while curr_node is not None:
                if left_column in curr_node.colnames and \
                        right_column in curr_node.colnames:
                    break
                curr_node = curr_node.prev
            new_frame = self._frame.bin_op(
                left_column=left_column,
                right_column=right_column,
                operator=operator_dict[op_name],
                frame=curr_node.frame._frame
            )
            return SnowflakeDataframe(frame=new_frame,
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
        """
       Sets self._join_index

       Returns
       -------
       SnowflakeDataframe
       """
        return True if len(self.columns) > 1 else False

    @track
    def _set_columns(
            self,
            new_columns
    ):
        """
       Renames the dataframe columns

       Returns
       -------
       SnowflakeDataframe
       """
        rename_dict = {}
        for index in range(len(self.columns)):
            rename_dict[self.columns[index]] = new_columns[index]
        new_frame = self._frame.rename(rename_dict=rename_dict)
        return SnowflakeDataframe(frame=new_frame,
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
        """
       Joins two dataframes, parameter (on) is not used, due to changes in upper layer
       we defer here instead of using concat

       Returns
       -------
       SnowflakeDataframe
       """
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

        return SnowflakeDataframe(frame=new_frame,
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
        """
        Dummy function to skip mechanism in upper layer
        Returns
        -------
        [str]
        """
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
        """
        Performs the combined function of group_by().agg(), since upper layer
        combines the execution into a single call
        Returns
        -------
        SnowflakeDataframe
        """
        comp_dict = {
            "sum": "sum",
            "mean": "mean",
            "min": "min",
            "max": "max"
        }
        diff = list(set(self.columns) - set(by.columns))

        new_frame = self._frame.groupby_agg(grouping_cols=by.columns,
                                            aggregator=comp_dict[list(op.values())[0]],
                                            agg_col=diff
                                            )
        return SnowflakeDataframe(frame=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=GroupByNode(
                                      colnames=self.columns,
                                      grouping_cols=by.columns,
                                      aggregator=comp_dict[list(op.values())[0]],
                                      prev=self.op_tree,
                                      frame=new_frame
                                  ))

    @track
    def sort_rows(
            self,
            columns,
            ascending,
            ignore_index,
            na_position

    ):
        """
        Sorts the rows of the dataframe according to the parameters given
        Returns
        -------
        SnowflakeDataframe
        """
        new_frame = self._frame.sort(dataframe=self,
                                     columns=columns,
                                     ascending=ascending
                                     )
        return SnowflakeDataframe(frame=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=SortNode(
                                      colnames=self.columns,
                                      sort_cols=columns,
                                      ascending=ascending,
                                      prev=self.op_tree,
                                      frame=new_frame
                                  ))

    @track
    def _execute(
            self
    ):
        """
        Dummy function to skip mechanism in upper layer
        Returns
        -------
        SnowflakeDataframe
        """
        return self

    @track
    def logic_op(self,
                 other,
                 logic_operator: str = None
                 ):
        """
        Performs logical operations between two frames that have been used for comparison
        Returns
        -------
        SnowflakeDataframe
        """

        self_last_comp = self.op_tree.prev
        other_last_comp = other._modin_frame.op_tree.prev
        new_frame = self._frame.logical_expression(left_comp=self_last_comp,
                                                   right_comp=other_last_comp,
                                                   logical_operator=logic_operator)
        return SnowflakeDataframe(frame=new_frame,
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
        """
        Renames the dataframes columns according to the dict given
        Returns
        -------
        SnowflakeDataframe
        """
        command_dict = {}
        for key in columns.keys():
            for df_col in self.columns:
                if key in df_col:
                    command_dict[df_col] = '"' + columns[key] + '"'
        new_frame = self._frame.rename(rename_dict=command_dict)
        return SnowflakeDataframe(frame=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=RenameNode(
                                      old_colnames=self.columns,
                                      new_colnames=list(command_dict.values()),
                                      prev=self.op_tree,
                                      frame=new_frame
                                  ))


    @track
    def setitem(self,
                loc,
                column,
                value
                ):
        """
        Simulates assignment to a column by replaying the operation performed on
        the SnowflakeDataframe given, operations stored in value._modin_frame.op_tree
        need to be applied to the corresponding columne in base frame

        TODO: so far only basic assignments can be done, namely operations between to columns
        TODO: of the form x["PROFIT"] = x["LO_REVENUE"] * x["LO_DISCOUNT"]
        TODO: Other operations could be caught here and handled the same was by passing to the
        TODO: self._frame.assign() function and handling a specific operatorNode there

        Returns
        -------
        SnowflakeDataframe
        """
        new_cols = self.columns
        if column in self.columns:
            new_frame = self._frame.assign(
                                         override_column=column,
                                        op_tree= value._modin_frame.op_tree
                                         )
        else:
            new_cols.append(column)
            new_frame = self._frame.assign(new_column=column,
                                           op_tree=value._modin_frame.op_tree
                                           )

        return SnowflakeDataframe(frame=new_frame,
                                  sf_session=self._sf_session,
                                  key_column=self.key_column,
                                  join_index=self._join_index,
                                  op_tree=AssignmentNode(
                                      colnames=new_cols,
                                      assignment_col=column,
                                      prev=self.op_tree,
                                      frame=new_frame
                                  ))
