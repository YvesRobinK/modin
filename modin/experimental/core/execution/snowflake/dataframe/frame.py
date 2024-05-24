from typing import List

import numpy
from snowflake.snowpark.functions import col, lit, when, mode
from snowflake.snowpark.dataframe import DataFrame, Column
from modin.experimental.core.execution.snowflake.dataframe.operaterNodes import \
    ComparisonNode, LogicalNode, RowAggregationNode


class Frame:
    def __init__(self,
                 sf_rep
                 ):
        self._frame: DataFrame = sf_rep

    def join(self,
             other_frame=None,
             own_index: str = None,
             other_index: str = None
             ):
        new_frame = self._frame.join(other_frame, col(own_index) == col(other_index))
        return Frame(new_frame)

    def col_selection(self,
                      col_labels: List[str] = None
                      ):
        new_frame = self._frame.select(col_labels)
        return Frame(new_frame)

    def bin_comp(self,
                 column=None,
                 operator: str = None,
                 other=None
                 ):
        new_frame = None
        if isinstance(other, str):
            if operator == "=":
                new_frame = self._frame.select(col(column) == other)
            elif operator == "<=":
                new_frame = self._frame.select(col(column) <= other)
            elif operator == ">=":
                new_frame = self._frame.select(col(column) >= other)
            elif operator == "<":
                new_frame = self._frame.select(col(column) < other)
            elif operator == ">":
                new_frame = self._frame.select(col(column) > other)
        else:
            new_frame = self._frame.selectExpr(f"{column} {operator} {other}")
        return Frame(new_frame)

    def bin_op(self,
               left_column=None,
               right_column=None,
               operator: str = None,
               frame=None
               ):
        new_frame = frame._frame.select_expr(f"{left_column} {operator} {right_column}")
        return Frame(new_frame)

    def agg(self,
            agg_dict=None
            ):
        new_frame = self._frame.agg(agg_dict)
        return Frame(new_frame)

    def logical_expression(self,
                           left_comp=None,
                           right_comp=None,
                           logical_operator=None
                           ):
        new_frame = self._frame.select_expr(f" __REDUCED__ "
                                            f"{left_comp.operator} "
                                            f"'{left_comp.value}' "
                                            f" {logical_operator} "
                                            f"__REDUCED__ "
                                            f"{right_comp.operator} "
                                            f"'{right_comp.value}'")
        return Frame(new_frame)

    def assign(self,
               override_column= None,
               new_column=None,
               op_tree=None
               ):
        if isinstance(op_tree, RowAggregationNode):
            agg_dict = {
                "sum": "+",
            }
            if override_column is not None:
                column = override_column
            else:
                column = new_column

            command_string  = f"self._frame.with_column(column, ("
            for colname in op_tree.aggregated_cols:
                print(colname)
                command_string += f"self._frame['{colname}'] {agg_dict[op_tree.agg]}"
            command_string = command_string[:-1] + "))"
            print(command_string)
            new_frame = eval(command_string)
            return Frame(new_frame)


        left_col = op_tree.prev.prev.colnames[0]
        right_col = op_tree.other.prev.colnames[0]
        operator = op_tree.operator
        if operator == "-":
            new_frame = self._frame.with_column("TEMP", (self._frame[left_col] - self._frame[right_col]))
        elif operator == "+":
            new_frame = self._frame.with_column("TEMP", (self._frame[left_col] + self._frame[right_col]))
        elif operator == "*":
            new_frame = self._frame.with_column("TEMP", (self._frame[left_col] * self._frame[right_col]))
        elif operator == "/":
            new_frame = self._frame.with_column("TEMP", (self._frame[left_col] / self._frame[right_col]))
        rename_dict = {'TEMP': '"' + new_column + '"'}
        if override_column is not None:
            new_frame = new_frame.drop(override_column)
            new_frame = new_frame.rename(rename_dict)
        else:
            new_frame =new_frame.rename(rename_dict)

        return Frame(new_frame)

    def assign_singular(self,
                        column: str,
                        value: "Frame"):
        """
        Assigns value to Frame[column]
        """
        dataframe_columns = value._frame.columns
        assert len(dataframe_columns) == 1, "Cannot assign a dataframe with more than 1 column to a column"

        new_frame = self._frame.with_column(column, value._frame[dataframe_columns[0]])
        return Frame(new_frame)

    def assign_scalar(self,
                      column,
                      value=None):

        new_frame = self._frame.with_column(column, lit(value))
        return Frame(new_frame)

    def filter(self,
               comp_Node=None
               ):
        if isinstance(comp_Node, ComparisonNode):
            new_frame = self._frame.filter(f'"{comp_Node.comp_column}" '
                                           f"{comp_Node.operator} "
                                           f"'{comp_Node.value}'"
                                           )
        elif isinstance(comp_Node, LogicalNode):
            left_comp = comp_Node.prev
            right_comp = comp_Node.right_comp
            operator_dict={
                "<=": "<=",
                ">=": ">=",
                "=": "==",
                "<": "<",
                ">": ">"
            }
            logical_dict={
                "or": "|",
                "and": "&"
            }
            command_string = (f"self._frame.filter((col(\"{left_comp.comp_column}\") {operator_dict[left_comp.operator]} '{left_comp.value}') {logical_dict[comp_Node.logical_operator]} "
                              f"(col(\"{right_comp.comp_column}\") {operator_dict[right_comp.operator]} '{right_comp.value}'))")
            print(command_string)
            new_frame = eval(command_string)

        return Frame(new_frame)

    def groupby_agg(self,
                    grouping_cols=None,
                    aggregator=None,
                    agg_col=None
                    ):
        assert len(agg_col) == 1, "Aggregation can only be performed on one columne"
        new_frame = self._frame.group_by(grouping_cols).function(aggregator)(agg_col[0])
        agg_col_after = ""
        for col_name in new_frame.schema:
            if agg_col[0] in col_name.name:
                agg_col_after = col_name
                break
        new_frame = new_frame.rename({agg_col_after.name: agg_col[0]})
        return Frame(new_frame)

    def sort(self,
             dataframe=None,
             columns=None,
             ascending=None
             ):
        command_string = 'self._frame.sort('
        for item in columns:
            for item2 in dataframe.columns:
                if item in item2:
                    command_string += 'col("' + str(item) + '")'
                    if ascending[0] == False:
                        command_string += ".desc(),"
                    else:
                        command_string += ".asc(),"
                    ascending = ascending[1:]
        command_string = command_string[:-1] + ')'
        new_frame = eval(command_string)
        return Frame(new_frame)

    def rename(self,
               rename_dict=None
               ):
        new_frame = self._frame.rename(rename_dict)
        return Frame(new_frame)

    def replace(self,
                to_replace= None,
                value = None,
                column= None,
                op_before_selection= None):
        print("Columne: ", column)
        new_frame = op_before_selection.frame._frame.na.replace({to_replace: value}, col(column))
        return Frame(new_frame)

    def split(self,
              pat=None,
              n=None,
              expand=None,
              regex=None,
              column=None):

        return self



    def assign_split(self,
                     other):
        expr_list = []
        print("Other type: ", type(other._modin_frame.op_tree.prev))
        sep = other._modin_frame.op_tree.prev.pat
        #sep = ","
        column = other._modin_frame.op_tree.prev.column
        count = 1

        for item in other._modin_frame.op_tree.prev.key:
            expr_list.append(f"expr(\"SPLIT_PART({column}, \'{sep}\', {count})\").alias(\"{item}\")")
            count += 1

        command_string = f"self._frame.select("
        for coler in self._frame.columns:
            print("Col", coler)
            command_string += f"col(\"{coler}\"),"
        for i in expr_list:
            command_string += i + ","
        command_string = command_string[:-1] + ")"
        print(command_string)
        new_frame = eval(command_string)
        return Frame(new_frame)

    def agg_row(self,
                agg=None,
                columns=None,
                ):
        agg_dict = {
            "sum": "+",
        }
        print("Agg: ", agg)
        expr_string = f""
        for column in columns:
            expr_string += f"{column} {agg_dict[agg]}"
        expr_string = expr_string[:-1]
        new_frame = self._frame.select_expr(expr_string)
        return Frame(new_frame)

    def write_items(self,
                    row_numeric_index=None,
                    col_numeric_index=None,
                    item=None):

        if isinstance(row_numeric_index._query_compiler._modin_frame.op_tree, ComparisonNode):
            comp_op = row_numeric_index._query_compiler._modin_frame.op_tree
            comparison_value = comp_op.value
            comp_column = comp_op.comp_column
            if comp_op.operator == "<":
                for c in col_numeric_index:
                    self._frame = self._frame.with_column(c, when(col(comp_column) < comparison_value, item).otherwise(col(c)))
            if comp_op.operator == ">":
                for c in col_numeric_index:
                    self._frame = self._frame.with_column(c, when(col(comp_column) > comparison_value, item).otherwise(col(c)))
            if comp_op.operator == "=":
                self._frame = self._frame.with_column(
                    comp_column,
                    col(comp_column).cast('BOOLEAN')
                )
                for c in col_numeric_index:
                    self._frame = self._frame.with_column(c, when(col(comp_column) == comparison_value, item).otherwise(col(c)))
        return Frame(self._frame)

    def drop(self,
             columns):
        new_frame = self._frame.drop(columns)
        return Frame(new_frame)

    def mode(self) -> "Frame":
        for column in self._frame.columns:
            mode_column = mode(self._frame[column])
            new_frame = self._frame.with_column(column, mode_column)
        return Frame(new_frame)

    def fillna(self,
               value) -> "Frame":
        # following hard coded case handling is because snowflake does not have
        # any mapping from numpy.bool_ to BooleanType
        # a clean fix to this issue would be adding following entry to `snowflake.snowpark._internal.type_utils.py`
        # PYTHON_TO_SNOW_TYPE_MAPPINGS.update({
        #   numpy.bool_: BooleanType
        # })
        if type(value) == numpy.bool_:
            value = bool(value)
        new_frame = self._frame.na.fill(value)
        return Frame(new_frame)
