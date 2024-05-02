from snowflake.snowpark import Table
from snowflake.snowpark.functions import col
from modin.experimental.core.execution.snowflake.dataframe.operaterNodes import \
    Node, ConstructionNode, SelectionNode, ComparisonNode, VirtualFrame, JoinNode, SetIndexNode, FilterNode, RenameNode, \
    LogicalNode

from snowflake.snowpark.types import StringType


class Frame:
    def __init__(self,
                 sf_rep
                 ):
        self._frame = sf_rep

    def join(self,
             other_frame=None,
             own_index: str = None,
             other_index: str = None
             ):
        new_frame = self._frame.join(other_frame, col(own_index) == col(other_index))
        return Frame(new_frame)

    def col_selection(self,
                      col_labels: [str] = None
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
        new_frame = frame.select_expr(f"{left_column} {operator} {right_column}")
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
        left_col = op_tree.prev.prev.colnames[0]
        right_col = op_tree.other.prev.colnames[0]
        operator = op_tree.operator
        new_frame = self._frame.with_column("TEMP", (self._frame[left_col] - self._frame[right_col]))
        rename_dict = {'TEMP': '"' + new_column + '"'}
        if override_column is not None:
            new_frame = new_frame.drop(override_column)
            new_frame = new_frame.rename(rename_dict)
        else:
            new_frame =new_frame.rename(rename_dict)
       
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
        return new_frame

    def rename(self,
               rename_dict=None
               ):
        print("rename_dict", rename_dict)
        new_frame = self._frame.rename(rename_dict)
        return Frame(new_frame)
