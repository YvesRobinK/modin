from snowflake.snowpark import Table
from snowflake.snowpark.functions import col
from modin.experimental.core.execution.snowflake.dataframe.operaterNodes import \
    Node, ConstructionNode, SelectionNode, ComparisonNode, VirtualFrame, JoinNode, SetIndexNode, FilterNode, RenameNode, \
    LogicalNode


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
        new_frame = self._frame.select_expr(f"{column} {operator} {other}")
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
                                            f"{left_comp.value} "
                                            f" {logical_operator} "
                                            f"__REDUCED__ "
                                            f"{right_comp.operator} "
                                            f"{right_comp.value}")
        return Frame(new_frame)

    def filter(self,
               comp_Node=None
               ):
        if isinstance(comp_Node, ComparisonNode):
            new_frame = self._frame.filter(f"{comp_Node.comp_column} "
                                           f"{comp_Node.operator} "
                                           f"{comp_Node.value}"
                                           )
        elif isinstance(comp_Node, LogicalNode):
            left_comp = comp_Node.prev
            right_comp = comp_Node.right_comp
            new_frame = self._frame.filter(f" {left_comp.comp_column} "
                                           f"{left_comp.operator} "
                                           f"{left_comp.value} "
                                           f" {comp_Node.logical_operator} "
                                           f"{right_comp.comp_column} "
                                           f"{right_comp.operator} "
                                           f"{right_comp.value}")

        return Frame(new_frame)

    def rename(self,
               rename_dict=None
               ):
        new_frame = self._frame.rename(rename_dict)
        return Frame(new_frame)
