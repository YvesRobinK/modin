from typing import List


class Node:
    def __init__(self,
                 prev=None,
                 frame=None
                 ):
        self.prev = prev
        self.frame = frame
        if prev is not None:
            prev.next_node = self


class SetIndexNode(Node):
    def __init__(self,
                 index: str = None,
                 colnames: str = None,
                 prev=None,
                 frame=None
                 ):
        self.name = "setIndexNode"
        self.colnames = colnames
        self.index = index
        super().__init__(prev=prev, frame=frame)


class ConstructionNode(Node):
    def __init__(self,
                 colnames: str = None,
                 prev=None,
                 frame=None
                 ):
        self.name = "constrNode"
        self.colnames = colnames
        super().__init__(prev=prev, frame=frame)


class JoinNode(Node):
    def __init__(self,
                 self_colnames=None,
                 other_colnames=None,
                 other_tree=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "JoinNode"
        self.other_tree = other_tree
        self.self_colnames = self_colnames
        self.other_colnames = other_colnames
        self.colnames = list(set(self.self_colnames) | set(self.other_colnames))
        super().__init__(prev=prev, frame=frame)


class SelectionNode(Node):
    def __init__(self,
                 colnames: str,
                 prev=None,
                 frame=None
                 ):
        self.name = "selectionNode"
        self.colnames = colnames
        super().__init__(prev=prev, frame=frame)


class ComparisonNode(Node):
    def __init__(self,
                 colnames: str = None,
                 operator: str = None,
                 value: str = None,
                 comp_column: str = None,
                 prev=None,
                 frame=None
                 ):
        self.name = "comparNode"
        self.colnames = colnames
        self.comp_column = comp_column
        self.operator = operator
        self.value = value
        super().__init__(prev=prev, frame=frame)


class AggNode(Node):
    def __init__(self,
                 colnames: str = None,
                 agg_dict=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "aggNode"
        self.colnames = colnames
        self.agg_dict = agg_dict
        super().__init__(prev=prev, frame=frame)


class BinOpNode(Node):
    def __init__(self,
                 colnames: str = None,
                 operator: str = None,
                 other=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "binopNode"
        self.colnames = colnames
        self.operator = operator
        self.other = other
        super().__init__(prev=prev, frame=frame)


class RenameNode(Node):
    def __init__(self,
                 old_colnames=None,
                 new_colnames=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "renameNode"
        self.colnames = new_colnames
        self.old_colnames = old_colnames
        self.new_colnames = new_colnames
        super().__init__(prev=prev, frame=frame)


class LogicalNode(Node):
    def __init__(self,
                 left_comp=None,
                 right_comp=None,
                 logical_operator=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "logicsNode"
        self.left_comp = left_comp
        self.right_comp = right_comp
        self.logical_operator = logical_operator
        super().__init__(prev=left_comp, frame=frame)


class FilterNode(Node):
    def __init__(self,
                 colnames: str,
                 prev=None,
                 frame=None
                 ):
        self.name = "filterNode"
        self.colnames = colnames
        super().__init__(prev=prev, frame=frame)


class SortNode(Node):
    def __init__(self,
                 colnames: str,
                 sort_cols=None,
                 ascending=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "sortNode"
        self.colnames = colnames
        super().__init__(prev=prev, frame=frame)


class GroupByNode(Node):
    def __init__(self,
                 colnames: str,
                 grouping_cols=None,
                 aggregator=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "groupbyNode"
        self.colnames = colnames
        self.grouping_cols = grouping_cols
        self.aggregator = aggregator
        super().__init__(prev=prev, frame=frame)


class AssignmentNode(Node):
    def __init__(self,
                 colnames: str,
                 assignment_col=None,
                 prev=None,
                 frame=None
                 ):
        self.name = "assignmentNode"
        self.colnames = colnames
        self.assignment_col = assignment_col
        super().__init__(prev=prev, frame=frame)


class VirtualFrame:
    def __init__(self,
                 node_list: List[Node] = None
                 ):
        self.node_list = node_list

    def __copy__(self):
        return self.__class__(node_list=self.node_list)

    def from_col_names(self,
                       col_names
                       ):
        res = []
        for node in self.node_list:
            for col_name in col_names:
                if node.colname == col_name:
                    res.append(node)
        return self.__class__(res)

    def extend_nodes(self,
                     node_class
                     ):
        new_node_list = [node_class(node.colname, node) for node in self.node_list]
        return self.__class__(new_node_list)

class ReplacementNode(Node):
    def __init__(self,
                 colnames: str = None,
                 rep_column: str = None,
                 prev=None,
                 frame=None
                 ):
        self.name = "replacementNode"
        self.colnames = colnames
        self.rep_column = rep_column
        super().__init__(prev=prev, frame=frame)

class SplitNode(Node):
    def __init__(self,
                 colnames: str = None,
                 pat = None,
                 expand = None,
                 regex = None,
                 column= None,
                 prev= None,
                 frame= None
                 ):
        self.name = "splitNode"
        self.colnames = colnames
        self.pat = pat
        self.expand = expand
        self.regex = regex
        self.column = column
        self.key = None
        super().__init__(prev=prev, frame=frame)

class SplitAssignNode(Node):
    def __init__(self,
                 colnames: str = None,
                 pat = None,
                 expand = None,
                 regex = None,
                 column= None,
                 prev= None,
                 frame= None
                 ):
        self.colnames = frame._frame.columns
        super().__init__(prev=prev, frame=frame)

class RowAggregationNode(Node):
    def __init__(self,
                 aggregated_cols=None,
                 agg=None,
                 prev= None,
                 frame= None
                 ):
        self.aggregated_cols=aggregated_cols
        self.agg = agg
        self.colnames = frame._frame.columns
        super().__init__(prev=prev, frame=frame)

class WriteItemsNode(Node):
    def __init__(self,
                 row_numeric_index=None,
                 col_numeric_index=None,
                 item=None,
                 prev=None,
                 frame=None
                 ):
        self.row_numeric_index = row_numeric_index
        self.col_numeric_index = col_numeric_index
        self.item = item
        self.colnames = frame._frame.columns
        super().__init__(prev=prev, frame=frame)

class DropNode(Node):
    def __init__(self, colnames=None, droped_columns=None, prev=None, frame=None):
        self.colnames = colnames
        self.droped_columns = droped_columns
        super().__init__(prev=prev, frame=frame)

class FillnaNode(Node):
    def __init__(self, value, prev=None, frame=None):
        self.value = value
        super().__init__(prev=prev, frame=frame)

class ModeNode(Node):
    def __init__(self, prev=None, frame=None):
        super().__init__(prev, frame)
