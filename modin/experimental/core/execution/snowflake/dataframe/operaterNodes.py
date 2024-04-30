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
        print(self.colnames)
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
        super().__init__(prev=right_comp, frame=frame)


class FilterNode(Node):
    def __init__(self,
                 colnames: str,
                 prev=None,
                 frame=None
                 ):
        self.name = "filterNode"
        self.colnames = colnames
        super().__init__(prev=prev, frame=frame)


class VirtualFrame:
    def __init__(self,
                 node_list: [Node] = None
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
                print(col_name)
                if node.colname == col_name:
                    res.append(node)
        return self.__class__(res)

    def extend_nodes(self,
                     node_class
                     ):
        new_node_list = [node_class(node.colname, node) for node in self.node_list]
        return self.__class__(new_node_list)
