
class Node:
    def __init__(self,
                 prev= None
                 ):
        self.name = "unnamed"
        self.prev = prev


class ConstructionNode(Node):
    def __init__(self,
                 colname,
                 prev: Node = None
                 ):
        self.name = "constrNode"
        self.colname = colname
        super().__init__(prev=prev)

class SelectionNode(Node):
    def __init__(self,
                 colname: str,
                 prev: Node = None
                 ):
        self.name = "selectionNode"
        self.colname = colname
        super().__init__(prev=prev)
class ComparisonNode(Node):
    def __init__(self,
                colname: str,
                operator: str,
                value: str,
                prev: Node
                 ):
        self.name = "comparNode"
        self.colname = colname
        self.operator = operator
        self.value = value
        super().__init__(prev=prev)

class VirtualFrame:
    def __init__(self,
                 node_list : [Node] = None
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
