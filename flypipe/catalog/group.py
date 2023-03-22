class Group:
    """
    Nodes can declare that they belong to a group. In the Catalog UI each group is a special node that encases all the
    constituent nodes. We need to translate the backend definition to an appropriate one for the frontend to support
    this.
    """

    def __init__(self, name):
        self.name = name
        self.group_id = self._get_id()
        self.nodes = []

    def _get_id(self):
        return self.name.lower().replace(" ", "_")

    def add_node(self, node):
        self.nodes.append(node)

    def get_def(self):
        return {
            "id": self.group_id,
            "name": self.name,
            "nodes": [node.key for node in self.nodes],
        }
