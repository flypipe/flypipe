from typing import Union, List, Set

from flypipe.node import Node
from flypipe.run_context import RunContext
from flypipe.schema.column import RelationshipType


def replace_dots_except_last(s: str) -> str:
    parts = s.rsplit(".", 1)  # Split into two parts, keeping the last '.'
    return parts[0].replace(".", "_") + ("." + parts[1] if len(parts) > 1 else "")


class Table:
    def __init__(
        self,
        node: Node,
        only_nodes_with_tags: Union[List[str], str] = None,
        only_nodes_with_cache: bool = False,
    ):
        self.node = node
        self.only_nodes_with_tags = only_nodes_with_tags
        self.only_nodes_with_cache = only_nodes_with_cache

    @property
    def columns(self):
        columns = []

        for col in self.node.output_schema.columns:
            column = f"\t{col.name} {str(col.type).replace(' ','')}"
            note = None if not col.description else f"note: '''{col.description}'''"
            pk = None
            if hasattr(col, "pk"):
                pk = "PK" if col.pk else None

            column_definition = [d for d in [note, pk] if d is not None]
            if column_definition:
                column_definition = f" [{', '.join(column_definition)}]"
            else:
                column_definition = ""
            columns.append(column + column_definition)

        return "\n".join(columns)

    @property
    def references(self):
        references = []

        for col in self.node.output_schema.columns:

            source = f"{self.table_name}.{col.name}"

            for fk_column, relationship in col.relationships.items():

                if is_tag_in_considered(
                    fk_column.parent.tags, self.only_nodes_with_tags
                ) and (
                    not self.only_nodes_with_cache
                    or (
                        self.only_nodes_with_cache
                        and fk_column.parent.cache is not None
                    )
                ):

                    relationship_type = ""
                    destiny = (
                        f"{self.get_table_name(fk_column.parent)}.{fk_column.name}"
                    )

                    if relationship.type.name == RelationshipType.MANY_TO_ONE.name:
                        relationship_type = ">"
                        destiny = f"{self.table_name}.{col.name}"
                        source = (
                            f"{self.get_table_name(fk_column.parent)}.{fk_column.name}"
                        )
                    elif relationship.type.name == RelationshipType.ONE_TO_MANY.name:
                        relationship_type = "<"
                    elif relationship.type.name == RelationshipType.MANY_TO_MANY.name:
                        relationship_type = "<>"
                    elif relationship.type.name == RelationshipType.ONE_TO_ONE.name:
                        relationship_type = "-"

                    relationship_name = (
                        ""
                        if relationship.description is None
                        else " " + relationship.description.replace(" ", "_")
                    )
                    references.append(
                        f"Ref{relationship_name}: {source} {relationship_type} {destiny}"
                    )

        return "" if not references else "\n\n" + "\n".join(references)

    @property
    def table_description(self):
        table_description = [
            f"Managed by flypipe node `{self.node.function.__name__}`",
        ]

        if self.node.description:
            table_description.append(self.node.description)

        table_description = "\n\n".join(table_description)
        table_description = f"\n\n\tNote: '''{table_description}'''"
        return table_description

    def get_table_name(self, node):
        return (
            replace_dots_except_last(
                node.function.__name__ if node.cache is None else node.cache.name
            )
            .replace(" ", "_")
            .lower()
        )

    @property
    def table_color(self):
        table_color = ""
        if self.node.cache and hasattr(self.node.cache, "color"):
            table_color = f" [headercolor: {self.node.cache.color}]"
        return table_color

    @property
    def table_name(self):
        return self.get_table_name(self.node)

    def to_dbml(self):
        return f"""Table {self.table_name}{self.table_color} {{
{self.columns}{self.table_description}
}}{self.references}"""


def get_node_graph(node, spark=None):
    run_context = RunContext(spark=spark)
    node.create_graph(run_context)
    return node.node_graph


def is_tag_in_considered(tags, required_tags):
    if required_tags is not None:
        if tags is None:
            return False
        return any(tag in required_tags for tag in tags)

    return True


def add_to_frontier(node: Node, frontier: Set[Node]):
    if node.output_schema is not None:
        frontier.add(node)

    return frontier


def sorted_values_by_keys(d):
    # Sort the dictionary by keys and return the list of values
    return [d[key] for key in sorted(d.keys())]


def build_dbml(
    nodes: Union[List[Node], Node],
    only_nodes_with_tags: Union[List[str], str] = None,
    only_nodes_with_cache: bool = False,
    file_path_name=None,
):
    """
    Reads flypipe nodes and builds a dbml source code with the relationships, if output columns have been defined.

    Parameters
    ----------

    nodes : Union[List[Node], Node]
        nodes to be considered to be added to the dbml
    only_nodes_with_tags : Union[List[str], str], optional, defafult None
        include only nodes with tags
    only_nodes_with_cache: bool, optional, default True
        True: include nodes with cache, False: do not include nodes.

        Note: the cache objects must have name method implemented, example:

            class MyCache(Cache):
                ...
                @property
                def name(self):
                    return <NAME TO BE USED FOR THE DBML TABLE NAME>

    file_path_name: str, optional, default None
        full file path name where the dbml table is stored.

    Returns
    -------

    dbml string if file_path_name is None else it will return None
    """

    nodes = [nodes] if isinstance(nodes, Node) else nodes
    dbml = {}
    frontier = set()
    nodes_seen = []

    for node in nodes:
        node_graph = get_node_graph(node)
        for n in node_graph.graph.nodes():
            node_ = node_graph.get_node(n)["transformation"]
            if (node_ in nodes) or (
                is_tag_in_considered(node_.tags, only_nodes_with_tags)
                and (
                    not only_nodes_with_cache
                    or (only_nodes_with_cache and node_.cache is not None)
                )
            ):
                add_to_frontier(node_, frontier)

    while frontier:

        node_ = frontier.pop()

        if node_ in nodes_seen:
            continue

        nodes_seen.append(node_)

        table = Table(node_, only_nodes_with_tags, only_nodes_with_cache)
        dbml[table.table_name] = table.to_dbml()

        for column in node_.output_schema.columns:
            for fk_node, fk in column.relationships.items():
                if is_tag_in_considered(fk_node.parent.tags, only_nodes_with_tags) and (
                    not only_nodes_with_cache
                    or (only_nodes_with_cache and fk_node.parent.cache is not None)
                ):
                    add_to_frontier(fk_node.parent, frontier)

    dbml = sorted_values_by_keys(dbml)
    dbml = "\n\n".join(dbml)
    if file_path_name is not None:
        with open(file_path_name, "w") as f:
            f.writelines(dbml)
    else:
        return None if dbml == "" else dbml
