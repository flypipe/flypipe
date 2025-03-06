import os
import subprocess
from typing import Union, List
from uuid import uuid4

from flypipe.misc.dbml import build_dbml
from flypipe.node import Node


def invoke(commands: str):
    result = subprocess.run(
        commands.split(" "), capture_output=True, text=True
    )  # Replace with your command

    if result.returncode != 0:
        print("Command failed!")
        print(result.stderr)  # Error message


def build_erd_svg(
    nodes: Union[List[Node], Node],
    output_path,
    only_nodes_with_tags: Union[List[str], str] = None,
    only_nodes_with_cache: bool = False,
):
    """
    Reads flypipe nodes and builds a SVG image with the ERD diagram

    Parameters
    ----------

    nodes : Union[List[Node], Node]
        nodes to be considered to be added to the dbml
    output_path: str
        full file path name where the svg is stored.
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
    """

    dbml_file_path = f"./{str(uuid4())}.dbml"
    build_dbml(
        nodes,
        only_nodes_with_tags=only_nodes_with_tags,
        only_nodes_with_cache=only_nodes_with_cache,
        file_path_name=dbml_file_path,
    )

    try:
        invoke(f"dbml-renderer -i {dbml_file_path} -o {output_path}")
    except FileNotFoundError:
        raise FileNotFoundError(
            "dbml-renderer not installed! Please install it and try again "
            "(https://github.com/softwaretechnik-berlin/dbml-renderer)."
        )
    except Exception as e:
        raise e
    finally:
        os.remove(dbml_file_path)
