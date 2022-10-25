from typing import List, Mapping
from flypipe.node_input import InputNode
from flypipe.node_result import NodeResult


class InputNodes:

    def __init__(self, input_nodes: List[InputNode]):
        self.input_nodes = input_nodes

    def get_node_inputs(self, outputs: Mapping[str, NodeResult]):
        inputs = {}
        for node_input in self.dependencies:
            node_input_value = outputs[node_input.__name__].as_type(self.input_dataframe_type)
            # TODO: problem- how will the node flag translate to converting all inputs to a pandas on spark node to pandas?

            # TODO: how do we cast the type and also filter the columns?
            # Only select the columns that were requested in the dependency definition

            inputs[node_input.__name__] = node_input_value.select_columns(*node_input.selected_columns)
        return inputs