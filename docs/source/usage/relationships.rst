Relationships
=============

You can document relationships between nodes outputs. These relationships differs from the nodes dependencies as they
document how the output of the nodes relate to each other.

Nodes dependencies are a list of dependencies needed to generate the output of a node, when the output of the node can
end up being materialised into a table.

In order to help document the relationships of nodes or tables, you can declare the relationships as follow:

.. code-block:: python
    @node(
        ...
        output=Schema(
            Column("col_name", String(), "description)

            # declare the relationship with other nodes output columns
            .one_to_many(another_node.output.col, "relationship description")
        )
    def my_node(...):
        ...



The possible relationships are ``.one_to_one(...)``, ``.one_to_many(...)``, ``.many_to_one(...)`` and
``.many_to_many(...)``

Go to the `Build DBML <../notebooks/miscellaneous/build-dbml.ipynb>`_ for more information.

.. raw:: html

    <hr>

.. autoclass:: flypipe.schema.column.Column
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: copy, set_parent




