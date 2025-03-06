Relationships
=============

You can document relationships between node outputs. These relationships differ from node dependencies, as they describe
how the outputs of different nodes are related to each other.

Documenting these relationships helps your team better understand how nodes interact, making it easier to work with
materialized tables. Additionally, these relationships facilitate the generation of `Entity-Relationship Diagrams (ERDs) <../notebooks/miscellaneous/build-erd.ipynb>`_.

**Advantages of Having ERD Diagrams Available for the Team**

* **Improved Data Understanding**: ERDs provide a clear visual representation of how data flows between different nodes, making it easier for team members to grasp the structure of the data.
* **Faster Onboarding**: New team members can quickly understand the relationships between different tables and nodes, reducing the learning curve.
* **Better Collaboration**: A shared ERD diagram enables data engineers, analysts, and stakeholders to discuss and align on data relationships efficiently.
* **Reduced Errors**: Understanding relationships between nodes helps prevent inconsistencies and ensures that transformations and aggregations align with business logic.
* **Optimized Query Performance**: By visualizing relationships, teams can identify redundant joins or inefficient queries and optimize database performance accordingly.

To document the relationships between nodes or tables, you can declare them as follows:

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

.. important::
   **Relationships are solely for documentation purposes**; they do not enforce any constraints at runtime.
   Therefore, not documenting relationships has no impact on execution.

.. raw:: html

    <hr>

Here is a more detailed implementation of these relationships and its usage:

.. autoclass:: flypipe.schema.column.Column
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: copy, set_parent




