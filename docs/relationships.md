# Relationships


You can document relationships between node outputs. These relationships differ from node dependencies, as they describe
how the outputs of different nodes are related to each other.

Documenting these relationships helps your team better understand how nodes interact, making it easier to work with
materialized tables. Additionally, these relationships facilitate the generation of [Entity-Relationship Diagrams (ERDs)](notebooks/miscellaneous/build-erd.ipynb).

**Advantages of Having ERD Diagrams Available for the Team**

* **Improved Data Understanding**: ERDs provide a clear visual representation of how data flows between different nodes, making it easier for team members to grasp the structure of the data.
* **Faster Onboarding**: New team members can quickly understand the relationships between different tables and nodes, reducing the learning curve.
* **Better Collaboration**: A shared ERD diagram enables data engineers, analysts, and stakeholders to discuss and align on data relationships efficiently.
* **Reduced Errors**: Understanding relationships between nodes helps prevent inconsistencies and ensures that transformations and aggregations align with business logic.
* **Optimized Query Performance**: By visualizing relationships, teams can identify redundant joins or inefficient queries and optimize database performance accordingly.

To document the relationships between nodes or tables, you can declare them as follows:

``` py
@node(
    ...
    output=Schema(
        Column("col_name", String(), "description")

        # declare the relationship with other nodes output columns
        .one_to_many(another_node.ref.col, "relationship description")
    )
def my_node(...):
    ...
```


The possible relationships are `.one_to_one(...)`, `.one_to_many(...)`, `.many_to_one(...)` and
`.many_to_many(...)`

!!! warning "Important"
    **Relationships are solely for documentation purposes**; they do not enforce any constraints at runtime.
    Therefore, not documenting relationships has no impact on execution.

## `node.output` vs `node.ref` — which to use where

A `Node` exposes two views over its output schema. Each view is reserved for
exactly one use case and **flypipe will raise** if you use the wrong one:

* **`node.output`** — for **column-spec inheritance**, i.e. embedding a
  parent column into a downstream `Schema`:

    ``` py
    @node(
        ...
        output=Schema(
            parent.output.zip_code,   # reuse name/type/description from parent
        )
    )
    def child(...):
        ...
    ```

    The returned column has the parent's `pk` flag and outbound foreign keys
    stripped, so the child node doesn't accidentally inherit the parent's
    identity or its FKs.

    Passing `parent.output.<col>` as a relationship target raises `ValueError`.

* **`node.ref`** — for **foreign-key targets**, i.e. when you are pointing
  *at* a column on another node:

    ``` py
    @node(
        ...
        output=Schema(
            Column("person_id", String(), "person id")
                .many_to_one(person.ref.person_id, "person fk"),
        )
    )
    def person_event(...):
        ...
    ```

    The returned column has its outbound foreign keys stripped (a relationship
    target carries no FKs of its own) but its `pk` flag is **preserved**, so
    downstream tooling (validators, ERD/DBML emitters, lineage tools) can verify
    that the FK actually points at a primary-key column on the parent.

    Passing `parent.ref.<col>` to `Schema(...)` raises `ValueError`.

Rule of thumb: use `.output` when you are *absorbing* a column, and `.ref`
when you are *referencing* one.



Here is a more detailed implementation of these relationships and its usage:

!!! note "flypipe.schema.column.Column"

    ::: flypipe.schema.column.Column