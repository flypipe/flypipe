import pytest

from flypipe import node
from flypipe.config import config_context
from flypipe.schema import Schema, Column
from flypipe.schema.column import RelationshipType
from flypipe.schema.types import String


class TestColumn:
    """Tests for column"""

    def test_schema_mandatory_description(self):
        with pytest.raises(ValueError) as ex, config_context(
            require_schema_description=True
        ):

            @node(
                type="pandas",
                output=Schema(
                    [
                        Column("c1", String(), "test"),
                        Column("c2", String()),
                    ]
                ),
            )
            def transformation():
                return

        assert str(ex.value) == (
            "Descriptions on schema columns configured as mandatory but no description provided for column c2"
        )

    def test_relationship_one_to_one(self):
        print()

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").one_to_one(t1.ref.t1c1, "my desc"),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert relationships[t1.output_schema.t1c1].type == RelationshipType.ONE_TO_ONE
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_relationship_many_to_one(self):
        print()

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").many_to_one(
                        t1.ref.t1c1, "my desc"
                    ),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert relationships[t1.output_schema.t1c1].type == RelationshipType.MANY_TO_ONE
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_relationship_one_to_many(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").one_to_many(
                        t1.ref.t1c1, "my desc"
                    ),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert relationships[t1.output_schema.t1c1].type == RelationshipType.ONE_TO_MANY
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_relationship_many_to_many(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").many_to_many(
                        t1.ref.t1c1, "my desc"
                    ),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert (
            relationships[t1.output_schema.t1c1].type == RelationshipType.MANY_TO_MANY
        )
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_declared_more_than_one_relationship_same_column_raises_exception(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        with pytest.raises(ValueError):

            @node(
                type="pandas",
                output=Schema(
                    [
                        Column("t2c1", String(), "test")
                        .one_to_many(t1.output_schema.t1c1, "my desc")
                        .many_to_many(t1.output_schema.t1c1, "my desc"),
                    ]
                ),
            )
            def t2():
                return

    def test_add_extra_arguments_exists_in_column(self):

        col = Column("t1c1", String(), "test", pk=True)
        assert hasattr(col, "pk") and col.pk

    def test_print_column_without_parent(self):
        col = Column("t1c1", String())
        assert (
            str(col).replace("\n", "").replace(" ", "")
            == """Parent:NoneColumn:t1c1DataType:String()Description:''PK:False"""
        )

    def test_relationship_is_valid_on_output(self):
        def replace_chars(s: str) -> str:
            old_chars = [" ", "\n", "\t"]
            new_chars = ["", "", ""]
            translation_table = str.maketrans(dict(zip(old_chars, new_chars)))
            return s.translate(translation_table)

        @node(type="pyspark", output=Schema(Column("node_1_id", String(), "node_1 id")))
        def node_1():
            return None

        @node(
            type="pyspark",
            output=Schema(
                Column("node_2_id", String(), "node_2 id").many_to_one(
                    node_1.ref.node_1_id, "as of"
                )
            ),
        )
        def node_2():
            return None

        @node(
            type="pyspark",
            output=Schema(
                node_2.output.node_2_id.many_to_one(node_2.ref.node_2_id, "relates")
            ),
        )
        def node_3():
            return None

        col = node_2.output_schema.node_2_id
        col_expected = """
        Parent: node_2
        Column: node_2_id
        Data Type: String()
        Description: 'node_2 id'
        Foreign Keys:
                node_2.node_2_id N:1 (as of) node_1.node_1_id
        PK: False"""
        assert replace_chars(str(col)) == replace_chars(col_expected)

        col = node_3.output_schema.node_2_id
        col_expected = """
                Parent: node_3
                Column: node_2_id
                Data Type: String()
                Description: 'node_2 id'
                Foreign Keys:
                        node_3.node_2_id N:1 (relates) node_2.node_2_id
                PK: False"""
        assert replace_chars(str(col)) == replace_chars(col_expected)

    def test_output_colum_erase_pk_and_relationships(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test", pk=True),
                ]
            ),
        )
        def t1():
            pass

        @node(
            type="pandas",
            dependencies=[t1.select("t1c1")],
            output=Schema(
                [
                    t1.output.t1c1.many_to_one(t1.ref.t1c1, "my desc"),
                ]
            ),
        )
        def t2():
            pass

        @node(
            type="pandas",
            dependencies=[t2.select("t1c1")],
            output=Schema(
                [
                    t2.output.t1c1,
                ]
            ),
        )
        def t3():
            pass

        assert not t2.output.t1c1.pk
        assert not t3.output.t1c1.pk
        assert t2.output_schema.get("t1c1").relationships
        assert not t2.output.t1c1.relationships
        assert not t3.output_schema.get("t1c1").relationships
        assert isinstance(t3.output.t1c1.set_pk(True), Column)
        assert t3.output.t1c1.set_pk(True).pk

    def test_ref_preserves_pk_and_resets_relationships(self):
        """``Node.ref`` is the FK-target accessor: it must preserve the parent
        column's ``pk`` flag (so FK validators can see it) while still
        stripping the parent's outbound relationships."""

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test", pk=True),
                ]
            ),
        )
        def t1():
            pass

        @node(
            type="pandas",
            dependencies=[t1.select("t1c1")],
            output=Schema(
                [
                    t1.output.t1c1.many_to_one(t1.ref.t1c1, "self ref"),
                ]
            ),
        )
        def t2():
            pass

        ref_col = t1.ref.t1c1
        assert ref_col.pk is True
        assert ref_col.relationships == {}

        output_col = t1.output.t1c1
        assert output_col.pk is False

        ref_col_t2 = t2.ref.t1c1
        assert ref_col_t2.pk is False
        assert ref_col_t2.relationships == {}

    def test_ref_returns_independent_copy(self):
        """Mutations on the value returned by ``Node.ref`` must not leak back
        into ``output_schema``."""

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test", pk=True),
                ]
            ),
        )
        def t1():
            pass

        ref_col = t1.ref.t1c1
        ref_col.set_pk(False)

        assert t1.output_schema.get("t1c1").pk is True

    def test_ref_used_as_fk_target_preserves_pk_in_relationship(self):
        """When ``parent.ref.<col>`` is passed to ``.many_to_one`` (or any
        relationship method), the ``Column`` stored as the FK target keeps
        its ``pk`` flag, which is the whole point of having ``ref``."""

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("person_id", String(), "person id", pk=True),
                ]
            ),
        )
        def person():
            pass

        @node(
            type="pandas",
            dependencies=[person.select("person_id")],
            output=Schema(
                [
                    Column("person_id", String(), "fk to person").many_to_one(
                        person.ref.person_id, "person fk"
                    ),
                ]
            ),
        )
        def person_event():
            pass

        relationships = person_event.output_schema.get("person_id").relationships
        assert len(relationships) == 1
        target = next(iter(relationships))
        assert target.pk is True

    def test_relationship_target_via_output_raises(self):
        """Passing a column accessed via ``parent.output.<col>`` to a
        relationship method must raise — that path strips the parent's ``pk``
        flag and so destroys the validator-facing signal."""

        @node(
            type="pandas",
            output=Schema(Column("person_id", String(), "person id", pk=True)),
        )
        def person():
            pass

        with pytest.raises(ValueError) as ex:
            Column("person_id", String(), "fk to person").many_to_one(
                person.output.person_id, "person fk"
            )

        assert "person.person_id" in str(ex.value)
        assert "`.ref`" in str(ex.value)
        assert "person.ref.person_id" in str(ex.value)

    def test_schema_with_ref_column_raises(self):
        """Embedding a ``parent.ref.<col>`` column in ``Schema(...)`` must
        raise — ``ref`` is reserved for foreign-key targets, so reusing it as
        a column-spec would silently leak the parent's ``pk`` flag."""

        @node(
            type="pandas",
            output=Schema(Column("person_id", String(), "person id", pk=True)),
        )
        def person():
            pass

        with pytest.raises(ValueError) as ex:
            Schema(person.ref.person_id)

        assert "'person_id'" in str(ex.value)
        assert "`.output`" in str(ex.value)
        assert "person.output.person_id" in str(ex.value)

    def test_relationship_target_via_output_schema_does_not_raise(self):
        """Backwards-compat: passing the raw ``parent.output_schema.<col>``
        (no marker) as a relationship target must keep working — only the
        marker-bearing ``.output`` path is rejected."""

        @node(
            type="pandas",
            output=Schema(Column("person_id", String(), "person id", pk=True)),
        )
        def person():
            pass

        col = Column("person_id", String(), "fk to person").many_to_one(
            person.output_schema.person_id, "person fk"
        )

        relationships = col.relationships
        assert len(relationships) == 1
        target = next(iter(relationships))
        assert target.name == "person_id"

    def test_schema_with_output_column_does_not_raise(self):
        """Backwards-compat: ``Schema(parent.output.<col>, ...)`` is the
        canonical column-spec inheritance path and must keep working."""

        @node(
            type="pandas",
            output=Schema(Column("person_id", String(), "person id", pk=True)),
        )
        def person():
            pass

        schema = Schema(person.output.person_id)
        assert schema.get("person_id") is not None
        assert schema.get("person_id").pk is False
