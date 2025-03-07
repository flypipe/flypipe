import os

from flypipe import node
from flypipe.cache import Cache
from flypipe.misc.dbml import build_dbml, replace_dots_except_last
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Decimal


class MyCache(Cache):
    def __init__(self, cache_name):
        self.cache_name = cache_name

    @property
    def name(self):
        return self.cache_name

    def read(self, spark):
        pass

    def write(self, spark, df):
        pass

    def exists(self, spark=None):
        pass


def assert_strings_equal_ignore_whitespace(str1, str2):
    assert str1.replace("\n", "").replace("\t", "").replace(" ", "") == str2.replace(
        "\n", ""
    ).replace("\t", "").replace(
        " ", ""
    ), "Strings are not equal (ignoring spaces, \\n and \\t)."


class TestDBML:
    def test_add_node_without_output(self):
        @node(type="pandas")
        def A():
            pass

        dbml = build_dbml(A)
        assert dbml is None

    def test_node_with_output(self):
        @node(type="pandas", output=Schema(Column("a1", String(), "")))
        def A():
            pass

        dbml = build_dbml(A)

        expected_dbml = """
            Table A {
                a1 String()
                Note: '''Managed by flypipe node `A`'''
            }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_nodes_upstream_are_mapped(self):
        @node(
            type="pandas",
            output=Schema(Column("node_a_col1", String(), "description node_a_col1")),
        )
        def A():
            pass

        @node(
            type="pandas",
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1").one_to_one(
                    A.output.node_a_col1
                )
            ),
        )
        def B(**dfs):
            pass

        @node(
            type="pandas",
            output=Schema(
                Column("node_c_col1", String(), "description node_c_col1").one_to_one(
                    B.output.node_b_col1
                )
            ),
        )
        def C():
            pass

        dbml = build_dbml([C])
        expected_dbml = """
        Table A {
            node_a_col1 String() [note: '''description node_a_col1''']
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - A.node_a_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        
        Table C {
            node_c_col1 String() [note: '''description node_c_col1''', ref: - B.node_b_col1]
            Note: '''Managed by flypipe node `C`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_nodes_independents_are_mapped(self):
        @node(
            type="pandas",
            output=Schema(Column("node_a_col1", String(), "description node_a_col1")),
        )
        def A():
            pass

        @node(
            type="pandas",
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1").one_to_one(
                    A.output.node_a_col1
                )
            ),
        )
        def B(**dfs):
            pass

        @node(
            type="pandas",
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        @node(
            type="pandas",
            output=Schema(
                Column("node_d_col1", String(), "description node_d_col1").one_to_one(
                    C.output.node_c_col1
                )
            ),
        )
        def D(**dfs):
            pass

        dbml = build_dbml([B, D])
        expected_dbml = """
        Table A {
            node_a_col1 String() [note: '''description node_a_col1''']
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - A.node_a_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        
        Table C {
            node_c_col1 String() [note: '''description node_c_col1''']
            Note: '''Managed by flypipe node `C`'''
        }
        
        Table D {
            node_d_col1 String() [note: '''description node_d_col1''', ref: - C.node_c_col1]
            Note: '''Managed by flypipe node `D`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_only_nodes_in_list_is_mapped(self):
        @node(type="pandas")
        def A():
            pass

        @node(
            type="pandas",
            tags=["dummy"],
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        @node(
            type="pandas",
            tags=["dbml"],
            output=Schema(Column("node_d_col1", String(), "description node_d_col1")),
        )
        def D():
            pass

        @node(
            type="pandas",
            dependencies=[
                A.alias("df_a"),
            ],
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1")
                .one_to_one(C.output.node_c_col1)
                .one_to_one(D.output.node_d_col1)
            ),
        )
        def B(**dfs):
            pass

        dbml = build_dbml([B])
        expected_dbml = """
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - D.node_d_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        
        Table C {
            node_c_col1 String() [note: '''description node_c_col1''']
            Note: '''Managed by flypipe node `C`'''
        }
        
        Table D {
            node_d_col1 String() [note: '''description node_d_col1''']
            Note: '''Managed by flypipe node `D`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_only_nodes_in_list_and_with_tags_are_mapped(self):
        @node(type="pandas")
        def A():
            pass

        @node(
            type="pandas",
            tags=["dummy"],
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        @node(
            type="pandas",
            tags=["dbml"],
            output=Schema(Column("node_d_col1", String(), "description node_d_col1")),
        )
        def D():
            pass

        @node(
            type="pandas",
            dependencies=[
                A.alias("df_a"),
            ],
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1")
                .one_to_one(C.output.node_c_col1)
                .one_to_one(D.output.node_d_col1)
            ),
        )
        def B(**dfs):
            pass

        dbml = build_dbml([B], only_nodes_with_tags=["dbml"])
        expected_dbml = """
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - D.node_d_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        
        Table D {
            node_d_col1 String() [note: '''description node_d_col1''']
            Note: '''Managed by flypipe node `D`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_only_nodes_in_list_and_with_cache_are_mapped(self):
        @node(type="pandas")
        def A():
            pass

        @node(
            type="pandas",
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        @node(
            type="pandas",
            cache=MyCache("mycache"),
            output=Schema(Column("node_d_col1", String(), "description node_d_col1")),
        )
        def D():
            pass

        @node(
            type="pandas",
            dependencies=[
                A.alias("df_a"),
            ],
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1")
                .one_to_one(C.output.node_c_col1)
                .one_to_one(D.output.node_d_col1)
            ),
        )
        def B(**dfs):
            pass

        dbml = build_dbml([B], only_nodes_with_cache=True)
        expected_dbml = """
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - mycache.node_d_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        
        Table mycache {
            node_d_col1 String() [note: '''description node_d_col1''']
            Note: '''Managed by flypipe node `D`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_only_nodes_in_list_and_with_cache_and_tags_are_mapped(self):
        @node(type="pandas")
        def A():
            pass

        @node(
            type="pandas",
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        @node(
            type="pandas",
            cache=MyCache("mycache"),
            output=Schema(Column("node_d_col1", String(), "description node_d_col1")),
        )
        def D():
            pass

        @node(
            type="pandas",
            tags=["dbml"],
            cache=MyCache("E_mycache"),
            output=Schema(Column("node_e_col1", String(), "description node_e_col1")),
        )
        def E():
            pass

        @node(
            type="pandas",
            dependencies=[
                A.alias("df_a"),
            ],
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1")
                .one_to_one(C.output.node_c_col1)
                .one_to_one(D.output.node_d_col1)
                .one_to_one(E.output.node_e_col1)
            ),
        )
        def B(**dfs):
            pass

        dbml = build_dbml(
            [B], only_nodes_with_cache=True, only_nodes_with_tags=["dbml"]
        )
        expected_dbml = """
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - E_mycache.node_e_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        
        Table E_mycache {
            node_e_col1 String() [note: '''description node_e_col1''']
            Note: '''Managed by flypipe node `E`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_save_file(self):
        @node(
            type="pandas",
            output=Schema(Column("node_a_col1", String(), "description node_a_col1")),
        )
        def A():
            pass

        @node(
            type="pandas",
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1").one_to_one(
                    A.output.node_a_col1
                )
            ),
        )
        def B(**dfs):
            pass

        response = build_dbml([B], file_path_name="test.dbml")
        assert response is None
        with open("test.dbml", "r") as f:
            dbml = "".join(f.readlines())

        expected_dbml = """
        Table A {
            node_a_col1 String() [note: '''description node_a_col1''']
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - A.node_a_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

        os.remove("test.dbml")

    def test_only_parent_with_cache(self):
        @node(
            type="pandas",
            cache=MyCache("mycache"),
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        @node(
            type="pandas",
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1").one_to_one(
                    C.output.node_c_col1
                )
            ),
        )
        def B(**dfs):
            pass

        dbml = build_dbml([B])
        expected_dbml = """
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - mycache.node_c_col1]
            Note: '''Managed by flypipe node `B`'''
        }
        
        Table mycache {
            node_c_col1 String() [note: '''description node_c_col1''']
            Note: '''Managed by flypipe node `C`'''
        }
        """
        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_replace_dots_except_last(self):
        assert replace_dots_except_last("a.b") == "a.b"
        assert replace_dots_except_last("a.b.c") == "a_b.c"
        assert replace_dots_except_last("a.b.c.d") == "a_b_c.d"
        assert replace_dots_except_last("c.a.b.c.d") == "c_a_b_c.d"

    def test_cache_with_dots(self):
        @node(
            type="pandas",
            description="this is node C",
            cache=MyCache("a.b.c"),
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        @node(
            type="pandas",
            description="this is node B",
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1").one_to_one(
                    C.output.node_c_col1, "has"
                )
            ),
        )
        def B(**dfs):
            pass

        dbml = build_dbml([B])
        expected_dbml = """
        Table B {
        node_b_col1 String() [note: '''description node_b_col1''', ref: - a_b.c.node_c_col1]
        Note: '''Managed by flypipe node `B`
    
        this is node B'''
        }
        
        Table a_b.c {
            node_c_col1 String() [note: '''description node_c_col1''']
            Note: '''Managed by flypipe node `C`
        
        this is node C'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_save_file_decimal(self):
        @node(
            type="pandas",
            output=Schema(
                Column("node_a_col1", Decimal(15, 2), "description node_a_col1")
            ),
        )
        def A():
            pass

        @node(
            type="pandas",
            output=Schema(
                Column("node_b_col1", String(), "description node_b_col1").one_to_one(
                    A.output.node_a_col1
                )
            ),
        )
        def B(**dfs):
            pass

        response = build_dbml([B], file_path_name="test.dbml")
        assert response is None
        with open("test.dbml", "r") as f:
            dbml = "".join(f.readlines())

        expected_dbml = """
        Table A {
            node_a_col1 Decimal(15,2) [note: '''description node_a_col1''']
        
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table B {
            node_b_col1 String() [note: '''description node_b_col1''', ref: - A.node_a_col1]
        
            Note: '''Managed by flypipe node `B`'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

        os.remove("test.dbml")
