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
            Table a {
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
                    B.output.node_b_col1, "has"
                )
            ),
        )
        def C():
            pass

        dbml = build_dbml([C])
        expected_dbml = """
        Table a {
            node_a_col1 String() [note: '''description node_a_col1''']
        
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - a.node_a_col1
        
        Table c {
            node_c_col1 String() [note: '''description node_c_col1''']
        
            Note: '''Managed by flypipe node `C`'''
        }
        
        Ref has: c.node_c_col1 - b.node_b_col1
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
        Table a {
            node_a_col1 String() [note: '''description node_a_col1''']
        
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - a.node_a_col1
        
        Table c {
            node_c_col1 String() [note: '''description node_c_col1''']
        
            Note: '''Managed by flypipe node `C`'''
        }
        
        Table d {
            node_d_col1 String() [note: '''description node_d_col1''']
        
            Note: '''Managed by flypipe node `D`'''
        }
        
        Ref : d.node_d_col1 - c.node_c_col1
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
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - c.node_c_col1
        Ref : b.node_b_col1 - d.node_d_col1
        
        Table c {
            node_c_col1 String() [note: '''description node_c_col1''']
        
            Note: '''Managed by flypipe node `C`'''
        }
        
        Table d {
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
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - d.node_d_col1
        
        Table d {
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
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - mycache.node_d_col1
        
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
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - e_mycache.node_e_col1
        
        Table e_mycache {
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
        Table a {
            node_a_col1 String() [note: '''description node_a_col1''']
        
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - a.node_a_col1
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
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - mycache.node_c_col1
        
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
        Table a_b.c {
            node_c_col1 String() [note: '''description node_c_col1''']
        
            Note: '''Managed by flypipe node `C`
    
            this is node C'''
        }
        
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`
        
            this is node B'''
        }
        Ref has: b.node_b_col1 - a_b.c.node_c_col1
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
        Table a {
            node_a_col1 Decimal(15,2) [note: '''description node_a_col1''']
        
            Note: '''Managed by flypipe node `A`'''
        }
        
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`'''
        }
        
        Ref : b.node_b_col1 - a.node_a_col1
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

        os.remove("test.dbml")

    def test_table_color(self):
        class MyCacheColor(Cache):
            def __init__(self, cache_name):
                self.cache_name = cache_name

            @property
            def color(self):
                return "#FFFFF"

            @property
            def name(self):
                return self.cache_name

            def read(self, spark):
                pass

            def write(self, spark, df):
                pass

            def exists(self, spark=None):
                pass

        @node(
            type="pandas",
            description="this is node C",
            cache=MyCacheColor("a.b.c"),
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
        Table a_b.c [headercolor: #FFFFF] {
            node_c_col1 String() [note: '''description node_c_col1''']
        
            Note: '''Managed by flypipe node `C`
        
        this is node C'''
        }
        
        Table b {
            node_b_col1 String() [note: '''description node_b_col1''']
        
            Note: '''Managed by flypipe node `B`
        
        this is node B'''
        }
        
        Ref has: b.node_b_col1 - a_b.c.node_c_col1
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)

    def test_lower_case_non_space_table_cache_name(self):
        @node(
            type="pandas",
            description="this is node C",
            cache=MyCache("A B"),
            output=Schema(Column("node_c_col1", String(), "description node_c_col1")),
        )
        def C():
            pass

        dbml = build_dbml([C])

        expected_dbml = """
        Table a_b {
            node_c_col1 String() [note: '''description node_c_col1''']
        
            Note: '''Managed by flypipe node `C`
        
        this is node C'''
        }
        """

        assert_strings_equal_ignore_whitespace(dbml, expected_dbml)
