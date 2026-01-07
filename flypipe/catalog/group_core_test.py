"""Tests for the Group class"""

import os
import pytest

from flypipe.catalog.group import Group


class MockNode:
    """Mock node for testing"""

    def __init__(self, key):
        self.key = key


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "CORE",
    reason="Core tests require RUN_MODE=CORE",
)
class TestGroupCore:
    """Test suite for the Group class"""

    def test_group_initialization(self):
        """Test that a group can be initialized with a name"""
        group = Group("Test Group")

        assert group.name == "Test Group"
        assert group.group_id == "test_group"
        assert group.nodes == []

    def test_get_id_lowercase_conversion(self):
        """Test that group ID is converted to lowercase"""
        group = Group("UPPERCASE GROUP")

        assert group.group_id == "uppercase_group"

    def test_get_id_space_to_underscore(self):
        """Test that spaces in group name are converted to underscores"""
        group = Group("Group With Spaces")

        assert group.group_id == "group_with_spaces"

    def test_get_id_multiple_spaces(self):
        """Test that multiple consecutive spaces are handled"""
        group = Group("Group  With   Multiple    Spaces")

        assert group.group_id == "group__with___multiple____spaces"

    def test_get_id_mixed_case_and_spaces(self):
        """Test ID generation with mixed case and spaces"""
        group = Group("Training Thing")

        assert group.group_id == "training_thing"

    def test_add_single_node(self):
        """Test adding a single node to a group"""
        group = Group("Test Group")
        node = MockNode("node_1")

        group.add_node(node)

        assert len(group.nodes) == 1
        assert group.nodes[0].key == "node_1"

    def test_add_multiple_nodes(self):
        """Test adding multiple nodes to a group"""
        group = Group("Test Group")
        node1 = MockNode("node_1")
        node2 = MockNode("node_2")
        node3 = MockNode("node_3")

        group.add_node(node1)
        group.add_node(node2)
        group.add_node(node3)

        assert len(group.nodes) == 3
        assert group.nodes[0].key == "node_1"
        assert group.nodes[1].key == "node_2"
        assert group.nodes[2].key == "node_3"

    def test_get_def_empty_group(self):
        """Test get_def() returns correct structure for empty group"""
        group = Group("Empty Group")

        definition = group.get_def()

        assert definition == {"id": "empty_group", "name": "Empty Group", "nodes": []}

    def test_get_def_with_nodes(self):
        """Test get_def() includes all node keys"""
        group = Group("Training Thing")
        node1 = MockNode("flypipe_catalog_test_t1")
        node2 = MockNode("flypipe_catalog_test_t2")

        group.add_node(node1)
        group.add_node(node2)

        definition = group.get_def()

        assert definition == {
            "id": "training_thing",
            "name": "Training Thing",
            "nodes": ["flypipe_catalog_test_t1", "flypipe_catalog_test_t2"],
        }

    def test_get_def_preserves_original_name(self):
        """Test that get_def() preserves the original name capitalization"""
        group = Group("My Special Group")

        definition = group.get_def()

        assert definition["name"] == "My Special Group"
        assert definition["id"] == "my_special_group"

    def test_nodes_list_is_mutable(self):
        """Test that nodes can be added after group creation"""
        group = Group("Test Group")

        assert len(group.nodes) == 0

        group.add_node(MockNode("node_1"))
        assert len(group.nodes) == 1

        group.add_node(MockNode("node_2"))
        assert len(group.nodes) == 2
