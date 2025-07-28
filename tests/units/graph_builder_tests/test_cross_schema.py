from omd_airflow_utils.lineage_core.services.lineage_graph_builder import (
    LineageGraphService,
)
from omd_airflow_utils.tests.conftest import make_edge


def get_parents(graph, node_id: int) -> list[int]:
    return [src for src, dsts in graph.adj.items() if node_id in dsts]


def test_cross_schema_lineage(cross_schema_nodes):
    edges = [make_edge(1, 2), make_edge(2, 3)]
    graph = LineageGraphService().build_graph(cross_schema_nodes, edges)
    assert graph.adj.get(1, []) == [2]
    assert get_parents(graph, 2) == [1]
    assert graph.adj.get(2, []) == [3]
    assert get_parents(graph, 3) == [2]
    assert graph.adj.get(3, []) == []


def test_cross_schema_multiple_children(cross_schema_nodes_with_branching):
    edges = [make_edge(1, 2), make_edge(1, 4), make_edge(2, 3)]
    graph = LineageGraphService().build_graph(cross_schema_nodes_with_branching, edges)
    assert set(graph.adj[1]) == {2, 4}


def test_cross_schema_cycle(cyclic_nodes):
    edges = [make_edge(1, 2), make_edge(2, 1)]
    graph = LineageGraphService().build_graph(cyclic_nodes, edges)
    assert (1, 2) in graph.edges()
    assert (2, 1) in graph.edges()
    assert set(graph.adj[1]) == {2}
    assert set(graph.adj[2]) == {1}
    assert set(get_parents(graph, 1)) == {2}
    assert set(get_parents(graph, 2)) == {1}