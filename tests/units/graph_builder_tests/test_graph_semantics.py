from omd_airflow_utils.lineage_core.services.lineage_graph_builder import (
    LineageGraphService,
)
from omd_airflow_utils.tests.conftest import make_edge


def test_root_and_leaf_nodes(sample_nodes):
    edges = [make_edge(1, 2), make_edge(2, 3)]
    graph_service = LineageGraphService()
    graph = graph_service.build_graph(sample_nodes, edges)
    roots, leaves = graph_service.find_sources_and_sinks(graph)
    assert set(roots) == {1}
    assert set(leaves) == {3}


def test_fan_in_across_schemas(fan_in_nodes):
    edges = [make_edge(1, 3), make_edge(2, 3)]
    graph = LineageGraphService().build_graph(fan_in_nodes, edges)
    assert set(src for src, dsts in graph.adj.items() if 3 in dsts) == {1, 2}
    assert graph.adj[1] == [3]
    assert graph.adj[2] == [3]


def test_orphan_nodes(sample_nodes):
    edges = [make_edge(1, 2)]
    graph = LineageGraphService().build_graph(sample_nodes, edges)
    assert 3 in graph.nodes()
    assert graph.adj.get(3, []) == []
    assert all(3 not in dst for dst in graph.adj.values())

