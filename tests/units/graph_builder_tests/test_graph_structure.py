import pytest

from omd_airflow_utils.lineage_core.services.lineage_graph_builder import (
    LineageGraphService,
)
from omd_airflow_utils.tests.conftest import make_edge


def test_simple_lineage_graph(sample_nodes):
    edges = [make_edge(1, 2), make_edge(2, 3)]
    graph = LineageGraphService().build_graph(sample_nodes, edges)

    assert set(graph.nodes()) == {1, 2, 3}
    assert set(graph.edges()) == {(1, 2), (2, 3)}
    assert (1, 3) not in graph.edges()


def test_cyclic_graph(sample_nodes):
    edges = [make_edge(1, 2), make_edge(2, 1)]
    graph = LineageGraphService().build_graph(sample_nodes, edges)
    assert (1, 2) in graph.edges()
    assert (2, 1) in graph.edges()


def test_disconnected_graph(sample_nodes):
    edges = []
    graph = LineageGraphService().build_graph(sample_nodes, edges)
    assert graph.number_of_nodes() == 3
    assert graph.number_of_edges() == 0