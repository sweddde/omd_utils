from datetime import UTC, datetime

from operators.omd_airflow_utils.lineage_core.domain.models import Node
from operators.omd_airflow_utils.lineage_core.services.lineage_graph_builder import \
    LineageGraphService
from omd_airflow_utils.tests.conftest import make_edge


def test_long_cycle():
    now = datetime.now(UTC)
    nodes = [
        Node(id=1, name='A', db_schema='s', namespace_id=1, updated=now),
        Node(id=2, name='B', db_schema='s', namespace_id=1, updated=now),
        Node(id=3, name='C', db_schema='s', namespace_id=1, updated=now),
    ]
    edges = [make_edge(1, 2), make_edge(2, 3), make_edge(3, 1)]
    graph = LineageGraphService().build_graph(nodes, edges)
    assert (1, 2) in graph.edges()
    assert (2, 3) in graph.edges()
    assert (3, 1) in graph.edges()


def test_duplicate_edges(sample_nodes):
    edges = [make_edge(1, 2), make_edge(1, 2)]
    graph = LineageGraphService().build_graph(sample_nodes, edges)

    assert (1, 2) in graph.edges()
    assert graph.adj[1].count(2) == 2


def test_empty_nodes():
    graph = LineageGraphService().build_graph([], [])
    assert graph.number_of_nodes() == 0
    assert graph.number_of_edges() == 0