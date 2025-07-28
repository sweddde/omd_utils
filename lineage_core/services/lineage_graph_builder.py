import logging

from omd_airflow_utils.lineage_core.domain.models import LineageEdge, Node
from omd_airflow_utils.lineage_core.utils.simple_graph import SimpleDiGraph

logger = logging.getLogger(__name__)

class LineageGraphService:
    """Builds and transforms lineage graphs from nodes and edges."""

    def build_graph(
        self,
        nodes: list[Node],
        edges: list[LineageEdge],
        collapse_triggers: bool = False,
        trigger_operator_id: int = None,
    ) -> SimpleDiGraph:
        """Constructs a directed graph from nodes and edges with optional trigger collapsing."""
        graph = SimpleDiGraph()
        node_ids = set()

        for node in nodes:
            graph.add_node(node.id, **node.model_dump())
            node_ids.add(node.id)

        for edge in edges:
            src = int(edge.from_entity.id)
            dst = int(edge.to_entity.id)
            if src in node_ids and dst in node_ids:
                graph.add_edge(src, dst)

        if collapse_triggers and trigger_operator_id is not None:
            graph = self.collapse_trigger_nodes(graph, trigger_operator_id)
        sources, sinks = self.find_sources_and_sinks(graph)

        logger.info(
            'Graph stats: %d nodes, %d edges, %d sources, %d sinks',
            len(graph.nodes()),
            sum(len(adj) for adj in graph.adj.values()),
            len(sources),
            len(sinks)
        )
        return graph

    def find_sources_and_sinks(self, graph: SimpleDiGraph) -> tuple[list[int], list[int]]:
        """Identifies source (no incoming) and sink (no outgoing) nodes in the graph."""
        in_deg = graph.in_degree()
        out_deg = graph.out_degree()
        sources = [n for n in graph.nodes() if in_deg.get(n, 0) == 0]
        sinks = [n for n in graph.nodes() if out_deg.get(n, 0) == 0]
        return sources, sinks

    def collapse_trigger_nodes(self, graph: SimpleDiGraph, trigger_operator_id: int) -> SimpleDiGraph:
        """Removes trigger nodes from graph and reconnects their predecessors to successors."""
        trigger_nodes = [
            node_id for node_id in graph.nodes()
            if graph.get_node(node_id).get('operator_id') == trigger_operator_id
        ]

        if not trigger_nodes:
            return graph

        new_graph = SimpleDiGraph()

        for node_id in graph.nodes():
            if node_id not in trigger_nodes:
                node_data = graph.get_node(node_id)
                new_graph.add_node(node_id, **node_data)

        for trigger_id in trigger_nodes:
            incoming_nodes = [
                node_id for node_id in graph.nodes()
                if trigger_id in graph.adj.get(node_id, [])
            ]
            outgoing_nodes = graph.adj.get(trigger_id, [])

            for from_node in incoming_nodes:
                for to_node in outgoing_nodes:
                    if (
                        from_node not in trigger_nodes and
                        to_node not in trigger_nodes and
                        from_node in new_graph.nodes() and
                        to_node in new_graph.nodes()
                    ):
                        new_graph.add_edge(from_node, to_node)

        for node_id in graph.nodes():
            if node_id not in trigger_nodes:
                for target_id in graph.adj.get(node_id, []):
                    if (
                        target_id not in trigger_nodes and
                        node_id in new_graph.nodes() and
                        target_id in new_graph.nodes()
                    ):
                        new_graph.add_edge(node_id, target_id)

        return new_graph
