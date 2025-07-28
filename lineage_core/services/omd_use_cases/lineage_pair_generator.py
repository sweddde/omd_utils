import logging
from itertools import product
from typing import List, Optional

from airflow.exceptions import AirflowException

from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)
from omd_airflow_utils.lineage_core.domain.models import (
    DatabaseContext,
    LineageEdge,
    Node,
)
from omd_airflow_utils.lineage_core.domain.types import (
    EntityPair,
    EntityType,
    MappingType,
    TypedFQN,
)
from omd_airflow_utils.lineage_core.services.lineage_graph_builder import (
    LineageGraphService,
)

logger = logging.getLogger(__name__)

class LineagePairGenerationService:
    """Generates lineage pairs from explicit mappings or inferred graph paths."""

    def __init__(self, graph_service: Optional[LineageGraphService] = None):
        self._graph_service = graph_service or LineageGraphService()

    def generate_pairs(
        self,
        mapping: MappingType,
        sources: List[TypedFQN],
        targets: List[TypedFQN],
    ) -> List[EntityPair]:
        """Creates pairs between source and target entities using the selected mapping strategy."""
        if not sources or not targets:
            return []

        if mapping == MappingType.ONE_TO_ONE:
            if len(sources) != len(targets):
                raise AirflowException(
                    'one_to_one mapping requires equal source and target lengths.'
                )
            return [EntityPair(s, t) for s, t in zip(sources, targets)]

        return [EntityPair(s, t) for s, t in product(sources, targets)]

    def extract_pairs_from_graph_paths(
            self,
            nodes: List[Node],
            edges: List[LineageEdge],
            db_context: DatabaseContext,
            client: Optional[LineageAPIClient] = None,
            validate_existence: bool = True,
            collapse_triggers: bool = False,
            trigger_operator_id: Optional[int] = None,
    ) -> List[EntityPair]:
        """Extracts only direct lineage pairs from graph edges (no transitive links)."""

        if validate_existence and client:
            nodes, edges = self.filter_existing_nodes_only(nodes, edges, client, db_context)
            if not nodes:
                return []

        try:
            graph = self._graph_service.build_graph(
                nodes,
                edges,
                collapse_triggers=collapse_triggers,
                trigger_operator_id=trigger_operator_id
            )
        except Exception as e:
            raise AirflowException(f'Failed to build lineage graph: {e}')

        id_to_node = {node.id: node for node in nodes}
        pairs: set[EntityPair] = set()

        for from_id, to_id in graph.edges():
            from_node = id_to_node.get(from_id)
            to_node = id_to_node.get(to_id)
            if not from_node or not to_node:
                continue

            source = TypedFQN(EntityType.TABLE, db_context.fqn(from_node.db_schema, from_node.name))
            target = TypedFQN(EntityType.TABLE, db_context.fqn(to_node.db_schema, to_node.name))
            pairs.add(EntityPair(source=source, target=target))

        return list(pairs)

    def filter_existing_nodes_only(
        self,
        nodes: List[Node],
        edges: List[LineageEdge],
        client: LineageAPIClient,
        db_context: DatabaseContext
    ) -> tuple[List[Node], List[LineageEdge]]:
        """Removes nodes and edges for entities not present in OMD."""
        existing_nodes = []
        valid_node_ids = set()

        for node in nodes:
            fqn = db_context.fqn(schema=node.db_schema, name=node.name)
            try:
                client.get_entity(entity_type=EntityType.TABLE, fqn=fqn)
                existing_nodes.append(node)
                valid_node_ids.add(node.id)
            except AirflowException as e:
                if '404' not in str(e):
                    raise

        valid_edges = [
            edge for edge in edges
            if int(edge.from_entity.id) in valid_node_ids and int(edge.to_entity.id) in valid_node_ids
        ]

        return existing_nodes, valid_edges
