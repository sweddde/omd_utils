from typing import (
    List,
    Set,
    Tuple,
    Optional,
)

from omd_airflow_utils.lineage_core.domain.types import EntityPair


class LineageSyncService:
    """Computes diff between existing and new lineage edges, optionally scoped."""

    def diff_lineage_pairs(
        self,
        new_pairs: List[EntityPair],
        existing_edges: Set[Tuple[str, str]],
        affected_fqns_scope: Optional[Set[str]] = None
    ) -> Tuple[List[EntityPair], Set[Tuple[str, str]]]:
        """Returns pairs to add and FQN edges to delete based on current scope and difference."""
        new_edges = {(pair.source.fqn, pair.target.fqn) for pair in new_pairs}
        edges_to_add = new_edges - existing_edges
        pairs_to_add = [pair for pair in new_pairs if (pair.source.fqn, pair.target.fqn) in edges_to_add]

        if affected_fqns_scope:
            edges_to_delete = {
                edge for edge in (existing_edges - new_edges)
                if edge[0] in affected_fqns_scope or edge[1] in affected_fqns_scope
            }
        else:
            edges_to_delete = existing_edges - new_edges

        return pairs_to_add, edges_to_delete
