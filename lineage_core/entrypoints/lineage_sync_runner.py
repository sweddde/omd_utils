
from typing import (
    List,
    Optional,
    Set,
    Tuple,
)

from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)
from omd_airflow_utils.lineage_core.domain.types import (
    EntityPair,
    EntityType,
    MappingType,
    TypedFQN,
    LineageLoadType,
)
from omd_airflow_utils.lineage_core.domain.use_cases import LineageRequest
from omd_airflow_utils.lineage_core.services.lineage_executor import (
    LineageOperationExecutor,
)
from omd_airflow_utils.lineage_core.services.lineage_service import (
    LineageService,
)


class LineageSyncRunner:
    """Executes lineage synchronization based on configuration and pair sets."""

    def __init__(
        self,
        client: LineageAPIClient,
        service: LineageService,
        executor: LineageOperationExecutor
    ):
        self.client = client
        self.service = service
        self.executor = executor

    def run_sync(
        self,
        lineage_pairs: List[EntityPair],
        load_type: str,
        clean_before_update: bool,
        schema_filter: Set[str],
        affected_fqns: Optional[Set[str]] = None
    ) -> None:
        if not lineage_pairs and load_type == LineageLoadType.INIT:
            return

        request = LineageRequest(
            source_entities=[p.source for p in lineage_pairs],
            target_entities=[p.target for p in lineage_pairs],
            mapping=MappingType.ONE_TO_ONE,
        )
        result = self.service.prepare_lineage_processing(request, client=self.client)

        if clean_before_update:
            self._full_reload(lineage_pairs, result.entity_cache, schema_filter)
        elif load_type == LineageLoadType.INCREMENTAL and affected_fqns:
            self._diff_sync(lineage_pairs, result.entity_cache, affected_fqns)
        else:
            self._full_reload(lineage_pairs, result.entity_cache, schema_filter)

    def _full_reload(
        self,
        lineage_pairs: List[EntityPair],
        entity_cache: dict,
        schema_filter: Set[str]
    ) -> None:
        """Performs full lineage reload: deletes and re-creates all edges."""
        target_fqns = self._extract_target_schema_fqns(lineage_pairs, schema_filter)

        existing_edges = self.client.get_edges_for_scope(
            fqns=target_fqns,
            schema_filter=schema_filter
        )

        if existing_edges:
            delete_pairs = self._convert(existing_edges)
            self.executor.execute_delete_operations_sequentially(delete_pairs, self.client)

        self.executor.execute_add_operations_sequentially(lineage_pairs, entity_cache, self.client)

    def _diff_sync(
        self,
        lineage_pairs: List[EntityPair],
        entity_cache: dict,
        affected_fqns: Set[str]
    ) -> None:
        """Performs differential sync based on current and existing edges."""
        existing = self.client.get_edges_for_scope(affected_fqns)
        current = {(p.source.fqn, p.target.fqn) for p in lineage_pairs}

        to_add = current - existing
        to_delete = existing - current

        delete_pairs = self._convert(to_delete)
        add_pairs = [p for p in lineage_pairs if (p.source.fqn, p.target.fqn) in to_add]

        if delete_pairs:
            self.executor.execute_delete_operations_sequentially(delete_pairs, self.client)
        if add_pairs:
            self.executor.execute_add_operations_sequentially(add_pairs, entity_cache, self.client)

    def _convert(self, fqn_pairs: Set[Tuple[str, str]]) -> List[EntityPair]:
        """Converts (source_fqn, target_fqn) pairs into EntityPair objects."""
        return [
            EntityPair(
                source=TypedFQN(type=EntityType.TABLE, fqn=src),
                target=TypedFQN(type=EntityType.TABLE, fqn=dst)
            ) for src, dst in fqn_pairs
        ]

    def _extract_target_schema_fqns(
            self,
            pairs: List[EntityPair],
            schema_filter: Set[str]
    ) -> Set[str]:
        """Extracts FQNs belonging to target schemas using schema_filter."""

        all_entities = []
        for pair in pairs:
            all_entities.append(pair.source)
            all_entities.append(pair.target)

        target_fqns = set()
        for entity in all_entities:
            fqn_parts = entity.fqn.split('.')

            if len(fqn_parts) < 3:
                continue

            schema_name = fqn_parts[-2]

            if schema_name in schema_filter:
                target_fqns.add(entity.fqn)

        return target_fqns
