from typing import (
    Callable,
    Optional,
)

from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)
from omd_airflow_utils.lineage_core.domain.types import (
    EntityPair,
    TypedFQN,
)
from omd_airflow_utils.lineage_core.domain.use_cases import (
    LineageProcessingResult,
)
from omd_airflow_utils.lineage_core.services.lineage_executor import (
    LineageOperationExecutor,
)
from omd_airflow_utils.operators.omd_base_lineage_operator import (
    BaseLineageOperator,
)


class SyncLineageOperator(BaseLineageOperator):
    """Synchronizes lineage by computing diff between current and target state."""

    def __init__(
        self,
        metadata_conn_id: str,
        source_entities: list[TypedFQN],
        target_entities: list[TypedFQN],
        existing_edges_fetcher: Optional[Callable[[LineageAPIClient], set[tuple[str, str]]]] = None,
        executor: Optional[LineageOperationExecutor] = None,
        **kwargs,
    ):

        super().__init__(
            metadata_conn_id=metadata_conn_id,
            source_entities=source_entities,
            target_entities=target_entities,
            **kwargs
        )
        self.existing_edges_fetcher = existing_edges_fetcher
        self.executor: LineageOperationExecutor = executor or LineageOperationExecutor()

    def _execute_sequential_operations(
        self,
        client: LineageAPIClient,
        processing_result: LineageProcessingResult
    ) -> None:
        existing_edges = set()
        if self.existing_edges_fetcher:
            existing_edges = self.existing_edges_fetcher(client)

        pairs_to_add, pairs_to_delete = self.service.process_lineage_sync(
            processing_result.pairs, existing_edges
        )

        if not pairs_to_add and not pairs_to_delete:
            return

        if pairs_to_delete:
            delete_pairs = self._resolve_delete_pairs(pairs_to_delete, processing_result.pairs)
            self.executor.execute_delete_operations_sequentially(delete_pairs, client)

        if pairs_to_add:
            self.executor.execute_add_operations_sequentially(
                pairs_to_add, processing_result.entity_cache, client
            )

    def _resolve_delete_pairs(
        self,
        pairs_to_delete: set[tuple[str, str]],
        known_pairs: list[EntityPair]
    ) -> list[EntityPair]:
        """
        Resolves (fqn, fqn) pairs into full EntityPair with types using known_pairs as type source.
        """
        fqn_to_type = {}
        for pair in known_pairs:
            fqn_to_type[pair.source.fqn] = pair.source.type
            fqn_to_type[pair.target.fqn] = pair.target.type

        delete_pairs = []
        for source_fqn, target_fqn in pairs_to_delete:
            src_type = fqn_to_type.get(source_fqn)
            tgt_type = fqn_to_type.get(target_fqn)
            if src_type is None or tgt_type is None:
                raise ValueError(f'Unknown entity type for: {source_fqn} or {target_fqn}')
            delete_pairs.append(EntityPair(
                source=TypedFQN(type=src_type, fqn=source_fqn),
                target=TypedFQN(type=tgt_type, fqn=target_fqn),
            ))

        return delete_pairs