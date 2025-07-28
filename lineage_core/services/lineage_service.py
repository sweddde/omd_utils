from typing import Optional

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
    TypedFQN,
)
from omd_airflow_utils.lineage_core.domain.use_cases import (
    LineageProcessingResult,
    LineageRequest,
)
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_diff_calculator import (
    LineageSyncService,
)
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_metadata_cache import (
    LineageMetadataService,
)
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_pair_generator import (
    LineagePairGenerationService,
)


class LineageService:
    def __init__(
        self,
        metadata_service: LineageMetadataService,
        pair_generation_service: LineagePairGenerationService,
        sync_service: LineageSyncService,
    ):
        self._metadata_service = metadata_service
        self._pair_generation_service = pair_generation_service
        self._sync_service = sync_service

    @classmethod
    def create_default(cls) -> 'LineageService':
        return cls(
            metadata_service=LineageMetadataService(),
            pair_generation_service=LineagePairGenerationService(),
            sync_service=LineageSyncService(),
        )

    def prepare_lineage_processing(
        self,
        request: LineageRequest,
        client: LineageAPIClient
    ) -> LineageProcessingResult:
        validated_sources = self._validate_entities(request.source_entities, 'source')
        validated_targets = self._validate_entities(request.target_entities, 'target')

        pairs = self._pair_generation_service.generate_pairs(
            request.mapping, validated_sources, validated_targets
        )

        if not pairs:
            return LineageProcessingResult(
                pairs=[],
                entity_cache={},
                validated_sources=validated_sources,
                validated_targets=validated_targets
            )

        unique_entities = self._extract_unique_entities(pairs)
        entity_cache = self._metadata_service.preload_entities(client, unique_entities)

        return LineageProcessingResult(
            pairs=pairs,
            entity_cache=entity_cache,
            validated_sources=validated_sources,
            validated_targets=validated_targets
        )

    def process_lineage_sync(
        self,
        new_pairs: list[EntityPair],
        existing_edges: set[tuple[str, str]],
        updated_entities: Optional[set[str]] = None
    ) -> tuple[list[EntityPair], set[tuple[str, str]]]:
        return self._sync_service.diff_lineage_pairs(new_pairs, existing_edges, updated_entities)

    def extract_graph_lineage(
            self,
            nodes: list[Node],
            edges: list[LineageEdge],
            db_context: DatabaseContext,
            path_cutoff: int,
            collapse_triggers: bool = False,
            trigger_operator_id: int = None,
            client: Optional[LineageAPIClient] = None,
    ) -> list[EntityPair]:
        return self._pair_generation_service.extract_pairs_from_graph_paths(
            nodes=nodes,
            edges=edges,
            db_context=db_context,
            client=client,
            validate_existence=True,
            collapse_triggers=collapse_triggers,
            trigger_operator_id=trigger_operator_id,
        )

    def resolve_entity_fqn(
        self,
        entity_id: str,
        client: LineageAPIClient,
        entity_type: EntityType
    ) -> str:
        return self._metadata_service.get_fqn_from_entity_id(entity_id, client, entity_type)

    def _validate_entities(self, entities: list[TypedFQN], label: str) -> list[TypedFQN]:
        if entities:
            self._metadata_service.validate_entities(entities, label)
        return entities

    def _extract_unique_entities(self, pairs: list[EntityPair]) -> list[TypedFQN]:
        entities = []
        seen_fqns = set()
        for pair in pairs:
            for entity in [pair.source, pair.target]:
                if entity.fqn not in seen_fqns:
                    entities.append(entity)
                    seen_fqns.add(entity.fqn)
        return entities