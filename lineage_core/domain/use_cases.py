from dataclasses import dataclass

from omd_airflow_utils.lineage_core.adapters.omd.omd_response_models import (
    OMDResponseEntity,
)
from omd_airflow_utils.lineage_core.domain.types import (
    EntityPair,
    MappingType,
    TypedFQN,
)


@dataclass
class LineageRequest:
    source_entities: list[TypedFQN]
    target_entities: list[TypedFQN]
    mapping: MappingType


@dataclass
class LineageProcessingResult:
    pairs: list[EntityPair]
    entity_cache: dict[tuple[str, str], OMDResponseEntity]
    validated_sources: list[TypedFQN]
    validated_targets: list[TypedFQN]


@dataclass
class LineageSyncRequest:
    new_pairs: list[EntityPair]
    existing_edges: set[tuple[str, str]]


@dataclass
class LineageSyncResult:
    pairs_to_add: list[EntityPair]
    pairs_to_delete: set[tuple[str, str]]

    @property
    def total_changes(self) -> int:
        return len(self.pairs_to_add) + len(self.pairs_to_delete)