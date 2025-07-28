from typing import (
    Dict,
    Tuple,
)

from omd_airflow_utils.lineage_core.adapters.omd.omd_response_models import (
    OMDResponseEntity,
)
from omd_airflow_utils.lineage_core.domain.models import EntityRef
from omd_airflow_utils.lineage_core.domain.types import TypedFQN


def to_entity_ref(
    entity: TypedFQN, entity_cache: Dict[Tuple[str, str], OMDResponseEntity]
) -> EntityRef:
    """Converts a TypedFQN to an EntityRef using a cached entity lookup."""
    key = (entity.type.value, entity.fqn)
    data = entity_cache.get(key)
    if data is None:
        raise ValueError(f"Entity with FQN '{entity.fqn}' not found in cache.")
    return EntityRef(id=data.id, type=entity.type)
