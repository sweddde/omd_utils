
from abc import ABC, abstractmethod

from omd_airflow_utils.lineage_core.domain.types import EntityType


class IEntityPathResolver(ABC):
    """Resolves API path segments for entity types."""

    @abstractmethod
    def get_path(self, entity_type: EntityType) -> str:
        raise NotImplementedError
