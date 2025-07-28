
from airflow.exceptions import AirflowException

from omd_airflow_utils.lineage_core.domain.interfaces import (
    IEntityPathResolver,
)
from omd_airflow_utils.lineage_core.domain.types import EntityType


class EntityRegistry:
    _ENTITY_PATHS: dict[EntityType, str] = {
        EntityType.TABLE: 'tables',
        EntityType.DASHBOARD: 'dashboards',
        EntityType.PIPELINE: 'pipelines',
        EntityType.TOPIC: 'topics',
    }

    @classmethod
    def get_path(cls, entity_type: EntityType) -> str:
        try:
            return cls._ENTITY_PATHS[entity_type]
        except KeyError:
            raise AirflowException(f'Unsupported entity type: {entity_type}')


class EntityRegistryPathResolver(IEntityPathResolver):
    def get_path(self, entity_type: EntityType) -> str:
        return EntityRegistry.get_path(entity_type)
