from typing import (
    Dict,
    List,
    Tuple,
)
from airflow.exceptions import AirflowException

from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)
from omd_airflow_utils.lineage_core.adapters.omd.omd_response_models import (
    OMDResponseEntity,
)
from omd_airflow_utils.lineage_core.domain.interfaces import (
    IEntityPathResolver,
)
from omd_airflow_utils.lineage_core.domain.registry import (
    EntityRegistryPathResolver,
)
from omd_airflow_utils.lineage_core.domain.types import EntityType, TypedFQN


class EntityCache:
    """Manages entity caching with FQN and ID mappings."""

    def __init__(self):
        self._entity_cache: Dict[str, OMDResponseEntity] = {}
        self._id_to_fqn_cache: Dict[str, str] = {}

    def get_entity(self, entity_type: EntityType, fqn: str) -> OMDResponseEntity:
        cache_key = self._cache_key_fqn(entity_type, fqn)
        return self._entity_cache.get(cache_key)

    def cache_entity(self, entity_type: EntityType, fqn: str, entity: OMDResponseEntity) -> None:
        cache_key = self._cache_key_fqn(entity_type, fqn)
        self._entity_cache[cache_key] = entity

        if entity.id and entity.fully_qualified_name:
            self._id_to_fqn_cache[entity.id] = entity.fully_qualified_name
            canonical_key = self._cache_key_fqn(entity_type, entity.fully_qualified_name)
            self._entity_cache[canonical_key] = entity

    def get_fqn_by_id(self, entity_id: str) -> str:
        return self._id_to_fqn_cache.get(entity_id)

    def cache_fqn_mapping(self, entity_id: str, fqn: str) -> None:
        self._id_to_fqn_cache[entity_id] = fqn

    def _cache_key_fqn(self, entity_type: EntityType, fqn: str) -> str:
        return f"{entity_type.value}:{fqn}"


class EntityValidator:
    """Validates entity types and structure."""

    def __init__(self, path_resolver: IEntityPathResolver):
        self._path_resolver = path_resolver

    def validate_entities(self, entities: List[TypedFQN], label: str) -> None:
        for item in entities:
            if not isinstance(item, TypedFQN):
                raise AirflowException(
                    f'Each item in {label} must be a TypedFQN instance, '
                    f'got {type(item)}'
                )
            try:
                self._path_resolver.get_path(item.type)
            except Exception as e:
                raise AirflowException(
                    f'Invalid entity type {item.type} for FQN {item.fqn} '
                    f'in {label}: {e}'
                )


class EntityFetcher:
    """Handles entity fetching from API."""

    def __init__(self, cache: EntityCache):
        self._cache = cache

    def fetch_entity(self, client: LineageAPIClient, entity: TypedFQN) -> OMDResponseEntity:
        """Fetches single entity and caches result."""
        result = client.get_entity(entity_type=entity.type, fqn=entity.fqn)
        self._cache.cache_entity(entity.type, entity.fqn, result)
        return result

    def fetch_entity_by_id(self, client: LineageAPIClient, entity_type: EntityType,
                           entity_id: str) -> OMDResponseEntity:
        """Fetches entity by ID and caches FQN mapping."""
        entity_data = client.get_entity_by_id(entity_type, entity_id)

        fqn = entity_data.fully_qualified_name
        if not fqn:
            raise AirflowException(
                f'Fetched entity by ID {entity_id} has no "fullyQualifiedName". '
                f'Data: {entity_data.model_dump_json()}'
            )

        self._cache.cache_fqn_mapping(entity_id, fqn)
        self._cache.cache_entity(entity_type, fqn, entity_data)

        return entity_data


class LineageMetadataService:
    """Main service for entity metadata operations with caching and validation."""

    def __init__(self, entity_path_resolver: IEntityPathResolver = None):
        self._cache = EntityCache()
        self._validator = EntityValidator(entity_path_resolver or EntityRegistryPathResolver())
        self._fetcher = EntityFetcher(self._cache)

    def validate_entities(self, entities: List[TypedFQN], label: str) -> None:
        """Validates entity structure and types."""
        self._validator.validate_entities(entities, label)

    def preload_entities(
            self,
            client: LineageAPIClient,
            entities: List[TypedFQN],
    ) -> Dict[Tuple[str, str], OMDResponseEntity]:
        """Preloads entities with caching support."""
        cache: Dict[Tuple[str, str], OMDResponseEntity] = {}
        entities_to_fetch = []

        for entity in entities:
            cached_entity = self._cache.get_entity(entity.type, entity.fqn)
            if cached_entity:
                key = (entity.type.value, entity.fqn)
                cache[key] = cached_entity
            else:
                entities_to_fetch.append(entity)

        for entity in entities_to_fetch:
            key = (entity.type.value, entity.fqn)
            try:
                result = self._fetcher.fetch_entity(client, entity)
                cache[key] = result
            except AirflowException:
                raise

        return cache

    def get_fqn_from_entity_id(
            self,
            entity_id: str,
            client: LineageAPIClient,
            entity_type: EntityType,
    ) -> str:
        """Resolves FQN from entity ID with caching."""
        cached_fqn = self._cache.get_fqn_by_id(entity_id)
        if cached_fqn:
            return cached_fqn

        try:
            entity_data = self._fetcher.fetch_entity_by_id(client, entity_type, entity_id)
            return entity_data.fully_qualified_name
        except AirflowException:
            raise