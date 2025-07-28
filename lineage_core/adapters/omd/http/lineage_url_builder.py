import urllib.parse
from typing import Optional

from omd_airflow_utils.lineage_core.domain.interfaces import (
    IEntityPathResolver,
)
from omd_airflow_utils.lineage_core.domain.types import EntityType


class LineageUrlBuilder:
    """Builds OpenMetadata API URLs for entity and lineage operations."""
    def __init__(
        self,
        api_version: str,
        path_resolver: IEntityPathResolver,
    ):
        self.api_version = api_version.strip('/')
        self.path_resolver = path_resolver

    def by_fqn(
        self,
        entity_type: EntityType,
        fqn: str,
        fields: Optional[list[str]] = None,
    ) -> str:
        """Build URL to fetch entity by FQN with optional fields."""
        fqn_encoded = urllib.parse.quote(fqn, safe='')
        entity_path = self.path_resolver.get_path(entity_type)

        base_url = f'/api/{self.api_version}/{entity_path}/name/{fqn_encoded}'

        if fields:
            fields_str = ','.join(fields)
            return f'{base_url}?fields={fields_str}'

        return base_url

    def by_id(self, entity_type: EntityType, entity_id: str) -> str:
        """Build URL to fetch entity by ID"""
        entity_path = self.path_resolver.get_path(entity_type)
        return '/api/{}/{}/{}'.format(self.api_version, entity_path, entity_id)

    def lineage_table_by_fqn(self, fqn: str, upstream_depth: int, downstream_depth: int) -> str:
        """Build URL to fetch lineage for table by FQN"""
        fqn_encoded = urllib.parse.quote(fqn, safe='')
        return (
            '/api/{}/lineage/table/name/{}?upstreamDepth={}&downstreamDepth={}'
            .format(self.api_version, fqn_encoded, upstream_depth, downstream_depth)
        )

    def lineage_add_path(self) -> str:
        """URL for add/delete lineage by ID"""
        return '/api/{}/lineage'.format(self.api_version)

    def lineage_delete_path(self, from_fqn: str, to_fqn: str) -> str:
        """URL for delete lineage by FQN"""
        return '/api/{}/lineage/table/name/{}/table/name/{}'.format(
            self.api_version,
            urllib.parse.quote(from_fqn, safe=''),
            urllib.parse.quote(to_fqn, safe='')
        )