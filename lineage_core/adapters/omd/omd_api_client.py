import json
from http import HTTPStatus
from typing import (
    Optional, 
    Set, 
    Tuple, 
    Any,
)

import backoff
from airflow.exceptions import AirflowException

from omd_airflow_utils.lineage_core.adapters.config.config import LineageConfig
from omd_airflow_utils.lineage_core.adapters.httpx_client import HttpxClient
from omd_airflow_utils.lineage_core.adapters.omd.http.lineage_url_builder import (
    LineageUrlBuilder,
)
from omd_airflow_utils.lineage_core.adapters.omd.resolvers.entity_resolver import (
    EntityResolver,
)
from omd_airflow_utils.lineage_core.domain.models import (
    EntityRef,
    LineageEdge,
    LineagePayload,
)
from omd_airflow_utils.lineage_core.domain.registry import (
    EntityRegistryPathResolver,
)
from omd_airflow_utils.lineage_core.domain.types import EntityType
from omd_airflow_utils.lineage_core.utils.decorators import rate_limit
from omd_airflow_utils.lineage_core.adapters.omd.omd_response_models import (
    OMDResponseEntity,
)

import logging

logger = logging.getLogger(__name__)


class LineageAPIClient:
    """Client for OpenMetadata API lineage operations with simplified responsibilities."""

    def __init__(
        self,
        base_url: str,
        api_token: str,
        config: Optional[LineageConfig] = None,
    ) -> None:
        self.base_url = base_url.rstrip('/')
        self.config = config or LineageConfig()
        self.api_version = self.config.api.version

        self._httpx = HttpxClient(
            config=self.config.http_client,
            base_url=self.base_url,
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': self.config.api.headers.content_type,
            }
        )

        self._url_builder = LineageUrlBuilder(
            self.api_version,
            path_resolver=EntityRegistryPathResolver(),
        )

        self._resolver = EntityResolver(
            http=self._httpx,
            url_builder=self._url_builder
        )

    @rate_limit(calls_per_second=10)
    def get_entity(self, entity_type: EntityType, fqn: str) -> OMDResponseEntity:
        url = self._url_builder.by_fqn(entity_type, fqn, fields=['columns'])

        response = self._httpx.get(url)

        if response.status_code == HTTPStatus.NOT_FOUND:
            raise AirflowException(f'{entity_type.value.capitalize()} with FQN {fqn} not found')

        response.raise_for_status()

        return OMDResponseEntity(**response.json())

    @rate_limit(calls_per_second=10)
    def get_entity_by_id(self, entity_type: EntityType, entity_id: str) -> OMDResponseEntity:
        url = self._url_builder.by_id(entity_type, entity_id)
        response = self._httpx.get(url)
        if response.status_code == HTTPStatus.NOT_FOUND:
            raise AirflowException(f'{entity_type.value.capitalize()} with ID {entity_id} not found')
        response.raise_for_status()
        return OMDResponseEntity(**response.json())

    def get_edges_for_scope(
            self,
            fqns: Set[str],
            schema_filter: Optional[Set[str]] = None
    ) -> Set[Tuple[str, str]]:
        """Fetches lineage edges and optionally filters by schema."""
        edges: Set[Tuple[str, str]] = set()
        for fqn in fqns:
            try:
                downstream = self._fetch_lineage_edges(fqn, upstream_depth=0, downstream_depth=1)
                upstream = self._fetch_lineage_edges(fqn, upstream_depth=1, downstream_depth=0)
                for _, from_id, to_id in downstream + upstream:
                    try:
                        from_fqn = self._resolver.resolve_fqn_by_id(from_id)
                        to_fqn = self._resolver.resolve_fqn_by_id(to_id)

                        if schema_filter:
                            from_schema = from_fqn.split('.')[-2]
                            to_schema = to_fqn.split('.')[-2]
                            if from_schema not in schema_filter or to_schema not in schema_filter:
                                continue

                        edges.add((from_fqn, to_fqn))

                    except Exception as e:
                        logger.warning('Failed to resolve edge %s -> %s: %s', from_id, to_id, e)
            except Exception as e:
                logger.warning('Failed to fetch lineage for %s: %s', fqn, e)
        return edges

    def _fetch_lineage_edges(
        self,
        fqn: str,
        upstream_depth: int,
        downstream_depth: int
    ) -> list[tuple[str, str, str]]:
        url = self._url_builder.lineage_table_by_fqn(fqn, upstream_depth, downstream_depth)
        try:
            response = self._httpx.get(url)
            if response.status_code == HTTPStatus.NOT_FOUND:
                return []
            response.raise_for_status()
            return self._parse_edges(response.json(), fqn, direction='up' if upstream_depth else 'down')
        except Exception as e:
            logger.warning('Failed to fetch lineage edges for %s: %s', fqn, e)
            return []

    def _parse_edges(
        self,
        data: dict[str, Any],
        fqn: str,
        direction: str
    ) -> list[tuple[str, str, str]]:
        key = 'upstreamEdges' if direction == 'up' else 'downstreamEdges'
        edges = []
        edge_data = data.get(key, [])
        if not isinstance(edge_data, list):
            logger.warning('Lineage response for %s: %s not a list', fqn, key)
            return []
        for edge in edge_data:
            if isinstance(edge, dict):
                from_id = edge.get('fromEntity')
                to_id = edge.get('toEntity')
                if isinstance(from_id, str) and isinstance(to_id, str):
                    edges.append((fqn, from_id, to_id))
        return edges

    @backoff.on_exception(
        backoff.expo,
        AirflowException,
        max_tries=3,
        jitter=backoff.full_jitter,
    )
    @rate_limit(calls_per_second=10)
    def add_lineage(
        self,
        from_entity: EntityRef,
        to_entity: EntityRef,
        from_fqn: str,
        to_fqn: str,
    ) -> None:
        payload = LineagePayload(
            edge=LineageEdge(from_entity=from_entity, to_entity=to_entity)
        ).model_dump(by_alias=True)

        url = self._url_builder.lineage_add_path()
        response = self._httpx.put(
            url=url,
            json=payload
        )
        response.raise_for_status()

    def delete_lineage(self, from_entity: EntityRef, to_entity: EntityRef) -> None:
        payload = LineagePayload(
            edge=LineageEdge(from_entity=from_entity, to_entity=to_entity)
        ).model_dump(by_alias=True)

        url = self._url_builder.lineage_add_path()
        response = self._httpx.delete(
            url=url,
            json=payload,
        )
        response.raise_for_status()

    def delete_lineage_by_fqn(self, from_fqn: str, to_fqn: str) -> None:
        url = self._url_builder.lineage_delete_path(from_fqn, to_fqn)
        response = self._httpx.delete(url)
        if response.status_code not in {HTTPStatus.OK, HTTPStatus.NO_CONTENT}:
            logger.error('Failed to delete lineage by FQN: %s -> %s. Status: %s, Body: %s',
                         from_fqn, to_fqn, response.status_code, response.text)
            response.raise_for_status()

    @rate_limit(calls_per_second=10)
    def patch_table_description(self, fqn: str, description: str) -> None:
        """Updates the description of a table entity in OMD."""
        url = self._url_builder.by_fqn(EntityType.TABLE, fqn)

        payload_list = [
            {'op': 'add', 'path': '/description', 'value': description}
        ]

        patch_headers = {'Content-Type': 'application/json-patch+json'}

        response = self._httpx.patch(
            url=url,
            content=json.dumps(payload_list),
            headers=patch_headers
        )

        if response.status_code == HTTPStatus.NOT_FOUND:
            logger.warning('Table %s not found in OMD. Cannot update description.', fqn)
            return

        response.raise_for_status()
        logger.info('Successfully updated description for table %s', fqn)

    @rate_limit(calls_per_second=10)
    def patch_column_description_by_index(self, table_fqn: str, column_index: int, description: str) -> None:
        """Updates the description of a specific column using its array index."""
        url = self._url_builder.by_fqn(EntityType.TABLE, table_fqn)

        payload_list = [
            {
                'op': 'add',
                'path': f'/columns/{column_index}/description',
                'value': description
            }
        ]

        patch_headers = {'Content-Type': 'application/json-patch+json'}

        response = self._httpx.patch(
            url=url,
            content=json.dumps(payload_list),
            headers=patch_headers
        )

        if response.status_code != 200:
            logger.error(
                'Failed to patch column at index %d for table %s. Status: %s. Response: %s',
                column_index, table_fqn, response.status_code, response.text
            )

        response.raise_for_status()
        logger.info('Successfully updated description for column at index %d in table %s', column_index, table_fqn)

    def close(self) -> None:
        self._httpx.close()

    def __enter__(self) -> 'LineageAPIClient':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
