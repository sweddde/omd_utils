import logging
from http import HTTPStatus

from omd_airflow_utils.lineage_core.adapters.httpx_client import HttpxClient
from omd_airflow_utils.lineage_core.adapters.omd.http.lineage_url_builder import (
    LineageUrlBuilder,
)
from omd_airflow_utils.lineage_core.adapters.omd.omd_response_models import (
    OMDResponseEntity,
)
from omd_airflow_utils.lineage_core.domain.types import EntityType

logger = logging.getLogger(__name__)


class EntityResolver:
    def __init__(
        self,
        http: HttpxClient,
        url_builder: LineageUrlBuilder
    ):
        self.http = http
        self.urls = url_builder

    def resolve_fqn_by_id(self, entity_id: str) -> str:
        """Resolve FQN from ID."""
        url = self.urls.by_id(EntityType.TABLE, entity_id)
        try:
            response = self.http.get(url)
        except Exception as e:
            logger.error('Request failed while resolving entity %s: %s', entity_id, e)
            raise

        if response.status_code == HTTPStatus.NOT_FOUND:
            raise ValueError(f'Entity not found: {entity_id}')
        if response.status_code != HTTPStatus.OK:
            logger.error('Failed to resolve entity %s. Status: %s - %s', entity_id, response.status_code, response.text)
            response.raise_for_status()

        entity = OMDResponseEntity(**response.json())
        if not entity.fully_qualified_name:
            raise ValueError(f'Entity {entity_id} resolved with missing FQN')
        return entity.fully_qualified_name