import logging
from typing import Optional

from airflow.exceptions import AirflowException

from omd_airflow_utils.lineage_core.adapters.config.config import LineageConfig
from omd_airflow_utils.lineage_core.adapters.db.connection_providers import (
    AirflowConnectionParamsProvider,
)
from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)

logger = logging.getLogger(__name__)


class LineageAPIClientFactory:
    """Factory for creating LineageAPIClient instances."""

    @staticmethod
    def create_from_connection(
            conn_id: str,
            config: Optional[LineageConfig] = None
    ) -> LineageAPIClient:
        """Creates client from Airflow connection."""
        try:
            connection_provider = AirflowConnectionParamsProvider(conn_id)
            conn_params = connection_provider.get_params()

            base_url = conn_params.get('host')
            token = conn_params.get('password')

            if not base_url:
                raise AirflowException(f'Connection {conn_id} is missing host')

            if not token:
                raise AirflowException(f'Connection {conn_id} is missing token/password')

            return LineageAPIClient(
                base_url=base_url,
                api_token=token,
                config=config,
            )

        except ValueError as e:
            raise AirflowException(f'Invalid connection {conn_id}: {e}') from e
        except Exception as e:
            logger.error('Failed to create LineageAPIClient from connection %s: %s', conn_id, e)
            raise AirflowException(f'Failed to create API client: {e}') from e

    @staticmethod
    def create_from_params(
            base_url: str,
            api_token: str,
            config: Optional[LineageConfig] = None
    ) -> LineageAPIClient:
        """Creates client from direct parameters."""
        if not base_url:
            raise AirflowException('base_url cannot be empty')

        if not api_token:
            raise AirflowException('api_token cannot be empty')

        try:
            return LineageAPIClient(
                base_url=base_url,
                api_token=api_token,
                config=config,
            )
        except Exception as e:
            logger.error('Failed to create LineageAPIClient: %s', e)
            raise AirflowException(f'Failed to create API client: {e}') from e