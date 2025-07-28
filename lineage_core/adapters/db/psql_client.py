import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection

from omd_airflow_utils.lineage_core.adapters.db.connection_providers import (
    DBConnectionParamsProvider,
)

logger = logging.getLogger(__name__)


class PostgresClient:
    """PostgreSQL database client with connection management."""

    def __init__(self, params_provider: DBConnectionParamsProvider):
        self.params_provider = params_provider

    @contextmanager
    def get_connection(self) -> Generator[connection, None, None]:
        """Provides PostgreSQL connection with automatic cleanup."""
        conn = None
        try:
            conn_params = self.params_provider.get_params()
            conn = psycopg2.connect(**conn_params)
            yield conn
        except Exception as e:
            logger.error('Database connection failed: %s', e)
            raise
        finally:
            if conn:
                conn.close()