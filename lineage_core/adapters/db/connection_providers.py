from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Dict,
    Union,
)

from omd_airflow_utils.lineage_core.connections.utils import (
    Connections as Conn,
)
from omd_airflow_utils.lineage_core.domain.models import Settings


class DBConnectionParamsProvider(ABC):
    """Abstract base class for database connection parameters providers."""

    @abstractmethod
    def get_params(self) -> Dict[str, Union[str, int]]:
        raise NotImplementedError


class AirflowConnectionParamsProvider(DBConnectionParamsProvider):
    """Provides database connection parameters from Airflow connections."""

    def __init__(self, conn_id: str):
        if not conn_id:
            raise ValueError('Connection ID is required')
        self.conn_id = conn_id

    def get_params(self) -> Dict[str, Union[str, int]]:
        return Conn.get_connection_params(self.conn_id)


class SettingsConnectionParamsProvider(DBConnectionParamsProvider):
    """Provides database connection parameters from Settings object."""

    def __init__(self, settings: Settings):
        if not settings:
            raise ValueError('Settings object is required')
        self.settings = settings