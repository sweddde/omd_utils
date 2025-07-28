import logging
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Callable,
    Optional,
)

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from omd_airflow_utils.lineage_core.adapters.config.config import LineageConfig
from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)
from omd_airflow_utils.lineage_core.adapters.omd.omd_client_factory import (
    LineageAPIClientFactory,
)
from omd_airflow_utils.lineage_core.domain.models import Settings
from omd_airflow_utils.lineage_core.domain.types import (
    MappingType,
    TypedFQN,
)
from omd_airflow_utils.lineage_core.domain.use_cases import LineageRequest
from omd_airflow_utils.lineage_core.services.lineage_executor import (
    LineageOperationError,
)
from omd_airflow_utils.lineage_core.services.lineage_service import (
    LineageService,
)
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_diff_calculator import (
    LineageSyncService,
)
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_metadata_cache import (
    LineageMetadataService,
)
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_pair_generator import (
    LineagePairGenerationService,
)

logger = logging.getLogger(__name__)


class BaseLineageOperator(BaseOperator, ABC):
    """Abstract base operator for lineage tasks with common OMD client and execution logic."""

    def __init__(
        self,
        metadata_conn_id: str,
        source_entities: list[TypedFQN],
        target_entities: list[TypedFQN],
        config: LineageConfig = None,
        mapping: Optional[MappingType] = None,
        client_factory: Callable[..., LineageAPIClient] = LineageAPIClientFactory.create_from_connection,
        service: Optional[LineageService] = None,
        settings: Optional[Settings] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.metadata_conn_id = metadata_conn_id
        self.source_entities = source_entities
        self.target_entities = target_entities
        self.config = config or LineageConfig()
        self.verify_ssl = self.config.operator.defaults.verify_ssl
        self.mapping = mapping or self.config.operator.defaults.mapping
        self.client_factory = client_factory
        self.service = service or self._create_default_service()
        self.settings = settings or Settings()

    def _create_default_service(self) -> LineageService:
        return LineageService(
            metadata_service=LineageMetadataService(),
            pair_generation_service=LineagePairGenerationService(),
            sync_service=LineageSyncService(),
        )

    def _create_client(self) -> LineageAPIClient:
        return LineageAPIClientFactory.create_from_connection(
            conn_id=self.metadata_conn_id,
            config=self.config
        )

    def execute(self, context: dict[str, Any]) -> None:
        try:
            with self._create_client() as client:
                request = LineageRequest(
                    source_entities=self.source_entities,
                    target_entities=self.target_entities,
                    mapping=self.mapping,
                )
                processing_result = self.service.prepare_lineage_processing(request, client=client)
                if not processing_result.pairs:
                    return
                self._execute_sequential_operations(client, processing_result)
        except LineageOperationError as e:
            raise AirflowException(f'Lineage operation failed: {e}') from e
        except Exception as e:
            raise AirflowException(f'Lineage operation failed: {e}') from e

    @abstractmethod
    def _execute_sequential_operations(self, client: LineageAPIClient, processing_result) -> None:
        raise NotImplementedError
