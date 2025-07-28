import logging
import time
from dataclasses import dataclass
from enum import Enum

from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)
from omd_airflow_utils.lineage_core.adapters.omd.omd_response_models import (
    OMDResponseEntity,
)
from omd_airflow_utils.lineage_core.domain.types import EntityPair
from omd_airflow_utils.lineage_core.utils.entity_utils import to_entity_ref

logger = logging.getLogger(__name__)


class OperationType(Enum):
    ADD = 'add'
    DELETE = 'delete'


@dataclass
class OperationResult:
    operation_type: OperationType
    pair: EntityPair
    success: bool
    error: Exception = None


@dataclass
class ExecutionResult:
    total_operations: int
    successful_operations: int
    failed_operations: int
    results: list[OperationResult]

    @property
    def success_rate(self) -> float:
        if self.total_operations == 0:
            return 0.0
        return (self.successful_operations / self.total_operations) * 100


class LineageOperationExecutor:
    """Executes add/delete lineage operations with delays and progress tracking."""

    def execute_add_operations_sequentially(
        self,
        pairs: list[EntityPair],
        entity_cache: dict,
        client: LineageAPIClient,
        delay_between_operations: float = 0.1,
        batch_pause_size: int = 30,
        batch_pause_duration: float = 1.0
    ) -> ExecutionResult:
        results = []

        for i, pair in enumerate(pairs, 1):
            try:
                logger.info('ADD %s/%s: source=%s -> target=%s', i, len(pairs), pair.source.fqn, pair.target.fqn)
                self._execute_add_operation(pair, entity_cache, client)
                results.append(OperationResult(OperationType.ADD, pair, True))
            except Exception as e:
                results.append(OperationResult(OperationType.ADD, pair, False, e))

            if i < len(pairs):
                time.sleep(batch_pause_duration if i % batch_pause_size == 0 else delay_between_operations)

        return self._create_execution_result(results)

    def execute_delete_operations_sequentially(
        self,
        pairs: list[EntityPair],
        client: LineageAPIClient,
        delay_between_operations: float = 0.15,
        batch_pause_size: int = 20,
        batch_pause_duration: float = 2.0
    ) -> ExecutionResult:
        results = []

        for i, pair in enumerate(pairs, 1):
            try:
                logger.info('DELETE %s/%s: source=%s -> target=%s', i, len(pairs), pair.source.fqn, pair.target.fqn)
                self._execute_delete_operation(pair, client)
                results.append(OperationResult(OperationType.DELETE, pair, True))
            except Exception as e:
                results.append(OperationResult(OperationType.DELETE, pair, False, e))

            if i < len(pairs):
                time.sleep(batch_pause_duration if i % batch_pause_size == 0 else delay_between_operations)

        return self._create_execution_result(results)

    def _execute_add_operation(
        self,
        pair: EntityPair,
        entity_cache: dict[tuple[str, str], OMDResponseEntity],
        client: LineageAPIClient
    ) -> None:
        from_ref = to_entity_ref(pair.source, entity_cache)
        to_ref = to_entity_ref(pair.target, entity_cache)
        client.add_lineage(from_ref, to_ref, pair.source.fqn, pair.target.fqn)

    def _execute_delete_operation(self, pair: EntityPair, client: LineageAPIClient) -> None:
        client.delete_lineage_by_fqn(pair.source.fqn, pair.target.fqn)

    def _create_execution_result(self, results: list[OperationResult]) -> ExecutionResult:
        successful = sum(1 for r in results if r.success)
        return ExecutionResult(
            total_operations=len(results),
            successful_operations=successful,
            failed_operations=len(results) - successful,
            results=results
        )


class LineageOperationError(Exception):
    pass