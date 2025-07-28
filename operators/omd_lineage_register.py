from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import (
    LineageAPIClient,
)
from omd_airflow_utils.lineage_core.domain.use_cases import (
    LineageProcessingResult,
)
from omd_airflow_utils.lineage_core.services.lineage_executor import (
    LineageOperationError,
    LineageOperationExecutor,
)
from omd_airflow_utils.operators.omd_base_lineage_operator import (
    BaseLineageOperator,
)


class RegisterLineageOperator(BaseLineageOperator):
    """Registers lineage links between source and target OMD entities."""

    def __init__(
        self,
        executor: LineageOperationExecutor = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.executor: LineageOperationExecutor = executor or LineageOperationExecutor()

    def _execute_sequential_operations(
        self,
        client: LineageAPIClient,
        processing_result: LineageProcessingResult
    ) -> None:
        result = self.executor.execute_add_operations_sequentially(
            processing_result.pairs,
            processing_result.entity_cache,
            client,
        )

        if result.successful_operations == 0 and result.total_operations > 0:
            raise LineageOperationError('All lineage registration operations failed')