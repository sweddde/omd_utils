from datetime import (
    datetime,
    timezone,
)
from typing import (
    Any,
    Optional,
)

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from omd_airflow_utils.lineage_core.adapters.config.config import LineageConfig
from omd_airflow_utils.lineage_core.adapters.config.config_manager import (
    ConfigManager,
)
from omd_airflow_utils.lineage_core.adapters.db.psql_client import PostgresClient
from omd_airflow_utils.lineage_core.adapters.node_repository import NodeRepository
from omd_airflow_utils.lineage_core.adapters.omd.omd_client_factory import (
    LineageAPIClientFactory,
)
from omd_airflow_utils.lineage_core.domain.models import Settings
from omd_airflow_utils.lineage_core.domain.types import (
    TypedFQN,
    LineageLoadType,
)
from omd_airflow_utils.lineage_core.entrypoints.lineage_graph_fetcher import (
    LineageGraphFetcher,
)
from omd_airflow_utils.lineage_core.entrypoints.lineage_sync_runner import (
    LineageSyncRunner,
)
from omd_airflow_utils.lineage_core.services.lineage_executor import (
    LineageOperationExecutor,
)
from omd_airflow_utils.lineage_core.services.lineage_service import (
    LineageService,
)
from omd_airflow_utils.lineage_core.services.omd_use_cases.description_sync import (
    DescriptionSyncService,
)


class MGraphToOMDLineageOperator(BaseOperator):
    """Operator for syncing lineage from MGraph (Postgres) to OMD API."""

    def __init__(
        self,
        metadata_conn_id: str,
        database_conn_id: str,
        source_entities: Optional[list[TypedFQN]] = None,
        target_entities: Optional[list[TypedFQN]] = None,
        schema_filter: Optional[list[str]] = None,
        settings: Optional[Settings] = None,
        path_cutoff: int = 20,
        config_variable_name: Optional[str] = None,
        config: Optional[LineageConfig] = None,
        sync_descriptions: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.metadata_conn_id = metadata_conn_id
        self.database_conn_id = database_conn_id
        self.source_entities = source_entities or []
        self.target_entities = target_entities or []
        self.schema_filter = schema_filter
        self.settings = settings
        self.path_cutoff = path_cutoff
        self.config_variable_name = config_variable_name
        self.config = config or LineageConfig()
        self.service = LineageService.create_default()
        self.sync_descriptions = sync_descriptions


    def execute(self, context: dict[str, Any]) -> None:
        try:
            config_mgr = ConfigManager(variable_name=self.config_variable_name)
            self.settings = config_mgr.load_settings()
            self._apply_overrides()

            graph_fetcher = LineageGraphFetcher(self.settings, self.database_conn_id, self.schema_filter)
            result = graph_fetcher.fetch()
            nodes = result.nodes
            edges = result.edges
            affected_fqns = result.affected_fqns

            if not nodes and self.settings.load_type == LineageLoadType.INIT:
                return
            if not nodes and not affected_fqns:
                return

            with LineageAPIClientFactory.create_from_connection(self.metadata_conn_id, self.config) as client:
                pairs = self.service.extract_graph_lineage(
                    nodes=nodes,
                    edges=edges,
                    db_context=self.settings.context,
                    path_cutoff=self.path_cutoff,
                    collapse_triggers=True,
                    trigger_operator_id=self.settings.operator_id,
                    client=client,
                )

                runner = LineageSyncRunner(client, self.service, LineageOperationExecutor())
                runner.run_sync(
                    lineage_pairs=pairs,
                    load_type=self.settings.load_type,
                    clean_before_update=self.settings.clean_before_update,
                    affected_fqns=affected_fqns,
                    schema_filter=set(self.settings.schema_filter),
                )

                if self.sync_descriptions:
                    if not nodes:
                        self.log.info('No nodes found, skipping description sync.')
                    else:
                        self.log.info('Starting descriptions synchronization for %d nodes...', len(nodes))

                        provider = graph_fetcher._get_provider()
                        with PostgresClient(params_provider=provider).get_connection() as conn:
                            node_repo = NodeRepository(conn)

                            description_sync_service = DescriptionSyncService(
                                node_repo=node_repo,
                                omd_client=client,
                                db_context=self.settings.context,
                            )
                            description_sync_service.sync_descriptions_for_nodes(nodes)

                        self.log.info('Descriptions synchronization completed.')

            self._update_config(config_mgr)

        except Exception as e:
            raise AirflowException(f'MGraph lineage sync failed: {e}') from e

    def _apply_overrides(self) -> None:
        """Overrides settings with schema_filter and reset last_executed if init load."""
        if self.settings.load_type == LineageLoadType.INIT:
            self.settings.last_executed = None
        if self.schema_filter:
            self.settings.schema_filter = self.schema_filter

    def _update_config(self, config_mgr: ConfigManager) -> None:
        """Updates Airflow variable state after sync completes."""
        config_mgr.update_last_executed(datetime.now(timezone.utc))
        if self.settings.load_type == LineageLoadType.INIT:
            config_mgr.update_config_flag('load_type', LineageLoadType.INCREMENTAL.value)

        if self.settings.clean_before_update:
            config_mgr.update_config_flag('clean_before_update', False)