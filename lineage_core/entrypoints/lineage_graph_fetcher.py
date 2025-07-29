from dataclasses import dataclass
from typing import (
    List,
    Optional,
    Set,
)

from omd_airflow_utils.lineage_core.adapters.db.psql_client import PostgresClient
from omd_airflow_utils.lineage_core.adapters.db.connection_providers import (
    AirflowConnectionParamsProvider,
    DBConnectionParamsProvider,
    SettingsConnectionParamsProvider,
)
from omd_airflow_utils.lineage_core.adapters.node_repository import (
    NodeRepository,
)
from omd_airflow_utils.lineage_core.domain.models import (
    LineageEdge,
    Node,
    Settings,
)
from omd_airflow_utils.lineage_core.domain.types import LineageLoadType


@dataclass
class LineageGraphResult:
    nodes: List[Node]
    edges: List[LineageEdge]
    affected_fqns: Optional[Set[str]] = None

class LineageGraphFetcher:
    """Fetches nodes and edges from the MGraph storage based on lineage settings."""

    def __init__(
        self,
        settings: Settings,
        database_conn_id: Optional[str] = None,
        schema_filter: Optional[list[str]] = None,
    ):
        self.settings = settings
        self.database_conn_id = database_conn_id
        self.schema_filter = schema_filter

    def fetch(self) -> LineageGraphResult:
        if self.settings.load_type == LineageLoadType.INIT:
            return self._fetch_init()
        else:
            return self._fetch_incremental()

    def _fetch_init(self) -> LineageGraphResult:
        schemas = self.schema_filter or self.settings.schema_filter
        provider = self._get_provider()

        with PostgresClient(params_provider=provider).get_connection() as conn:
            repo = NodeRepository(conn)
            nodes = repo.fetch_nodes(
                tag_id=self.settings.tag_id,
                schemas=schemas,
                state=self.settings.state,
                last_executed=self.settings.last_executed,
                operator_id=self.settings.operator_id,
            )
            edges = repo.fetch_edges([n.id for n in nodes])
            nodes = self._add_missing_nodes(repo, nodes, edges)
            return LineageGraphResult(nodes=nodes, edges=edges)

    def _fetch_incremental(self) -> LineageGraphResult:
        schemas = self.schema_filter or self.settings.schema_filter
        provider = self._get_provider()

        with PostgresClient(params_provider=provider).get_connection() as conn:
            repo = NodeRepository(conn)
            active, inactive = repo.fetch_nodes_for_incremental(
                tag_id=self.settings.tag_id,
                schemas=schemas,
                operator_id=self.settings.operator_id,
                last_executed=self.settings.last_executed,
                safety_window_hours=48,
            )
            nodes = active
            affected_fqns = {
                self.settings.context.fqn(n.db_schema, n.name) for n in active + inactive
            }
            edges = repo.fetch_edges([n.id for n in nodes]) if nodes else []
            nodes = self._add_missing_nodes(repo, nodes, edges) if edges else nodes
            return LineageGraphResult(
                nodes=nodes,
                edges=edges,
                affected_fqns=affected_fqns,
            )

    def _add_missing_nodes(
        self, repo: NodeRepository, nodes: List[Node], edges: List[LineageEdge]
    ) -> List[Node]:
        existing = {n.id for n in nodes}
        required = set()

        for edge in edges:
            from_id = int(edge.from_entity.id)
            to_id = int(edge.to_entity.id)

            if from_id not in existing:
                required.add(from_id)
            if to_id not in existing:
                required.add(to_id)

        if not required:
            return nodes

        extras = repo.fetch_nodes_additional_for_edges(
            operator_id=self.settings.operator_id,
            node_ids=list(required),
            state=self.settings.state,
        )
        return nodes + extras

    def _get_provider(self) -> DBConnectionParamsProvider:
        if self.database_conn_id:
            return AirflowConnectionParamsProvider(self.database_conn_id)
        if self.settings:
            return SettingsConnectionParamsProvider(self.settings)
        raise ValueError('Either "database_conn_id" or "settings" must be provided')