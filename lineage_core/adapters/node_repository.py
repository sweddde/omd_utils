import logging
from datetime import (
    datetime,
    timedelta,
)
from typing import (
    Any,
    Optional,
    Sequence,
)

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection

from omd_airflow_utils.lineage_core.adapters.sql.fetch_description import (
    FETCH_COLUMNS_DESCRIPTIONS,
    FETCH_TABLE_DESCRIPTION,
)
from omd_airflow_utils.lineage_core.adapters.sql.fetch_edges import FETCH_EDGES
from omd_airflow_utils.lineage_core.adapters.sql.fetch_nodes import FETCH_NODES
from omd_airflow_utils.lineage_core.adapters.sql.fetch_nodes_additional import (
    FETCH_NODES_ADDITIONAL,
)
from omd_airflow_utils.lineage_core.adapters.sql.fetch_nodes_incremental import (
    FETCH_NODES_INCREMENTAL,
)
from omd_airflow_utils.lineage_core.domain.models import (
    EntityRef,
    EntityType,
    LineageEdge,
    Node,
)

logger = logging.getLogger(__name__)


class NodeRepository:
    """Repository for database operations on nodes and edges."""

    def __init__(self, conn: connection):
        self.conn = conn

    def _rows_to_nodes(self, rows: Sequence[Any]) -> list[Node]:
        """Converts database rows to Node objects with error handling."""
        nodes = []
        failed_count = 0

        for row in rows:
            try:
                row_dict = dict(row)
                nodes.append(Node(**row_dict))
            except Exception as e:
                failed_count += 1
                row_id = row.get('id', 'unknown') if hasattr(row, 'get') else 'unknown'
                logger.warning('Failed to create Node from row id=%s: %s', row_id, e)

        if failed_count > 0:
            logger.warning('Failed to parse %d out of %d node records', failed_count, len(rows))

        return nodes

    def _rows_to_edges(self, rows: Sequence[Any]) -> list[LineageEdge]:
        """Converts database rows to LineageEdge objects with error handling."""
        edges = []
        failed_count = 0

        for row in rows:
            try:
                row_dict = dict(row) if hasattr(row, 'keys') else row
                from_id = str(row_dict['from_node_id'])
                to_id = str(row_dict['to_node_id'])
                from_entity = EntityRef(id=from_id, type=EntityType.TABLE)
                to_entity = EntityRef(id=to_id, type=EntityType.TABLE)
                edges.append(LineageEdge(from_entity=from_entity, to_entity=to_entity))
            except Exception as e:
                failed_count += 1
                logger.warning('Failed to create LineageEdge from row: %s', e)

        if failed_count > 0:
            logger.warning('Failed to parse %d out of %d edge records', failed_count, len(rows))

        return edges

    def fetch_nodes(
        self,
        tag_id: int,
        schemas: list[str],
        state: str,
        operator_id: int,
        last_executed: Optional[datetime] = None,
        safety_window_hours: int = 48,
    ) -> list[Node]:
        """Fetches nodes based on filtering criteria with optional incremental support."""
        if last_executed:
            extra_filter = """
            and (n.updated > (%(last_executed)s - interval '%(safety_window_hours)s hours')
            or (n.updated is null and n.created > (%(last_executed)s - interval '%(safety_window_hours)s hours')))
            """
        else:
            extra_filter = ''

        query = FETCH_NODES.format(extra_filter=extra_filter)
        params = {
            'tag_id': tag_id,
            'schemas': schemas,
            'state': state,
            'operator_id': operator_id,
            'safety_window_hours': safety_window_hours,
        }

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        except Exception as e:
            logger.error('Error fetching nodes: %s', e)
            raise

        return self._rows_to_nodes(rows)

    def fetch_edges(self, node_ids: list[int]) -> list[LineageEdge]:
        """Fetches lineage edges for given node IDs."""
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(FETCH_EDGES, {'node_ids': node_ids})
                rows = cur.fetchall()
        except Exception as e:
            logger.error('Error fetching edges: %s', e)
            raise

        return self._rows_to_edges(rows)

    def fetch_nodes_additional_for_edges(
        self,
        operator_id: int,
        node_ids: list[int],
        state: str = 'accepted',
    ) -> list[Node]:
        """Fetches additional nodes needed for edge relationships."""
        if not node_ids:
            return []

        params = {
            'node_ids': node_ids,
            'state': state,
            'operator_id': operator_id,
        }

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(FETCH_NODES_ADDITIONAL, params)
                rows = cur.fetchall()
        except Exception as e:
            logger.error('Error fetching additional nodes: %s', e)
            raise

        return self._rows_to_nodes(rows)

    def fetch_nodes_for_incremental(
        self,
        tag_id: int,
        schemas: list[str],
        operator_id: int,
        last_executed: Optional[datetime],
        safety_window_hours: int = 48,
    ) -> tuple[list[Node], list[Node]]:
        """Fetches nodes for incremental processing, returning active and inactive separately."""
        params = {
            'tag_id': tag_id,
            'schemas': schemas,
            'operator_id': operator_id,
            'last_executed': last_executed,
            'safety_window_hours': safety_window_hours,
        }

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(FETCH_NODES_INCREMENTAL, params)
                rows = cur.fetchall()

            all_latest_nodes = self._rows_to_nodes(rows)
            safety_threshold = last_executed - timedelta(hours=safety_window_hours)
            changed_nodes = [
                node for node in all_latest_nodes
                if node.updated and node.updated > safety_threshold
            ]

            active_nodes = [n for n in changed_nodes if n.state == 'accepted']
            inactive_nodes = [n for n in changed_nodes if n.state != 'accepted']

            return active_nodes, inactive_nodes

        except Exception as e:
            logger.error('Error fetching incremental nodes: %s', e)
            raise

    def fetch_table_description(self, node_id: int) -> Optional[str]:
        """Fetches the description for a single table (node)."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(FETCH_TABLE_DESCRIPTION, {'node_id': node_id})
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error('Error fetching table description for node_id %s: %s', node_id, e)
            return None

    def fetch_columns_descriptions(self, node_id: int) -> dict[str, str]:
        """Fetches descriptions for all columns of a table, returned as a dict."""
        descriptions = {}
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(FETCH_COLUMNS_DESCRIPTIONS, {'node_id': node_id})
                rows = cur.fetchall()
                for row in rows:
                    descriptions[row['column_name']] = row['description']
        except Exception as e:
            logger.error('Error fetching column descriptions for node_id %s: %s', node_id, e)
        return descriptions