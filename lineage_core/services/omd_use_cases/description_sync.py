import logging
from typing import List, Optional

from omd_airflow_utils.lineage_core.adapters.node_repository import NodeRepository
from omd_airflow_utils.lineage_core.adapters.omd.omd_api_client import LineageAPIClient
from omd_airflow_utils.lineage_core.domain.models import Node, DatabaseContext
from omd_airflow_utils.lineage_core.domain.types import EntityType

logger = logging.getLogger(__name__)


class DescriptionSyncService:
    def __init__(self, node_repo: NodeRepository, omd_client: LineageAPIClient, db_context: DatabaseContext):
        self.node_repo = node_repo
        self.omd_client = omd_client
        self.db_context = db_context

    def _get_column_index(self, table_fqn: str, column_name: str) -> Optional[int]:
        """Fetches table details and finds the index of a column by its simple name."""
        try:
            table_entity = self.omd_client.get_entity(EntityType.TABLE, table_fqn)

            for i, col in enumerate(table_entity.columns):
                if col.name == column_name:
                    return i

            logger.warning('Column "%s" not found in table "%s" in OMD.', column_name, table_fqn)
            return None
        except Exception as e:
            logger.error('Failed to get entity or find column index for %s.%s: %s', table_fqn, column_name, e)
            return None

    def sync_descriptions_for_nodes(self, nodes: List[Node]):
        """
        Iterates through nodes, fetches their descriptions from the source DB,
        and updates them in OpenMetadata.
        """
        logger.info('Starting description sync for %d nodes.', len(nodes))

        for i, node in enumerate(nodes, 1):
            node_fqn = self.db_context.fqn(schema=node.db_schema, name=node.name)
            logger.info('Processing node %d/%d: %s (id: %s)', i, len(nodes), node_fqn, node.id)

            table_description = node.description
            if table_description:
                try:
                    self.omd_client.patch_table_description(node_fqn, table_description)
                except Exception as e:
                    logger.error('Failed to update table description for %s: %s', node_fqn, e)

            columns_descriptions = self.node_repo.fetch_columns_descriptions(node.id)
            if not columns_descriptions:
                continue

            logger.info('Found %d column descriptions to sync for %s.', len(columns_descriptions), node_fqn)
            for col_name, col_description in columns_descriptions.items():
                if not col_description:
                    continue

                col_index = self._get_column_index(node_fqn, col_name)

                if col_index is not None:
                    try:
                        self.omd_client.patch_column_description_by_index(
                            table_fqn=node_fqn,
                            column_index=col_index,
                            description=col_description
                        )
                    except Exception as e:
                        logger.error(
                            'Failed to update column "%s" (index %d) for table %s: %s',
                            col_name, col_index, node_fqn, e
                        )