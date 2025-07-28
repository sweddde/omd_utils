from omd_airflow_utils.lineage_core.domain.models import (
    DatabaseContext,
    Node,
)
from omd_airflow_utils.lineage_core.domain.types import (
    EntityType,
    TypedFQN,
)


def to_typed_fqn(node: Node, context: DatabaseContext) -> TypedFQN:
    """Creates a TABLE-typed FQN from a graph node using the DB context."""
    return TypedFQN(
        type=EntityType.TABLE,
        fqn=context.fqn(schema=node.db_schema, name=node.name),
    )

