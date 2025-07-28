from datetime import UTC, datetime

import pytest

from operators.omd_airflow_utils.lineage_core.domain.models import (
    EntityRef,
    EntityType,
    LineageEdge,
    Node,
)


def make_edge(from_id: int, to_id: int) -> LineageEdge:
    return LineageEdge(
        from_entity=EntityRef(id=str(from_id), type=EntityType.TABLE),
        to_entity=EntityRef(id=str(to_id), type=EntityType.TABLE),
    )


@pytest.fixture
def sample_nodes():
    now = datetime.now(UTC)
    return [
        Node(id=1, name='A', namespace_id=1, db_schema='schema1', updated=now),
        Node(id=2, name='B', namespace_id=1, db_schema='schema1', updated=now),
        Node(id=3, name='C', namespace_id=1, db_schema='schema1', updated=now),
    ]


@pytest.fixture
def cross_schema_nodes():
    now = datetime.now(UTC)
    return [
        Node(id=1, name='table1', namespace_id=1, db_schema='sp_raw', updated=now),
        Node(id=2, name='table2', namespace_id=1, db_schema='sp_stage', updated=now),
        Node(id=3, name='table3', namespace_id=1, db_schema='sp_marts', updated=now),
    ]


@pytest.fixture
def cross_schema_nodes_with_branching():
    now = datetime.now(UTC)
    return [
        Node(id=1, name='table1', db_schema='sp_raw', namespace_id=1, updated=now),
        Node(id=2, name='table2', db_schema='sp_stage', namespace_id=1, updated=now),
        Node(id=4, name='table4', db_schema='sp_stage', namespace_id=1, updated=now),
        Node(id=3, name='table3', db_schema='sp_marts', namespace_id=1, updated=now),
    ]


@pytest.fixture
def fan_in_nodes():
    now = datetime.now(UTC)
    return [
        Node(id=1, name='table1', db_schema='sp_raw', namespace_id=1, updated=now),
        Node(id=2, name='table2', db_schema='sp_raw', namespace_id=1, updated=now),
        Node(id=3, name='table3', db_schema='sp_stage', namespace_id=1, updated=now),
    ]


@pytest.fixture
def cyclic_nodes():
    now = datetime.now(UTC)
    return [
        Node(id=1, name='table1', db_schema='sp_raw', namespace_id=1, updated=now),
        Node(id=2, name='table2', db_schema='sp_stage', namespace_id=1, updated=now),
    ]