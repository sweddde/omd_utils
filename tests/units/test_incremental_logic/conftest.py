from datetime import datetime

import pytest

from operators.omd_airflow_utils.lineage_core.adapters.node_repository import (
    NodeRepository,
)
from operators.omd_airflow_utils.lineage_core.domain.models import Node


@pytest.fixture
def now():
    return datetime(2025, 7, 1, 12, 0, 0)


@pytest.fixture
def node_factory(now):
    def _make_node(id: int, updated, state='accepted'):
        return Node(
            id=id,
            name=f'node_{id}',
            db_schema='schema',
            namespace_id=1,
            updated=updated,
            state=state,
        )
    return _make_node


@pytest.fixture
def mock_repo(monkeypatch):
    class MockRepo:
        def __init__(self):
            self.repo = NodeRepository(conn=None)
            self._nodes = []

            monkeypatch.setattr(self.repo, '_rows_to_nodes', lambda rows: self._nodes)
            monkeypatch.setattr(self.repo, 'conn', type('MockConn', (), {
                'cursor': lambda *a, **kw: type('Cursor', (), {
                    '__enter__': lambda self: self,
                    '__exit__': lambda self, *args: None,
                    'execute': lambda self, q, p: None,
                    'fetchall': lambda self: [],
                })()
            })())

        def override_nodes(self, nodes):
            self._nodes = nodes

        def call_incremental(self, now):
            return self.repo.fetch_nodes_for_incremental(
                tag_id=1,
                schemas=['schema'],
                operator_id=1,
                last_executed=now,
                safety_window_hours=48,
            )

    return MockRepo()