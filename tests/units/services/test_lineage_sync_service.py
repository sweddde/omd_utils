import pytest
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_diff_calculator import LineageSyncService
from omd_airflow_utils.lineage_core.domain.types import EntityPair, TypedFQN, EntityType


class TestLineageSyncService:

    @pytest.fixture
    def service(self):
        return LineageSyncService()

    @pytest.fixture
    def sample_pairs(self):
        return [
            EntityPair(
                source=TypedFQN(EntityType.TABLE, 'raw.users'),
                target=TypedFQN(EntityType.TABLE, 'marts.user_summary')
            ),
            EntityPair(
                source=TypedFQN(EntityType.TABLE, 'raw.orders'),
                target=TypedFQN(EntityType.TABLE, 'marts.order_summary')
            ),
        ]

    def test_diff_lineage_pairs_additions_only(self, service, sample_pairs):
        existing_edges = set()

        pairs_to_add, pairs_to_delete = service.diff_lineage_pairs(
            sample_pairs, existing_edges
        )

        assert len(pairs_to_add) == 2
        assert len(pairs_to_delete) == 0

        added_fqns = {(pair.source.fqn, pair.target.fqn) for pair in pairs_to_add}
        expected_fqns = {('raw.users', 'marts.user_summary'), ('raw.orders', 'marts.order_summary')}
        assert added_fqns == expected_fqns

    def test_diff_lineage_pairs_deletions_only(self, service):
        new_pairs = []
        existing_edges = {
            ('old.table1', 'old.target1'),
            ('old.table2', 'old.target2'),
        }

        pairs_to_add, pairs_to_delete = service.diff_lineage_pairs(
            new_pairs, existing_edges
        )

        assert len(pairs_to_add) == 0
        assert pairs_to_delete == existing_edges

    def test_diff_lineage_pairs_mixed_changes(self, service, sample_pairs):
        existing_edges = {
            ('raw.users', 'marts.user_summary'),
            ('old.deprecated', 'old.target'),
        }

        pairs_to_add, pairs_to_delete = service.diff_lineage_pairs(
            sample_pairs, existing_edges
        )

        assert len(pairs_to_add) == 1
        assert pairs_to_add[0].source.fqn == 'raw.orders'
        assert pairs_to_add[0].target.fqn == 'marts.order_summary'

        assert pairs_to_delete == {('old.deprecated', 'old.target')}

    def test_diff_lineage_pairs_no_changes(self, service, sample_pairs):
        existing_edges = {
            ('raw.users', 'marts.user_summary'),
            ('raw.orders', 'marts.order_summary'),
        }

        pairs_to_add, pairs_to_delete = service.diff_lineage_pairs(
            sample_pairs, existing_edges
        )

        assert len(pairs_to_add) == 0
        assert len(pairs_to_delete) == 0

    def test_diff_lineage_pairs_with_affected_scope(self, service, sample_pairs):
        existing_edges = {
            ('raw.users', 'marts.user_summary'),
            ('raw.orders', 'old.target'),
            ('external.table', 'external.target'),
        }

        affected_scope = {'raw.users', 'raw.orders', 'marts.user_summary'}

        pairs_to_add, pairs_to_delete = service.diff_lineage_pairs(
            sample_pairs, existing_edges, affected_scope
        )

        assert len(pairs_to_add) == 1
        assert pairs_to_add[0].source.fqn == 'raw.orders'
        assert pairs_to_add[0].target.fqn == 'marts.order_summary'

        assert pairs_to_delete == {('raw.orders', 'old.target')}
