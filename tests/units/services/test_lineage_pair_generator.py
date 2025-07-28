import pytest
from omd_airflow_utils.lineage_core.services.omd_use_cases.lineage_pair_generator import (
    LineagePairGenerationService,
)
from omd_airflow_utils.lineage_core.domain.types import (
    TypedFQN,
    EntityType,
    MappingType,
)
from omd_airflow_utils.lineage_core.domain.models import (
    Node,
    LineageEdge,
    EntityRef,
    DatabaseContext,
)
from datetime import (
    datetime,
    UTC,
)


class TestLineagePairGenerationService:

    @pytest.fixture
    def service(self):
        return LineagePairGenerationService()

    @pytest.fixture
    def sample_sources(self):
        return [
            TypedFQN(EntityType.TABLE, 'service.db.raw.users'),
            TypedFQN(EntityType.TABLE, 'service.db.raw.orders'),
        ]

    @pytest.fixture
    def sample_targets(self):
        return [
            TypedFQN(EntityType.TABLE, 'service.db.marts.user_summary'),
            TypedFQN(EntityType.TABLE, 'service.db.marts.order_summary'),
        ]

    def test_generate_pairs_one_to_one(self, service, sample_sources, sample_targets):
        pairs = service.generate_pairs(MappingType.ONE_TO_ONE, sample_sources, sample_targets)

        assert len(pairs) == 2
        assert pairs[0].source.fqn == 'service.db.raw.users'
        assert pairs[0].target.fqn == 'service.db.marts.user_summary'
        assert pairs[1].source.fqn == 'service.db.raw.orders'
        assert pairs[1].target.fqn == 'service.db.marts.order_summary'

    def test_generate_pairs_one_to_many(self, service, sample_targets):
        sources = [TypedFQN(EntityType.TABLE, 'service.db.raw.events')]

        pairs = service.generate_pairs(MappingType.ONE_TO_MANY, sources, sample_targets)

        assert len(pairs) == 2
        assert all(pair.source.fqn == 'service.db.raw.events' for pair in pairs)
        target_fqns = {pair.target.fqn for pair in pairs}
        expected_targets = {'service.db.marts.user_summary', 'service.db.marts.order_summary'}
        assert target_fqns == expected_targets

    def test_generate_pairs_many_to_one(self, service, sample_sources):
        targets = [TypedFQN(EntityType.TABLE, 'service.db.marts.combined_report')]

        pairs = service.generate_pairs(MappingType.MANY_TO_ONE, sample_sources, targets)

        assert len(pairs) == 2
        assert all(pair.target.fqn == 'service.db.marts.combined_report' for pair in pairs)
        source_fqns = {pair.source.fqn for pair in pairs}
        expected_sources = {'service.db.raw.users', 'service.db.raw.orders'}
        assert source_fqns == expected_sources

    def test_generate_pairs_many_to_many(self, service, sample_sources, sample_targets):
        pairs = service.generate_pairs(MappingType.MANY_TO_MANY, sample_sources, sample_targets)

        assert len(pairs) == 4

        pair_tuples = {(pair.source.fqn, pair.target.fqn) for pair in pairs}
        expected_combinations = {
            ('service.db.raw.users', 'service.db.marts.user_summary'),
            ('service.db.raw.users', 'service.db.marts.order_summary'),
            ('service.db.raw.orders', 'service.db.marts.user_summary'),
            ('service.db.raw.orders', 'service.db.marts.order_summary'),
        }
        assert pair_tuples == expected_combinations

    def test_generate_pairs_one_to_one_unequal_length_raises_error(self, service):
        sources = [TypedFQN(EntityType.TABLE, 'service.db.raw.users')]
        targets = [
            TypedFQN(EntityType.TABLE, 'service.db.marts.user_summary'),
            TypedFQN(EntityType.TABLE, 'service.db.marts.order_summary'),
        ]

        with pytest.raises(Exception):
            service.generate_pairs(MappingType.ONE_TO_ONE, sources, targets)

    def test_generate_pairs_empty_sources_returns_empty(self, service, sample_targets):
        pairs = service.generate_pairs(MappingType.ONE_TO_MANY, [], sample_targets)
        assert pairs == []

    def test_generate_pairs_empty_targets_returns_empty(self, service, sample_sources):
        pairs = service.generate_pairs(MappingType.MANY_TO_ONE, sample_sources, [])
        assert pairs == []

    def test_extract_pairs_from_graph_direct_edges_only(self, service):
        now = datetime.now(UTC)
        nodes = [
            Node(id=1, name='table1', db_schema='raw', namespace_id=1, updated=now),
            Node(id=2, name='table2', db_schema='stage', namespace_id=1, updated=now),
            Node(id=3, name='table3', db_schema='marts', namespace_id=1, updated=now),
        ]

        edges = [
            LineageEdge(
                from_entity=EntityRef(id='1', type=EntityType.TABLE),
                to_entity=EntityRef(id='2', type=EntityType.TABLE)
            ),
            LineageEdge(
                from_entity=EntityRef(id='2', type=EntityType.TABLE),
                to_entity=EntityRef(id='3', type=EntityType.TABLE)
            ),
        ]

        db_context = DatabaseContext(service_name='test_service', database_name='test_db')

        pairs = service.extract_pairs_from_graph_paths(
            nodes=nodes,
            edges=edges,
            db_context=db_context,
            validate_existence=False,
        )

        assert len(pairs) == 2

        pair_tuples = {(pair.source.fqn, pair.target.fqn) for pair in pairs}
        expected_pairs = {
            ('test_service.test_db.raw.table1', 'test_service.test_db.stage.table2'),
            ('test_service.test_db.stage.table2', 'test_service.test_db.marts.table3'),
        }
        assert pair_tuples == expected_pairs

    def test_extract_pairs_from_graph_fan_in_pattern(self, service):
        now = datetime.now(UTC)
        nodes = [
            Node(id=1, name='users', db_schema='raw', namespace_id=1, updated=now),
            Node(id=2, name='orders', db_schema='raw', namespace_id=1, updated=now),
            Node(id=3, name='user_orders', db_schema='marts', namespace_id=1, updated=now),
        ]

        edges = [
            LineageEdge(
                from_entity=EntityRef(id='1', type=EntityType.TABLE),
                to_entity=EntityRef(id='3', type=EntityType.TABLE)
            ),
            LineageEdge(
                from_entity=EntityRef(id='2', type=EntityType.TABLE),
                to_entity=EntityRef(id='3', type=EntityType.TABLE)
            ),
        ]

        db_context = DatabaseContext()

        pairs = service.extract_pairs_from_graph_paths(
            nodes=nodes,
            edges=edges,
            db_context=db_context,
            validate_existence=False
        )

        assert len(pairs) == 2
        pair_tuples = {(pair.source.fqn, pair.target.fqn) for pair in pairs}
        expected_pairs = {
            ('Sacristy.sacristy.raw.users', 'Sacristy.sacristy.marts.user_orders'),
            ('Sacristy.sacristy.raw.orders', 'Sacristy.sacristy.marts.user_orders'),
        }
        assert pair_tuples == expected_pairs