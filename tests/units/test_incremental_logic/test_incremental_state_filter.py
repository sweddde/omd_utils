from datetime import timedelta



def test_accepted_node_goes_to_active(mock_repo, now, node_factory):
    node = node_factory(id=1, updated=now - timedelta(hours=1), state='accepted')
    mock_repo.override_nodes([node])
    active, inactive = mock_repo.call_incremental(now)
    assert [n.id for n in active] == [1]
    assert inactive == []


def test_archived_node_goes_to_inactive(mock_repo, now, node_factory):
    node = node_factory(id=2, updated=now - timedelta(hours=1), state='archived')
    mock_repo.override_nodes([node])
    active, inactive = mock_repo.call_incremental(now)
    assert active == []
    assert [n.id for n in inactive] == [2]


def test_other_state_node_goes_to_inactive(mock_repo, now, node_factory):
    node = node_factory(id=3, updated=now - timedelta(hours=1), state='verification')
    mock_repo.override_nodes([node])
    active, inactive = mock_repo.call_incremental(now)
    assert active == []
    assert [n.id for n in inactive] == [3]