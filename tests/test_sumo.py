import shutil
from collections import Counter
import pytest
from simulation.scenarios.intersection import Scenario, generate
from simulation.traci_adapter.client import TraCIClient
from dt.adapter import SQLiteTwinAdapter
from monitoring.store import Journal
from synchronization.operation_manager.manager import OperationManager


@pytest.mark.integration
def test_real_sumo_reproducible_and_transfer(tmp_path):
    if not shutil.which('sumo') or not shutil.which('netconvert'):
        pytest.skip('SUMO/netconvert executables unavailable')
    config = Scenario(duration_s=100, arrival_rate_per_s=0.12, observation_interval_s=2)
    path = generate(tmp_path/'scenario', config)
    first = list(TraCIClient(config,path).events())
    second = list(TraCIClient(config,path).events())
    assert [e.model_dump(exclude={'emitted_at_unix_ms'}) for e in first] == [e.model_dump(exclude={'emitted_at_unix_ms'}) for e in second]
    assert len({e.entity_id for e in first if e.entity_type == 'vehicle'}) > 1
    assert {'vehicle','traffic_signal','road_segment'} <= {e.entity_type for e in first}
    manager = OperationManager(SQLiteTwinAdapter(tmp_path/'twins.db'),Journal(tmp_path/'journal.db'))
    # Vehicle events are enough to validate crossing/arrival operation semantics.
    for e in first:
        if e.entity_type == 'vehicle':
            manager.submit(e)
    ops = manager.journal.operations()
    assert {'Attach','Update','Transfer','Detach'} <= {o.operation_type for o in ops}
    assert all(o.status == 'COMMITTED' for o in ops)

    replay = OperationManager(SQLiteTwinAdapter(tmp_path/'replay-twins.db'), Journal(tmp_path/'replay-journal.db'))
    for e in second:
        if e.entity_type == 'vehicle':
            replay.submit(e)
    def signature(operation):
        return (operation.operation_id, operation.operation_type, operation.dt_id,
                operation.dependencies, operation.status, operation.version_before,
                operation.version_after, operation.owner)
    assert [signature(o) for o in ops] == [signature(o) for o in replay.journal.operations()]
