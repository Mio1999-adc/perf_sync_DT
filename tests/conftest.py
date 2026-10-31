import pytest


@pytest.fixture(autouse=True)
def sqlite_process_restart_durability(monkeypatch):
    # NORMAL is process-crash safe, but not an OS/power-loss durability test.
    # Production defaults to FULL. This avoids repeated fsync on test storage.
    monkeypatch.setenv('PERFSYNC_SQLITE_SYNCHRONOUS', 'NORMAL')
