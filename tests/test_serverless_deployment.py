"""Deployment invariants that protect credentials and retained data."""
from pathlib import Path
from perfsyncgnn.launcher.config import ROOT

def test_private_config_rejects_world_readable_credentials(tmp_path,monkeypatch):
    import pytest
    import perfsyncgnn.serverless.config as config
    path=tmp_path/'connection.json';path.write_text('{}');path.chmod(0o644)
    monkeypatch.setattr(config,'CONFIG',path)
    with pytest.raises(RuntimeError,match='0600'):config.environment({})

def test_dashboard_bundle_does_not_embed_server_credentials():
    import os
    dist=ROOT/'dashboard/frontend/dist'
    if not dist.exists():return
    for path in dist.rglob('*'):
        if path.is_file():
            body=path.read_text()
            for key in ('DITTO_PASSWORD','OPENWHISK_AUTH','__perfsync_config'):
                assert key not in body
