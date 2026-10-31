"""Ditto is authoritative. SQLiteTwinAdapter is an explicit local test backend."""
import os
import json
import sqlite3
from urllib.parse import quote
import requests
from dt.models import Twin


class Conflict(RuntimeError):
    pass


class SQLiteTwinAdapter:
    def __init__(self, path):
        self.db = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=' + ('NORMAL' if os.getenv('PERFSYNC_SQLITE_SYNCHRONOUS') == 'NORMAL' else 'FULL'))
        self.db.execute('CREATE TABLE IF NOT EXISTS twins (id TEXT PRIMARY KEY, version INTEGER, body TEXT)')

    def get(self, dt_id):
        row = self.db.execute('SELECT body FROM twins WHERE id=?', (dt_id,)).fetchone()
        return Twin.model_validate_json(row[0]) if row else None

    def commit(self, twin, expected_version):
        body = twin.model_dump_json()
        if expected_version is None:
            try:
                self.db.execute('INSERT INTO twins VALUES (?,?,?)', (twin.dt_id, twin.version, body))
            except sqlite3.IntegrityError as exc:
                raise Conflict('entity already exists') from exc
        else:
            result = self.db.execute('UPDATE twins SET version=?,body=? WHERE id=? AND version=?', (twin.version, body, twin.dt_id, expected_version))
            if result.rowcount != 1:
                raise Conflict('state version changed')


class DittoAdapter:
    def __init__(self, base_url, username=None, password=None, policy_id=None, timeout=10):
        self.base = base_url.rstrip('/') + '/api/2/things/'
        self.session = requests.Session()
        if username is not None:
            self.session.auth = (username, password)
        self.policy_id = policy_id
        self.timeout = timeout
        self.etags = {}

    def get(self, dt_id):
        response = self.session.get(self.base + quote(dt_id, safe=''), timeout=self.timeout)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        etag = response.headers.get('ETag')
        if not etag:
            raise RuntimeError('Ditto ETag required for conditional writes')
        self.etags[(dt_id, response.json()['attributes']['perfsync']['version'])] = etag
        return Twin.model_validate(response.json()['attributes']['perfsync'])

    def commit(self, twin, expected_version):
        if expected_version is None:
            headers = {'If-None-Match': '*'}
        else:
            etag = self.etags.get((twin.dt_id, expected_version))
            if not etag:
                raise Conflict('read expected version before commit')
            headers = {'If-Match': etag}
        body = {'thingId': twin.dt_id, 'attributes': {'perfsync': twin.model_dump()}}
        if self.policy_id:
            body['policyId'] = self.policy_id
        response = self.session.put(self.base + quote(twin.dt_id, safe=''), json=body, headers=headers, timeout=self.timeout)
        if response.status_code in (409, 412):
            raise Conflict('Ditto conditional write rejected')
        response.raise_for_status()
