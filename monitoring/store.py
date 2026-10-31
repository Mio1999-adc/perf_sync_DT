"""Durable inbox/operation journal. It never stores authoritative twin state."""
import fcntl
import os
import json
import sqlite3
import time
from contextlib import contextmanager
from synchronization.models import Operation


class Journal:
    def __init__(self, path):
        self.lock_path = str(path) + '.lock'
        self.db = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=' + ('NORMAL' if os.getenv('PERFSYNC_SQLITE_SYNCHRONOUS') == 'NORMAL' else 'FULL'))
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS inbox (key TEXT PRIMARY KEY, fingerprint TEXT, body TEXT, done INTEGER DEFAULT 0, outcome TEXT);
        CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, body TEXT);
        CREATE INDEX IF NOT EXISTS operations_event_key ON operations(json_extract(body, '$.payload.event_key'));
        CREATE TABLE IF NOT EXISTS lifecycle (ordinal INTEGER PRIMARY KEY, body TEXT);
        CREATE TABLE IF NOT EXISTS outbox (key TEXT PRIMARY KEY, body TEXT, sent INTEGER DEFAULT 0);
        ''')

    @contextmanager
    def worker_lock(self):
        with open(self.lock_path, 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def ingest(self, event):
        old = self.db.execute('SELECT fingerprint FROM inbox WHERE key=?', (event.deduplication_id,)).fetchone()
        if old and old[0] != event.fingerprint:
            raise ValueError('conflicting event identity')
        self.db.execute('INSERT OR IGNORE INTO inbox(key,fingerprint,body) VALUES (?,?,?)', (event.deduplication_id, event.fingerprint, event.model_dump_json()))

    def save(self, operation):
        if not self.db.in_transaction:
            with self.transaction():
                self.save(operation)
            return
        self.db.execute('INSERT INTO operations VALUES (?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body', (operation.operation_id, operation.model_dump_json()))
        record = operation.model_dump(mode='json')
        record['recorded_unix_ns'] = time.time_ns()
        record['physical_entity_id'] = operation.event['entity_id']
        record['source_rsu'] = operation.event['source_id']
        self.db.execute('INSERT INTO lifecycle(body) VALUES (?)', (json.dumps(record),))

    def operations(self, event_key=None):
        if event_key is None:
            rows = self.db.execute('SELECT body FROM operations ORDER BY rowid')
        else:
            rows = self.db.execute("SELECT body FROM operations WHERE json_extract(body, '$.payload.event_key')=? ORDER BY rowid", (event_key,))
        return [Operation.model_validate_json(r[0]) for r in rows]

    def export(self, path):
        with open(path, 'w') as output:
            for row in self.db.execute('SELECT body FROM lifecycle ORDER BY ordinal'):
                output.write(row[0] + '\n')
