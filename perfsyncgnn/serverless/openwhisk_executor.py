"""Blocking OpenWhisk REST execution followed by independent Ditto verification."""
import json
from pathlib import Path
import os
import time
from urllib.parse import quote
import requests
from opentelemetry import trace, propagate
from synchronization.executor import LocalExecutor, HANDLERS
from synchronization.operation_manager.preconditions import PreconditionError
from .config import environment
from synchronization.models import Status
from monitoring.telemetry import INVOCATIONS, FAILURES, RETRIES, INFLIGHT, LATENCY, EXECUTION

class OpenWhiskClient:
    def __init__(self, url=None, auth=None, ca=None):
        env = environment()
        self.url = (url or env['OPENWHISK_API_URL']).rstrip('/')
        self.session = requests.Session()
        self.session.auth = tuple((auth or env['OPENWHISK_AUTH']).split(':', 1))
        self.session.verify = ca or env.get('OPENWHISK_CA_BUNDLE') or True
    def endpoint(self, kind, name):
        return self.url + '/api/v1/namespaces/_/' + kind + '/' + quote(name, safe='/')
    def health(self):
        r = self.session.get(self.url + '/api/v1/namespaces', timeout=5)
        r.raise_for_status()
        return True
    def invoke(self, name, payload):
        r = self.session.post(self.endpoint('actions', name), params={'blocking':'true', 'result':'false', 'timeout':60000}, json=payload, timeout=70)
        try:
            body = r.json()
        except ValueError:
            r.raise_for_status()
            raise RuntimeError('OpenWhisk returned a non-JSON response')
        # Failed blocking activations use HTTP 502 but still carry authoritative IDs.
        if isinstance(body,dict) and body.get('activationId') and 'response' in body:
            return body
        r.raise_for_status()
        if r.status_code == 202:
            activation = body['activationId']
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                r = self.session.get(self.endpoint('activations', activation), timeout=10)
                if r.status_code == 404:
                    time.sleep(.25)
                    continue
                r.raise_for_status()
                body = r.json()
                break
            else:
                raise TimeoutError('activation completion not observable')
        return body

class OpenWhiskExecutor(LocalExecutor):
    def __init__(self, adapter, journal, client=None):
        super().__init__(adapter, journal)
        self.client = client or OpenWhiskClient()
    def execute(self, op):
        tracer = trace.get_tracer('perfsyncgnn')
        with tracer.start_as_current_span('synchronization', attributes={'operation.id':op.operation_id, 'operation.type':op.operation_type, 'run.id':op.event['run_id'], 'source.event':op.event['event_id']}) as span:
            if op.status == Status.CREATED: self.move(op, Status.VALIDATED)
            if op.status == Status.VALIDATED: self.move(op, Status.QUEUED)
            if op.status == Status.QUEUED:
                op.start_unix_ns = time.time_ns()
                self.move(op, Status.RUNNING)
            context = {}
            propagate.inject(context)
            op.trace_context = context
            for attempt in range(3):
                INFLIGHT.inc()
                try:
                    twin = self.adapter.get(op.dt_id)
                    payload = {'operation':op.model_dump(mode='json'), 'expected_version':twin.version if twin else None,
                               'current_owner':twin.owner if twin else None, 'trace_context':context}
                    INVOCATIONS.labels(op.operation_type).inc()
                    with tracer.start_as_current_span('openwhisk.invoke'):
                        activation = self.client.invoke('perfsync/' + op.operation_type.lower(), payload)
                    op.activation_id = activation.get('activationId')
                    op.function_metadata = {k:activation[k] for k in ('start','end','duration','annotations') if k in activation}
                    response = activation.get('response', {})
                    with (Path(self.journal.lock_path).parent/'invocations.jsonl').open('a') as log:
                        log.write(json.dumps({'operation_id':op.operation_id,'activation_id':op.activation_id,'operation_type':op.operation_type,'platform_success':response.get('success'),'retry_count':op.retry_count})+'\n')
                    if not response.get('success'):
                        raise RuntimeError('OpenWhisk activation failed: ' + str(response.get('status', 'unknown')))
                    result = response.get('result', {})
                    op.function_metadata.update({k:result[k] for k in ('function_start_unix_ns','function_end_unix_ns') if k in result})
                    if result.get('error_category'):
                        with (Path(self.journal.lock_path).parent/'remote-traces.jsonl').open('a') as log:
                            for remote_span in result.get('spans',[]): log.write(json.dumps(remote_span)+'\n')
                    if result.get('error_category') in {'ValueError','PreconditionError'}:
                        FAILURES.labels(op.operation_type).inc()
                        self.journal.save(op)
                        raise PreconditionError(result.get('message','remote precondition failed'))
                    if result.get('error_category'):
                        raise RuntimeError(result['error_category'])
                    op.function_metadata.update({k:result[k] for k in ('function_start_unix_ns','function_end_unix_ns') if k in result})
                    annotations = {a['key']:a['value'] for a in activation.get('annotations',[]) if 'key' in a and 'value' in a}
                    op.function_metadata['cold_start_observed'] = annotations['initTime'] > 0 if isinstance(annotations.get('initTime'), (int,float)) else None
                    path = Path(self.journal.lock_path).parent
                    with (path/'activations.jsonl').open('a') as log:
                        log.write(json.dumps({'operation_id':op.operation_id,'activation_id':op.activation_id,'function':op.operation_type,'retry_count':op.retry_count,'metadata':op.function_metadata})+'\n')
                    with (path/'remote-traces.jsonl').open('a') as log:
                        for remote_span in result.get('spans',[]): log.write(json.dumps(remote_span)+'\n')
                    if result.get('operation_id') != op.operation_id or result.get('dt_id') != op.dt_id:
                        raise RuntimeError('activation identity mismatch')
                    if op.status == Status.RUNNING: self.move(op, Status.COMMITTING)
                    with tracer.start_as_current_span('ditto.commit_validation'):
                        verified = self.adapter.get(op.dt_id)
                        receipt = verified.applied_operations.get(op.operation_id) if verified else None
                        if not receipt or receipt.get('operation_type') != op.operation_type or receipt.get('event_fingerprint') != op.payload['event_fingerprint']:
                            raise RuntimeError('Ditto durable commit receipt missing or invalid')
                        if receipt['version_after'] != result.get('version_after') or verified.version < receipt['version_after']:
                            raise RuntimeError('Ditto receipt version mismatch')
                    op.version_before, op.version_after, op.owner = receipt['version_before'], receipt['version_after'], receipt['owner']
                    if verified.version == op.version_after and not HANDLERS[op.operation_type].verify(verified,op):
                        raise RuntimeError('Ditto committed postcondition invalid')
                    if op.status == Status.RUNNING: self.move(op, Status.COMMITTING)
                    op.commit_unix_ns, op.commit_monotonic_ns = time.time_ns(), time.monotonic_ns()
                    op.duration_ns = op.commit_monotonic_ns - op.request_monotonic_ns if op.clock_session == self.clock_session else None
                    op.error = op.error_category = None
                    self.move(op, Status.COMMITTED)
                    if op.duration_ns is not None: LATENCY.labels(op.operation_type).observe(op.duration_ns / 1e9)
                    if activation.get('duration') is not None: EXECUTION.labels(op.operation_type).observe(activation['duration']/1000)
                    span.set_attribute('openwhisk.activation_id', op.activation_id or '')
                    return op
                except (requests.RequestException, RuntimeError, TimeoutError):
                    FAILURES.labels(op.operation_type).inc()
                    if attempt == 2: raise
                    op.retry_count += 1
                    RETRIES.labels(op.operation_type).inc()
                    self.journal.save(op)
                    time.sleep(.2 * 2**attempt)
                finally:
                    INFLIGHT.dec()
