"""OTel JSONL and Prometheus aggregates; simulation and execution clocks stay separate."""
import json
import os
import threading
import time
from pathlib import Path
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

INVOCATIONS=Counter('perfsync_invocations_total','OpenWhisk requests',['operation'])
FAILURES=Counter('perfsync_invocation_failures_total','Invocation or verification failures',['operation'])
RETRIES=Counter('perfsync_retries_total','Retried requests',['operation'])
INFLIGHT=Gauge('perfsync_invocations_inflight','Requests outstanding in this manager process')
LATENCY=Histogram('perfsync_sync_seconds','Verified success duration in caller monotonic clock',['operation'])
EXECUTION=Histogram('perfsync_function_seconds','Platform activation duration',['operation'])
STORAGE=Histogram('perfsync_journal_save_seconds','Local SQLite WAL save transaction elapsed time, including commit')
CPU=Gauge('perfsync_host_cpu_percent','Host CPU usage, interval sampled')
MEMORY=Gauge('perfsync_host_memory_used_bytes','Host memory used bytes')
NET=Gauge('perfsync_host_network_bytes','Host cumulative network bytes',['direction'])

class JSONExporter(SpanExporter):
    def __init__(self,path): self.path=Path(path); self.lock=threading.Lock()
    def export(self,spans):
        with self.lock,self.path.open('a') as out:
            for span in spans: out.write(span.to_json(indent=None)+'\n')
        return SpanExportResult.SUCCESS

def configure(path):
    provider=TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(JSONExporter(path)))
    trace.set_tracer_provider(provider)
    return provider

def resources():
    import psutil
    net=psutil.net_io_counters(); memory=psutil.virtual_memory()
    pressure={}
    for name in ('cpu','memory','io'):
        path=Path('/proc/pressure')/name
        if path.exists(): pressure[name]=path.read_text().strip()
    measured={'host_cpu_percent':psutil.cpu_percent(),'host_memory_used_bytes':memory.used,
              'host_memory_available_bytes':memory.available,'host_network_sent_bytes':net.bytes_sent,
              'host_network_received_bytes':net.bytes_recv,'linux_pressure_stall':pressure}
    CPU.set(measured['host_cpu_percent']);MEMORY.set(memory.used)
    NET.labels('sent').set(net.bytes_sent);NET.labels('received').set(net.bytes_recv)
    return {'observed_unix_ns':time.time_ns(),'measured':measured,
            'derived':{},'unavailable':['pod_utilization: run scripts/collect-phase3-resources for real nested Docker statistics or metrics-server',
                'broker_backlog: no cross-broker queue measurement configured','remote_ditto_storage_latency: not instrumented; local journal save latency is measured separately',
                'queue_delay: no reliable cross-host time subtraction','cold_start: use activation initTime annotation when present']}

def export_metrics(path):
    path=Path(path);tmp=path.with_suffix('.tmp')
    tmp.write_bytes(generate_latest());os.replace(tmp,path)

class Sampler:
    def __init__(self,output):
        self.output=Path(output);self.stop=threading.Event()
        self.thread=threading.Thread(target=self.run,daemon=True)
    def start(self): self.thread.start()
    def run(self):
        while not self.stop.is_set():
            with (self.output/'resources.jsonl').open('a') as out: out.write(json.dumps(resources())+'\n')
            export_metrics(self.output/'metrics.prom')
            self.stop.wait(2)
    def close(self):
        self.stop.set();self.thread.join(timeout=3);export_metrics(self.output/'metrics.prom')

class TracedAdapter:
    def __init__(self,adapter): self.adapter=adapter
    def get(self,identity):
        with trace.get_tracer('perfsyncgnn').start_as_current_span('ditto.read_or_validate' if hasattr(self.adapter,'session') else 'sqlite.read'):
            return self.adapter.get(identity)
    def commit(self,twin,expected):
        with trace.get_tracer('perfsyncgnn').start_as_current_span('ditto.conditional_write' if hasattr(self.adapter,'session') else 'sqlite.commit'):
            return self.adapter.commit(twin,expected)
