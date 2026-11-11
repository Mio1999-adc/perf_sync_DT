FROM openwhisk/action-python-v3.11@sha256:e59968159997672b4275c04d89cfacbb4c6fb7358cbd5bf691895d5461149a27
USER root
RUN pip install --no-cache-dir 'pydantic>=2,<3' 'requests>=2' 'opentelemetry-sdk>=1.27,<2'
COPY dt /opt/perfsync/dt
COPY synchronization /opt/perfsync/synchronization
COPY simulation/event_generator/schemas.py /opt/perfsync/simulation/event_generator/schemas.py
COPY perfsyncgnn/serverless/action.py /opt/perfsync/action.py
ENV PYTHONPATH=/opt/perfsync
RUN touch /opt/perfsync/simulation/__init__.py /opt/perfsync/simulation/event_generator/__init__.py
