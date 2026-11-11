"""Process/TraCI observations, separate from authoritative lifecycle records."""
import json
import os
import time
from pathlib import Path


class RuntimeReporter:
    def __init__(self, path=None):
        self.path=Path(path) if path else None
        self.state={'pid':os.getpid()}

    def update(self, **fields):
        self.state.update(fields)
        self.state['observed_unix_ns']=time.time_ns()
        if self.path:
            self.path.parent.mkdir(parents=True,exist_ok=True)
            temporary=self.path.with_name(self.path.name+'.tmp')
            temporary.write_text(json.dumps(self.state))
            temporary.replace(self.path)
