"""JSON Reporter — serializes DiffResult to machine-readable JSON."""

from __future__ import annotations

import json
from pathlib import Path

from difflake.models import DiffResult


class JsonReporter:
    def __init__(self, result: DiffResult):
        self.result = result

    def render(self, path: str | Path | None = None, indent: int = 2) -> str:
        """Return JSON string and optionally write to file."""
        data = self.result.to_dict()
        output = json.dumps(data, indent=indent, default=str)
        if path:
            Path(path).write_text(output)
        return output
