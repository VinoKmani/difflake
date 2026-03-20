"""
Structured logging for difflake.

Provides two formatters:
  text  — human-readable: "2026-03-19 12:34:56 [INFO ] difflake: message"
  json  — one JSON object per line, suitable for log aggregation pipelines

Usage from library code::

    from difflake.logging_setup import get_logger
    log = get_logger(__name__)
    log.info("diff started", extra={"source": src, "target": tgt})

Usage from CLI (called once at startup)::

    from difflake.logging_setup import configure
    configure(level="INFO", fmt="text", log_file=None)
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_ROOT = "difflake"
_HANDLER_NAME = "difflake_handler"


# ── Formatters ────────────────────────────────────────────────────────────────

class _TextFormatter(logging.Formatter):
    _LEVEL_WIDTH = 7
    _COLOURS = {
        "DEBUG":    "\033[36m",   # cyan
        "INFO":     "\033[32m",   # green
        "WARNING":  "\033[33m",   # yellow
        "ERROR":    "\033[31m",   # red
        "CRITICAL": "\033[35m",   # magenta
    }
    _RESET = "\033[0m"

    def __init__(self, colour: bool = True) -> None:
        super().__init__()
        self.colour = colour and sys.stderr.isatty()

    def format(self, record: logging.LogRecord) -> str:
        ts    = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(self._LEVEL_WIDTH)
        name  = record.name
        msg   = record.getMessage()

        # Append any extra structured fields
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in logging.LogRecord.__dict__ and not k.startswith("_")
            and k not in {"message", "asctime", "args", "exc_info",
                          "exc_text", "stack_info", "taskName"}
        }
        extra_str = "  " + "  ".join(f"{k}={v}" for k, v in extras.items()) if extras else ""

        if self.colour:
            colour = self._COLOURS.get(record.levelname, "")
            line = f"{ts} {colour}[{level}]{self._RESET} {name}: {msg}{extra_str}"
        else:
            line = f"{ts} [{level}] {name}: {msg}{extra_str}"

        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        # Include any extra structured fields passed via extra={}
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in logging.LogRecord.__dict__ and not k.startswith("_")
            and k not in {"message", "asctime", "args", "exc_info",
                          "exc_text", "stack_info", "taskName"}
        }
        payload.update(extras)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


# ── Public API ────────────────────────────────────────────────────────────────

def configure(
    level: str = "WARNING",
    fmt: str = "text",
    log_file: str | None = None,
) -> None:
    """
    Configure the difflake root logger.

    Args:
        level:    Python log level name: DEBUG / INFO / WARNING / ERROR.
        fmt:      "text" (human-readable) or "json" (structured).
        log_file: Optional path — if given, logs go to this file in addition
                  to stderr. The file receives plain text (no ANSI colours).
    """
    root = logging.getLogger(_ROOT)
    root.setLevel(getattr(logging, level.upper(), logging.WARNING))

    # Remove any handler we previously installed (idempotent)
    root.handlers = [h for h in root.handlers if getattr(h, "_difflake", False) is False]

    formatter: logging.Formatter = (
        _JsonFormatter() if fmt.lower() == "json" else _TextFormatter(colour=True)
    )

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    stderr_handler._difflake = True  # type: ignore[attr-defined]
    root.addHandler(stderr_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(
            _TextFormatter(colour=False) if fmt.lower() != "json" else _JsonFormatter()
        )
        file_handler._difflake = True  # type: ignore[attr-defined]
        root.addHandler(file_handler)

    # Prevent log records from bubbling up to the root Python logger
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the difflake namespace."""
    if name.startswith(_ROOT):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT}.{name}")
