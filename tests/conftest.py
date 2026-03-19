"""pytest configuration — ensure package is importable from repo root."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
