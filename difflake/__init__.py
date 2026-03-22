"""difflake — git diff, but for your data lake. Powered by DuckDB."""

from difflake.core import DiffLake
from difflake.models import DiffResult, RowDiff, SchemaDiff, StatsDiff

__version__ = "1.1.1"
__all__ = ["DiffLake", "DiffResult", "SchemaDiff", "StatsDiff", "RowDiff"]
