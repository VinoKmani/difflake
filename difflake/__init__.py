"""difflake — git diff, but for your data lake. Powered by DuckDB."""

from difflake.core import LakeDiff
from difflake.models import DiffResult, RowDiff, SchemaDiff, StatsDiff

__version__ = "1.1.0"
__all__ = ["LakeDiff", "DiffResult", "SchemaDiff", "StatsDiff", "RowDiff"]
