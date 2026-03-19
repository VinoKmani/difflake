"""
Core data models for LakeDiff results.
All diff outputs are structured as these dataclasses for
easy serialization and downstream consumption.
No Polars or DuckDB dependency — pure Python.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class ChangeType(str, Enum):
    ADDED     = "added"
    REMOVED   = "removed"
    CHANGED   = "changed"
    UNCHANGED = "unchanged"


# ── Schema Diff ────────────────────────────────────────────────────────────

@dataclass
class ColumnSchemaDiff:
    name: str
    change_type: ChangeType
    old_dtype: str | None = None
    new_dtype: str | None = None
    rename_from: str | None = None
    rename_similarity: float | None = None


@dataclass
class SchemaDiff:
    added_columns: list[ColumnSchemaDiff]       = field(default_factory=list)
    removed_columns: list[ColumnSchemaDiff]     = field(default_factory=list)
    type_changed_columns: list[ColumnSchemaDiff]= field(default_factory=list)
    renamed_columns: list[ColumnSchemaDiff]     = field(default_factory=list)
    order_changed: bool                         = False
    old_column_order: list[str]                 = field(default_factory=list)
    new_column_order: list[str]                 = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_columns or self.removed_columns
            or self.type_changed_columns or self.renamed_columns
            or self.order_changed
        )

    def summary(self) -> str:
        parts = []
        if self.added_columns:
            parts.append(f"+{len(self.added_columns)} added")
        if self.removed_columns:
            parts.append(f"-{len(self.removed_columns)} removed")
        if self.type_changed_columns:
            parts.append(f"~{len(self.type_changed_columns)} type changed")
        if self.renamed_columns:
            parts.append(f"↔ {len(self.renamed_columns)} renamed")
        return ", ".join(parts) if parts else "No schema changes"


# ── Stats Diff ─────────────────────────────────────────────────────────────

@dataclass
class ColumnStatsDiff:
    column: str
    dtype_category: str   # "numeric" | "categorical" | "datetime" | "other"

    null_rate_before: float | None = None
    null_rate_after:  float | None = None

    mean_before:     float | None = None
    mean_after:      float | None = None
    mean_drift_pct:  float | None = None

    median_before:   float | None = None
    median_after:    float | None = None

    std_before:      float | None = None
    std_after:       float | None = None

    min_before:      float | None = None
    min_after:       float | None = None

    max_before:      float | None = None
    max_after:       float | None = None

    cardinality_before: int | None = None
    cardinality_after:  int | None = None

    new_categories:     list[str] = field(default_factory=list)
    dropped_categories: list[str] = field(default_factory=list)

    kl_divergence: float | None = None
    drift_score:   float | None = None

    is_drifted:    bool       = False
    drift_reasons: list[str]  = field(default_factory=list)


@dataclass
class StatsDiff:
    column_diffs:     list[ColumnStatsDiff] = field(default_factory=list)
    drifted_columns:  list[str]             = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.drifted_columns)


# ── Row Diff ───────────────────────────────────────────────────────────────

@dataclass
class RowDiff:
    row_count_before: int = 0
    row_count_after:  int = 0
    rows_added:       int = 0
    rows_removed:     int = 0
    rows_changed:     int = 0
    rows_unchanged:   int = 0

    sample_added:   list[dict[str, Any]] = field(default_factory=list)
    sample_removed: list[dict[str, Any]] = field(default_factory=list)
    sample_changed: list[dict[str, Any]] = field(default_factory=list)

    primary_key_used: str | None = None
    key_based_diff:   bool          = False
    sampled:          bool          = False
    sample_size_used: int | None = None
    key_error:        str | None = None  # set when key validation fails

    @property
    def row_count_delta(self) -> int:
        return self.row_count_after - self.row_count_before

    @property
    def row_count_delta_pct(self) -> float:
        if self.row_count_before == 0:
            return float("inf")
        return (self.row_count_delta / self.row_count_before) * 100


# ── Top-Level Result ───────────────────────────────────────────────────────

@dataclass
class DiffResult:
    source_path:   str
    target_path:   str
    source_format: str
    target_format: str

    schema_diff: SchemaDiff = field(default_factory=SchemaDiff)
    stats_diff:  StatsDiff  = field(default_factory=StatsDiff)
    row_diff:    RowDiff    = field(default_factory=RowDiff)

    drift_alerts:    list[str]     = field(default_factory=list)
    threshold_used:  float | None = None
    diff_mode:       str             = "full"
    elapsed_seconds: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self, path: str | None = None, indent: int = 2) -> str | None:
        data    = self.to_dict()
        output  = json.dumps(data, indent=indent, default=str)
        if path:
            with open(path, "w") as f:
                f.write(output)
            return None
        return output

    def to_html(self, path: str) -> None:
        from difflake.reporters.html_reporter import HtmlReporter
        HtmlReporter(self).write(path)

    def to_markdown(self, path: str | None = None) -> str | None:
        from difflake.reporters.markdown_reporter import MarkdownReporter
        return MarkdownReporter(self).render(path)
