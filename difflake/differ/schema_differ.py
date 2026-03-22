"""
Schema Differ — detects column additions, removals, type changes, and renames.
Uses DuckDB DESCRIBE to fetch schema without loading any row data.
Works on ALL formats including remote (S3, GCS, Delta, Iceberg).

Rename detection uses Jaro-Winkler string similarity.
"""

from __future__ import annotations

from difflib import SequenceMatcher

from difflake.connection import DuckDBConnection
from difflake.models import ChangeType, ColumnSchemaDiff, SchemaDiff

_RENAME_SIMILARITY_THRESHOLD = 0.75


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


class SchemaDiffer:
    """
    Compares schemas of old and new views already registered in DuckDB.
    Never loads row data — uses DESCRIBE which reads only metadata.
    """

    def __init__(self, con: DuckDBConnection, old_view: str, new_view: str):
        self.con       = con
        self.old_view  = old_view
        self.new_view  = new_view

    def run(self) -> SchemaDiff:
        old_schema = dict(self.con.columns(self.old_view))  # {col: dtype}
        new_schema = dict(self.con.columns(self.new_view))

        old_cols = set(old_schema)
        new_cols = set(new_schema)

        raw_added   = new_cols - old_cols
        raw_removed = old_cols - new_cols

        # ── Rename detection (Jaro-Winkler) ──────────────────────────────
        renamed: list[ColumnSchemaDiff] = []
        confirmed_added   = set(raw_added)
        confirmed_removed = set(raw_removed)

        for removed_col in list(raw_removed):
            best_match: str | None = None
            best_score = 0.0
            for added_col in list(raw_added):
                score = _similarity(removed_col, added_col)
                if score > best_score and score >= _RENAME_SIMILARITY_THRESHOLD:
                    best_score = score
                    best_match = added_col
            if best_match:
                renamed.append(ColumnSchemaDiff(
                    name=best_match,
                    change_type=ChangeType.CHANGED,
                    rename_from=removed_col,
                    rename_similarity=round(best_score, 3),
                    old_dtype=old_schema[removed_col],
                    new_dtype=new_schema[best_match],
                ))
                confirmed_added.discard(best_match)
                confirmed_removed.discard(removed_col)

        # ── Added / removed ────────────────────────────────────────────────
        added = [
            ColumnSchemaDiff(name=c, change_type=ChangeType.ADDED,
                             new_dtype=new_schema[c])
            for c in sorted(confirmed_added)
        ]
        removed = [
            ColumnSchemaDiff(name=c, change_type=ChangeType.REMOVED,
                             old_dtype=old_schema[c])
            for c in sorted(confirmed_removed)
        ]

        # ── Type-changed columns ───────────────────────────────────────────
        type_changed = [
            ColumnSchemaDiff(
                name=c,
                change_type=ChangeType.CHANGED,
                old_dtype=old_schema[c],
                new_dtype=new_schema[c],
            )
            for c in sorted(old_cols & new_cols)
            if old_schema[c] != new_schema[c]
        ]

        # ── Column order change ────────────────────────────────────────────
        old_order    = list(old_schema.keys())
        new_order    = list(new_schema.keys())
        common_old   = [c for c in old_order if c in new_cols]
        common_new   = [c for c in new_order if c in old_cols]
        order_changed = common_old != common_new

        return SchemaDiff(
            added_columns=added,
            removed_columns=removed,
            type_changed_columns=type_changed,
            renamed_columns=renamed,
            order_changed=order_changed,
            old_column_order=old_order,
            new_column_order=new_order,
        )
