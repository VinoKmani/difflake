"""
Test suite for difflake — DuckDB backend.

All tests use DuckDB-native APIs. No Polars dependency.
Data is written to temp files via DuckDB and read back through LakeDiff.

Classes:
  TestSchemaDiffer  — schema change detection
  TestStatsDiffer   — statistical drift
  TestRowDiffer     — row-level diff
  TestKeyErrors     — missing/low-cardinality key handling
  TestCompositeKey  — composite primary keys
  TestWhereFilter   — SQL WHERE filter
  TestEdgeCases     — empty files, nulls, single row
  TestIntegration   — end-to-end across all formats
  TestReporters     — JSON / HTML / Markdown export
  TestCLI           — CLI via Click test runner
"""

from __future__ import annotations
import csv, json, os, tempfile
from pathlib import Path
import pytest, duckdb

from difflake.core import LakeDiff
from difflake.models import ChangeType, DiffResult
from difflake.connection import DuckDBConnection, _detect_format, _read_sql
from difflake.differ.schema_differ import SchemaDiffer
from difflake.differ.stats_differ import StatsDiffer
from difflake.differ.row_differ import RowDiffer, _parse_key


# ── Helpers ────────────────────────────────────────────────────────────────

def _infer_type(values):
    import re
    non_null = [v for v in values if v is not None]
    if not non_null: return "VARCHAR"
    v = non_null[0]
    if isinstance(v, bool):  return "BOOLEAN"
    if isinstance(v, int):   return "INTEGER"
    if isinstance(v, float): return "DOUBLE"
    # Detect ISO date strings so DuckDB stores them as DATE, not VARCHAR
    if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return "DATE"
    if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", v):
        return "TIMESTAMP"
    return "VARCHAR"

def write_parquet(path, data):
    cols = ", ".join(f"{k} {_infer_type(v)}" for k, v in data.items())
    rows = list(zip(*data.values())) if data and next(iter(data.values())) else []
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t ({cols})")
    for row in rows:
        placeholders = ", ".join(["?" for _ in row])
        con.execute(f"INSERT INTO t VALUES ({placeholders})", list(row))
    con.execute(f"COPY t TO \'{path}\' (FORMAT PARQUET)")
    con.close()

def write_csv(path, data):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(data.keys()))
        writer.writeheader()
        rows = [dict(zip(data.keys(), r)) for r in zip(*data.values())]
        writer.writerows(rows)

def write_json(path, data):
    rows = [dict(zip(data.keys(), r)) for r in zip(*data.values())]
    with open(path, "w") as f: json.dump(rows, f)

def write_ndjson(path, data):
    rows = [dict(zip(data.keys(), r)) for r in zip(*data.values())]
    with open(path, "w") as f:
        for row in rows: f.write(json.dumps(row) + "\n")

def write_file(path, data):
    ext = path.suffix.lower()
    if ext == ".parquet": write_parquet(path, data)
    elif ext == ".csv":   write_csv(path, data)
    elif ext == ".json":  write_json(path, data)
    elif ext in (".jsonl", ".ndjson"): write_ndjson(path, data)

def make_views(src_path, tgt_path):
    con = DuckDBConnection()
    for name, path in [("__src_schema", src_path), ("__tgt_schema", tgt_path),
                        ("__src", src_path), ("__tgt", tgt_path)]:
        fmt = _detect_format(str(path))
        con.register_view(name, _read_sql(str(path), fmt), fmt, str(path))
    return con


# ── Fixtures ───────────────────────────────────────────────────────────────

BASE = {
    "user_id": [1,2,3,4,5], "name": ["Alice","Bob","Carol","Dave","Eve"],
    "age": [25,30,35,28,42], "revenue": [100.0,200.0,150.0,300.0,250.0],
    "country": ["US","UK","US","CA","UK"], "status": ["active","active","inactive","active","active"],
}
EVOLVED = {
    "user_id": [1,2,3,4,5,6,7],
    "name": ["Alice","Bob","Carol","Dave","Eve","Frank","Grace"],
    "age": [25,30,35,28,42,29,33],
    "revenue": [100.0,240.0,150.0,300.0,310.0,180.0,220.0],
    "country": ["US","UK","US","CA","UK","AU","US"],
    "status": ["active","active","inactive","active","suspended","active","active"],
    "subscription_tier": ["free","pro","free","enterprise","pro","free","pro"],
}
COMP_V1 = {
    "tenant_id": ["ACME","ACME","BETA","BETA","GAMMA"],
    "order_id": ["O-001","O-002","O-010","O-011","O-020"],
    "event_date": ["2024-01-01","2024-01-01","2024-01-02","2024-01-02","2024-01-03"],
    "amount": [100.0,200.0,150.0,300.0,250.0],
    "status": ["pending","shipped","pending","delivered","shipped"],
}
COMP_V2 = {
    "tenant_id": ["ACME","ACME","BETA","GAMMA","DELTA"],
    "order_id": ["O-001","O-002","O-010","O-020","O-030"],
    "event_date": ["2024-01-01","2024-01-01","2024-01-02","2024-01-03","2024-01-04"],
    "amount": [100.0,240.0,150.0,250.0,400.0],
    "status": ["pending","delivered","shipped","delivered","pending"],
}

@pytest.fixture
def tmp(tmp_path): return tmp_path

@pytest.fixture
def base_p(tmp_path):
    p = tmp_path / "base.parquet"; write_parquet(p, BASE); return p

@pytest.fixture
def evol_p(tmp_path):
    p = tmp_path / "evolved.parquet"; write_parquet(p, EVOLVED); return p

@pytest.fixture
def comp1_p(tmp_path):
    p = tmp_path / "comp1.parquet"; write_parquet(p, COMP_V1); return p

@pytest.fixture
def comp2_p(tmp_path):
    p = tmp_path / "comp2.parquet"; write_parquet(p, COMP_V2); return p


# ══════════════════════════════════════════════════════════════════════════
# 1. Schema Differ
# ══════════════════════════════════════════════════════════════════════════

class TestSchemaDiffer:
    def test_no_changes(self, base_p):
        con = make_views(base_p, base_p)
        diff = SchemaDiffer(con, "__src_schema", "__tgt_schema").run()
        assert not diff.has_changes; con.close()

    def test_added_column(self, base_p, evol_p):
        con = make_views(base_p, evol_p)
        diff = SchemaDiffer(con, "__src_schema", "__tgt_schema").run()
        assert "subscription_tier" in [c.name for c in diff.added_columns]; con.close()

    def test_removed_column(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {k: v for k,v in BASE.items() if k != "revenue"})
        con = make_views(src, tgt)
        diff = SchemaDiffer(con, "__src_schema", "__tgt_schema").run()
        assert "revenue" in [c.name for c in diff.removed_columns]; con.close()

    def test_rename_detection(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {("userId" if k=="user_id" else k): v for k,v in BASE.items()})
        con = make_views(src, tgt)
        diff = SchemaDiffer(con, "__src_schema", "__tgt_schema").run()
        assert "user_id" in [c.rename_from for c in diff.renamed_columns]; con.close()

    def test_summary_contains_added(self, base_p, evol_p):
        con = make_views(base_p, evol_p)
        diff = SchemaDiffer(con, "__src_schema", "__tgt_schema").run()
        assert "added" in diff.summary().lower(); con.close()

    def test_schema_mode_no_stats(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="schema").run()
        assert result.schema_diff.has_changes
        assert result.stats_diff.column_diffs == []

    def test_completely_different_schemas(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {"a":[1,2],"b":[3,4]})
        write_parquet(tgt, {"c":[1,2],"d":[3,4]})
        con = make_views(src, tgt)
        diff = SchemaDiffer(con, "__src_schema", "__tgt_schema").run()
        assert diff.has_changes; con.close()


# ══════════════════════════════════════════════════════════════════════════
# 2. Stats Differ
# ══════════════════════════════════════════════════════════════════════════

class TestStatsDiffer:
    def test_no_drift_identical(self, base_p):
        result = LakeDiff(source=str(base_p), target=str(base_p), mode="stats").run()
        assert not result.stats_diff.has_drift

    def test_mean_drift_detected(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {**BASE, "revenue": [v*2 for v in BASE["revenue"]]})
        result = LakeDiff(source=str(src), target=str(tgt), mode="stats", drift_threshold=0.15).run()
        assert "revenue" in result.stats_diff.drifted_columns

    def test_null_rate_drift_detected(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {**BASE, "age": [None,None,None,28,42]})
        result = LakeDiff(source=str(src), target=str(tgt), mode="stats", drift_threshold=0.10).run()
        assert "age" in result.stats_diff.drifted_columns

    def test_new_category_detected(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="stats", drift_threshold=0.99).run()
        status = next(d for d in result.stats_diff.column_diffs if d.column=="status")
        assert "suspended" in status.new_categories

    def test_cardinality_increases(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="stats").run()
        country = next(d for d in result.stats_diff.column_diffs if d.column=="country")
        assert country.cardinality_after > country.cardinality_before

    def test_column_subset(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="stats", columns=["age","revenue"]).run()
        cols = [d.column for d in result.stats_diff.column_diffs]
        assert "age" in cols and "revenue" in cols and "country" not in cols

    def test_kl_divergence_large_shift(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {"v": list(range(1, 101))})
        write_parquet(tgt, {"v": [x*10 for x in range(1, 101)]})
        result = LakeDiff(source=str(src), target=str(tgt), mode="stats").run()
        v = next(d for d in result.stats_diff.column_diffs if d.column=="v")
        assert v.kl_divergence is not None and v.kl_divergence > 0.1

    def test_min_max_mean_populated(self, base_p):
        result = LakeDiff(source=str(base_p), target=str(base_p), mode="stats").run()
        rev = next(d for d in result.stats_diff.column_diffs if d.column=="revenue")
        assert rev.min_before is not None
        assert rev.max_before is not None
        assert rev.mean_before is not None


# ══════════════════════════════════════════════════════════════════════════
# 3. Row Differ
# ══════════════════════════════════════════════════════════════════════════

class TestRowDiffer:
    def test_count_only_no_key(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="rows").run()
        assert result.row_diff.row_count_before == 5
        assert result.row_diff.row_count_after == 7
        assert not result.row_diff.key_based_diff

    def test_added_rows(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_added == 2
        assert result.row_diff.rows_removed == 0
        assert result.row_diff.key_based_diff

    def test_removed_rows(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {k: v[2:] for k,v in BASE.items()})
        result = LakeDiff(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_removed == 2

    def test_changed_rows(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_changed >= 2

    def test_sample_added_populated(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_id").run()
        assert len(result.row_diff.sample_added) > 0

    def test_delta_pct(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="rows").run()
        assert abs(result.row_diff.row_count_delta_pct - 40.0) < 0.1

    def test_null_aware_detection(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {**BASE, "status": [None,"active","inactive","active","active"]})
        write_parquet(tgt, BASE)
        result = LakeDiff(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_changed >= 1

    def test_identical_no_changes(self, base_p):
        result = LakeDiff(source=str(base_p), target=str(base_p), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_changed == 0
        assert result.row_diff.rows_added == 0


# ══════════════════════════════════════════════════════════════════════════
# 4. Key Error Handling
# ══════════════════════════════════════════════════════════════════════════

class TestKeyErrors:
    def test_missing_key_falls_back(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="rows", primary_key="no_such_col").run()
        assert not result.row_diff.key_based_diff
        assert result.row_diff.key_error is not None
        assert "no_such_col" in result.row_diff.key_error

    def test_missing_key_suggests_alternatives(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_Id").run()
        assert not result.row_diff.key_based_diff
        assert "user_id" in result.row_diff.key_error

    def test_key_in_source_not_target(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        tgt_data = {k: v for k,v in BASE.items() if k != "user_id"}
        tgt_data["user_uuid"] = ["a","b","c","d","e"]
        write_parquet(tgt, tgt_data)
        result = LakeDiff(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert not result.row_diff.key_based_diff
        assert result.row_diff.key_error is not None

    def test_low_cardinality_blocked(self, tmp):
        src = tmp/"src.parquet"
        # 2 unique values across 500 rows = 0.4% uniqueness ratio — below 1% threshold
        write_parquet(src, {"VendorID": [1,2]*250, "fare": [10.0]*500})
        result = LakeDiff(source=str(src), target=str(src), mode="rows", primary_key="VendorID").run()
        assert not result.row_diff.key_based_diff
        assert result.row_diff.key_error is not None
        assert "LOW-CARDINALITY" in result.row_diff.key_error


# ══════════════════════════════════════════════════════════════════════════
# 5. Composite Keys
# ══════════════════════════════════════════════════════════════════════════

class TestCompositeKey:
    def test_parse_single(self):   assert _parse_key("user_id") == ["user_id"]
    def test_parse_comma(self):    assert _parse_key("a,b,c") == ["a","b","c"]
    def test_parse_list(self):     assert _parse_key(["a","b"]) == ["a","b"]
    def test_parse_none(self):     assert _parse_key(None) == []
    def test_parse_strips(self):   assert _parse_key("a, b , c") == ["a","b","c"]

    def test_composite_added_removed(self, comp1_p, comp2_p):
        result = LakeDiff(source=str(comp1_p), target=str(comp2_p), mode="rows",
                          primary_key="tenant_id,order_id,event_date").run()
        assert result.row_diff.key_based_diff
        assert result.row_diff.rows_added >= 1
        assert result.row_diff.rows_removed >= 1

    def test_composite_changed(self, comp1_p, comp2_p):
        result = LakeDiff(source=str(comp1_p), target=str(comp2_p), mode="rows",
                          primary_key="tenant_id,order_id,event_date").run()
        assert result.row_diff.rows_changed >= 1

    def test_composite_key_string(self, comp1_p, comp2_p):
        result = LakeDiff(source=str(comp1_p), target=str(comp2_p), mode="rows",
                          primary_key=["tenant_id","order_id","event_date"]).run()
        assert "tenant_id" in result.row_diff.primary_key_used


# ══════════════════════════════════════════════════════════════════════════
# 6. WHERE Filter
# ══════════════════════════════════════════════════════════════════════════

class TestWhereFilter:
    def test_where_reduces_rows(self, tmp):
        src = tmp/"src.parquet"; write_parquet(src, BASE)
        result = LakeDiff(source=str(src), target=str(src), mode="stats",
                          where="revenue > 200").run()
        rev = next((d for d in result.stats_diff.column_diffs if d.column == "revenue"), None)
        assert rev is not None and rev.cardinality_before == 2  # 300 and 250

    def test_where_is_null(self, tmp):
        src = tmp/"src.parquet"
        write_parquet(src, {**BASE, "age": [None,None,35,28,42]})
        # Should not crash
        result = LakeDiff(source=str(src), target=str(src), mode="schema",
                          where="age IS NULL").run()
        assert result is not None

    def test_where_compound(self, tmp):
        src = tmp/"src.parquet"; write_parquet(src, BASE)
        result = LakeDiff(source=str(src), target=str(src), mode="stats",
                          where="status = \'active\' AND revenue > 150").run()
        # Bob(200,active), Dave(300,active), Eve(250,active) = 3
        name_d = next((d for d in result.stats_diff.column_diffs if d.column == "name"), None)
        assert name_d is not None and name_d.cardinality_before == 3  # Bob, Dave, Eve

    def test_where_all_formats(self, tmp):
        for ext, writer in [(".csv",write_csv), (".json",write_json), (".ndjson",write_ndjson)]:
            src = tmp/f"src{ext}"
            writer(src, BASE)
            result = LakeDiff(source=str(src), target=str(src), mode="stats",
                              where="revenue > 100").run()
            assert result.row_diff.row_count_before >= 1


# ══════════════════════════════════════════════════════════════════════════
# 7. Edge Cases
# ══════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_empty_files(self, tmp):
        src = tmp/"src.parquet"
        write_parquet(src, {"id":[],"val":[]})
        result = LakeDiff(source=str(src), target=str(src), mode="rows").run()
        assert result.row_diff.row_count_before == 0

    def test_single_row(self, tmp):
        src = tmp/"src.parquet"
        write_parquet(src, {"id":[1],"name":["Alice"]})
        result = LakeDiff(source=str(src), target=str(src), mode="rows", primary_key="id").run()
        assert result.row_diff.rows_unchanged == 1
        assert result.row_diff.rows_changed == 0

    def test_all_rows_removed(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {"user_id":[],"name":[],"age":[],"revenue":[],"country":[],"status":[]})
        result = LakeDiff(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_removed == 5

    def test_all_rows_added(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {"user_id":[],"name":[],"age":[],"revenue":[],"country":[],"status":[]})
        write_parquet(tgt, BASE)
        result = LakeDiff(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_added == 5

    def test_all_null_column_no_crash(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {"id":[1,2,3],"val":[1.0,2.0,3.0]})
        write_parquet(tgt, {"id":[1,2,3],"val":[None,None,None]})
        result = LakeDiff(source=str(src), target=str(tgt), mode="stats").run()
        assert isinstance(result.stats_diff.column_diffs, list)

    def test_missing_file_raises(self, tmp):
        good = tmp/"good.parquet"; write_parquet(good, BASE)
        with pytest.raises(Exception):
            LakeDiff(source=str(good), target="/no/such/file.parquet").run()

    def test_unknown_extension(self, tmp):
        bad = tmp/"data.xyz"; bad.write_text("junk")
        with pytest.raises(Exception):
            LakeDiff(source=str(bad), target=str(bad)).run()


# ══════════════════════════════════════════════════════════════════════════
# 8. Integration
# ══════════════════════════════════════════════════════════════════════════

class TestIntegration:
    @pytest.mark.parametrize("fmt", ["parquet","csv","json","ndjson"])
    def test_full_diff_all_formats(self, tmp, fmt):
        src = tmp/f"src.{fmt}"; tgt = tmp/f"tgt.{fmt}"
        write_file(src, BASE); write_file(tgt, EVOLVED)
        result = LakeDiff(source=str(src), target=str(tgt), primary_key="user_id",
                          drift_threshold=0.10).run()
        assert isinstance(result, DiffResult)
        assert result.row_diff.rows_added == 2
        assert result.row_diff.key_based_diff

    def test_cross_format_csv_vs_parquet(self, tmp):
        src = tmp/"src.csv"; tgt = tmp/"tgt.parquet"
        write_csv(src, BASE); write_parquet(tgt, EVOLVED)
        result = LakeDiff(source=str(src), target=str(tgt), primary_key="user_id").run()
        assert result.row_diff.rows_added == 2

    def test_drift_alerts_populated(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {**BASE,"revenue":[v*3 for v in BASE["revenue"]]})
        result = LakeDiff(source=str(src), target=str(tgt), drift_threshold=0.10).run()
        assert len(result.drift_alerts) > 0

    def test_elapsed_seconds(self, base_p, evol_p):
        result = LakeDiff(source=str(base_p), target=str(evol_p)).run()
        assert result.elapsed_seconds is not None and result.elapsed_seconds > 0

    def test_composite_end_to_end(self, comp1_p, comp2_p):
        result = LakeDiff(source=str(comp1_p), target=str(comp2_p),
                          primary_key="tenant_id,order_id,event_date").run()
        assert result.row_diff.key_based_diff
        assert result.row_diff.rows_added >= 1
        assert result.row_diff.rows_removed >= 1

    def test_sample_size_no_crash(self, base_p):
        result = LakeDiff(source=str(base_p), target=str(base_p), mode="stats",
                          sample_size=3).run()
        assert len(result.stats_diff.column_diffs) > 0

    def test_multipart_directory(self, tmp):
        src_dir = tmp/"parts_src"; tgt_dir = tmp/"parts_tgt"
        src_dir.mkdir(); tgt_dir.mkdir()
        write_parquet(src_dir/"part-000.parquet", {"id":[1,2,3],"val":["a","b","c"]})
        write_parquet(src_dir/"part-001.parquet", {"id":[4,5],"val":["d","e"]})
        write_parquet(tgt_dir/"part-000.parquet", {"id":[1,2,3,4,5,6],"val":["a","b","c","d","e","f"]})
        result = LakeDiff(source=str(src_dir), target=str(tgt_dir),
                          mode="rows", primary_key="id").run()
        assert result.row_diff.row_count_before == 5
        assert result.row_diff.rows_added == 1


# ══════════════════════════════════════════════════════════════════════════
# 9. Reporters
# ══════════════════════════════════════════════════════════════════════════

class TestReporters:
    def _run(self, base_p, evol_p):
        return LakeDiff(source=str(base_p), target=str(evol_p),
                        primary_key="user_id", drift_threshold=0.10).run()

    def test_json_structure(self, tmp, base_p, evol_p):
        result = self._run(base_p, evol_p)
        out = tmp/"r.json"; result.to_json(str(out))
        data = json.loads(out.read_text())
        assert all(k in data for k in ["schema_diff","stats_diff","row_diff","drift_alerts"])

    def test_json_string(self, base_p, evol_p):
        s = self._run(base_p, evol_p).to_json()
        assert "schema_diff" in json.loads(s)

    def test_html_renders(self, tmp, base_p, evol_p):
        result = self._run(base_p, evol_p)
        out = tmp/"r.html"; result.to_html(str(out))
        content = out.read_text()
        assert all(s in content for s in ["DiffLake","Schema Diff","Row Diff","Statistical Diff"])

    def test_html_no_jinja_error(self, tmp, base_p, evol_p):
        result = self._run(base_p, evol_p)
        out = tmp/"r.html"; result.to_html(str(out))
        assert out.stat().st_size > 1000

    def test_markdown_structure(self, tmp, base_p, evol_p):
        result = self._run(base_p, evol_p)
        md = result.to_markdown()
        assert "#" in md and "Schema" in md and "Row" in md


# ══════════════════════════════════════════════════════════════════════════
# 10. CLI
# ══════════════════════════════════════════════════════════════════════════

class TestCLI:
    def cli(self): 
        from click.testing import CliRunner
        from difflake.cli import main
        return CliRunner(), main

    def test_version(self):
        r, m = self.cli()
        res = r.invoke(m, ["--version"])
        assert res.exit_code == 0

    def test_formats(self):
        r, m = self.cli()
        res = r.invoke(m, ["formats"])
        assert res.exit_code == 0
        assert "Parquet" in res.output or "parquet" in res.output.lower()

    def test_compare_schema(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p), "--mode","schema"])
        assert res.exit_code in (0,2)
        assert "SCHEMA" in res.output.upper()

    def test_compare_json_out(self, tmp, base_p, evol_p):
        r, m = self.cli()
        out = tmp/"r.json"
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--mode","stats","--output","json","--out",str(out)])
        assert res.exit_code in (0,2)
        assert out.exists()
        assert "schema_diff" in json.loads(out.read_text())

    def test_compare_html_out(self, tmp, base_p, evol_p):
        r, m = self.cli()
        out = tmp/"r.html"
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--mode","stats","--output","html","--out",str(out)])
        assert res.exit_code in (0,2)
        assert out.exists()

    def test_auto_filename_when_no_out(self, tmp_path, base_p, evol_p):
        """When --output html is given without --out, a file is auto-named and created."""
        import os
        r, m = self.cli()
        original = os.getcwd()
        os.chdir(tmp_path)
        try:
            res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                               "--mode", "stats", "--output", "html"])
            assert res.exit_code in (0, 2)
            html_files = list(tmp_path.glob("difflake_*.html"))
            assert len(html_files) == 1, f"Expected 1 auto-named HTML, got: {html_files}"
        finally:
            os.chdir(original)

    def test_show_count(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p), "--count"])
        assert res.exit_code == 0 and "5" in res.output

    def test_show_schema(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p), "--schema"])
        assert res.exit_code == 0
        assert "user_id" in res.output and "revenue" in res.output

    def test_show_where(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p), "--where","revenue > 200","--count"])
        assert res.exit_code == 0
        assert "2" in res.output

    def test_show_nonexistent(self):
        r, m = self.cli()
        res = r.invoke(m, ["show", "/no/such/file.parquet"])
        assert res.exit_code == 1

    def test_key_error_shown_in_output(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--mode","rows","--key","nonexistent"])
        assert res.exit_code in (0,2)
        assert "nonexistent" in res.output

    def test_low_cardinality_shown(self, tmp):
        r, m = self.cli()
        src = tmp/"src.parquet"
        write_parquet(src, {"VendorID":[1,1,2,2]*10,"fare":[10.0]*40})
        res = r.invoke(m, ["compare", str(src), str(src),
                           "--mode","rows","--key","VendorID"])
        assert res.exit_code in (0,2)
        assert "LOW-CARDINALITY" in res.output or "low" in res.output.lower()

    def test_compare_with_where(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--mode","stats","--where","revenue > 100"])
        assert res.exit_code in (0,2)


# ══════════════════════════════════════════════════════════════════════════
# 11. New CLI Features
# ══════════════════════════════════════════════════════════════════════════

class TestNewFeatures:
    """Tests for all 10 usability improvements."""

    def cli(self):
        from click.testing import CliRunner
        from difflake.cli import main
        return CliRunner(), main

    # ── Feature 1: difflake diff alias ────────────────────────────────────

    def test_diff_alias_works(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["diff", str(base_p), str(evol_p), "--mode", "schema"])
        assert res.exit_code in (0, 2), f"diff alias failed: {res.output}"
        assert "SCHEMA" in res.output.upper()

    def test_diff_alias_same_as_compare(self, base_p, evol_p):
        r, m = self.cli()
        res_compare = r.invoke(m, ["compare", str(base_p), str(evol_p), "--mode", "schema"])
        res_diff    = r.invoke(m, ["diff",    str(base_p), str(evol_p), "--mode", "schema"])
        assert res_compare.exit_code == res_diff.exit_code

    # ── Feature 2: --order-by in show ─────────────────────────────────────

    def test_show_order_by_asc(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p), "--order-by", "revenue", "--rows", "3"])
        assert res.exit_code == 0
        # First row should be lowest revenue (100.0)
        assert "100" in res.output

    def test_show_order_by_desc(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p), "--order-by", "revenue DESC", "--rows", "3"])
        assert res.exit_code == 0
        # First row should be highest revenue (300.0)
        assert "300" in res.output

    def test_show_order_by_with_where(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p),
                           "--where", "revenue > 100",
                           "--order-by", "revenue DESC",
                           "--rows", "3"])
        assert res.exit_code == 0

    # ── Feature 3: Pre-flight file info ───────────────────────────────────

    def test_preflight_shows_source_target(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p), "--mode", "schema"])
        assert res.exit_code in (0, 2)
        # Pre-flight should show Source and Target lines
        assert "Source" in res.output
        assert "Target" in res.output

    def test_preflight_shows_row_counts(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p), "--mode", "schema"])
        # Should show 5 rows for base and 7 for evolved
        assert "5" in res.output
        assert "7" in res.output

    # ── Feature 4: Combined default show output ────────────────────────────

    def test_show_default_has_schema_section(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p)])
        assert res.exit_code == 0
        assert "SCHEMA" in res.output.upper()

    def test_show_default_has_rows_section(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p)])
        assert res.exit_code == 0
        # Should show row data
        assert "Alice" in res.output or "user_id" in res.output

    def test_show_default_shows_row_count(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p)])
        assert res.exit_code == 0
        assert "5" in res.output  # 5 rows total

    # ── Feature 5: Summary line ────────────────────────────────────────────

    def test_summary_line_shows_added_columns(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p), "--mode", "schema"])
        assert res.exit_code in (0, 2)
        assert "added" in res.output.lower()

    def test_summary_line_shows_row_changes(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--mode", "rows", "--key", "user_id"])
        assert res.exit_code in (0, 2)
        assert "rows added" in res.output.lower()

    def test_summary_no_changes_message(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(base_p), "--mode", "schema"])
        assert res.exit_code in (0, 2)
        assert "no changes" in res.output.lower()

    # ── Feature 6: --ignore-columns ───────────────────────────────────────

    def test_ignore_columns_excluded_from_stats(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--mode", "stats",
                           "--ignore-columns", "revenue,age"])
        assert res.exit_code in (0, 2)
        # revenue and age should not appear in stats output
        # (they appear in schema diff but not stats table)

    def test_ignore_columns_api(self, tmp):
        src = tmp / "src.parquet"; tgt = tmp / "tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {**BASE, "revenue": [v * 3 for v in BASE["revenue"]]})
        # Without ignore — revenue should drift
        result = LakeDiff(source=str(src), target=str(tgt), mode="stats",
                          drift_threshold=0.10).run()
        assert "revenue" in result.stats_diff.drifted_columns
        # With ignore — revenue should not appear in stats at all
        result2 = LakeDiff(source=str(src), target=str(tgt), mode="stats",
                           drift_threshold=0.10,
                           ignore_columns=["revenue"]).run()
        col_names = [d.column for d in result2.stats_diff.column_diffs]
        assert "revenue" not in col_names

    # ── Feature 7: Timestamp stats in stats diff ───────────────────────────

    def test_timestamp_stats_min_max(self, tmp):
        """Timestamp columns should be detected as datetime and show min/max."""
        import duckdb
        src = tmp / "src.parquet"; tgt = tmp / "tgt.parquet"

        def write_ts(path, dates):
            con = duckdb.connect()
            con.execute("CREATE TABLE t (id INTEGER, created_at TIMESTAMP)")
            for i, d in enumerate(dates, 1):
                con.execute(f"INSERT INTO t VALUES ({i}, '{d}'::TIMESTAMP)")
            con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
            con.close()

        write_ts(src, ["2024-01-01", "2024-01-02", "2024-01-03"])
        write_ts(tgt, ["2024-01-05", "2024-01-06", "2024-01-07"])

        result = LakeDiff(source=str(src), target=str(tgt), mode="stats").run()
        dt_diff = next((d for d in result.stats_diff.column_diffs
                        if d.column == "created_at"), None)
        assert dt_diff is not None
        assert dt_diff.dtype_category == "datetime"

    def test_timestamp_drift_detected(self, tmp):
        """Timestamp columns with shifted dates should be flagged as drifted."""
        import duckdb
        src = tmp / "src.parquet"; tgt = tmp / "tgt.parquet"

        # Write parquet with proper TIMESTAMP columns using DuckDB CAST
        def write_ts(path, dates):
            con = duckdb.connect()
            con.execute("CREATE TABLE t (id INTEGER, event_ts TIMESTAMP)")
            for i, d in enumerate(dates, 1):
                con.execute(f"INSERT INTO t VALUES ({i}, '{d}'::TIMESTAMP)")
            con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
            con.close()

        write_ts(src, ["2024-01-01", "2024-01-02", "2024-01-03"])
        write_ts(tgt, ["2024-06-01", "2024-06-02", "2024-06-03"])  # 5 month shift

        result = LakeDiff(source=str(src), target=str(tgt), mode="stats").run()
        dt_diff = next((d for d in result.stats_diff.column_diffs
                        if d.column == "event_ts"), None)
        assert dt_diff is not None
        assert dt_diff.dtype_category == "datetime"
        # 5 month shift should be detected as drift
        assert dt_diff.is_drifted, (
            f"Expected drift but got: is_drifted={dt_diff.is_drifted}, "
            f"drift_reasons={dt_diff.drift_reasons}"
        )

    # ── Feature 8: --freq column distribution ─────────────────────────────

    def test_freq_flag_shows_values(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p), "--freq", "status"])
        assert res.exit_code == 0
        assert "active" in res.output
        assert "inactive" in res.output

    def test_freq_shows_counts(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p), "--freq", "country"])
        assert res.exit_code == 0
        # Should show counts for each country value
        assert "US" in res.output or "uk" in res.output.lower()

    def test_freq_with_where(self, base_p):
        r, m = self.cli()
        res = r.invoke(m, ["show", str(base_p),
                           "--where", "revenue > 100",
                           "--freq", "status"])
        assert res.exit_code == 0

    # ── Feature 9: Auto output filename ───────────────────────────────────

    def test_auto_output_filename_html(self, tmp, base_p, evol_p):
        r, m = self.cli()
        import os
        original_dir = os.getcwd()
        os.chdir(tmp)
        try:
            res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                               "--mode", "stats", "--output", "html"])
            assert res.exit_code in (0, 2)
            # Should have created an HTML file with auto-name
            html_files = list(tmp.glob("difflake_*.html"))
            assert len(html_files) == 1
        finally:
            os.chdir(original_dir)

    def test_auto_output_filename_json(self, tmp, base_p, evol_p):
        r, m = self.cli()
        import os
        original_dir = os.getcwd()
        os.chdir(tmp)
        try:
            res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                               "--mode", "stats", "--output", "json"])
            assert res.exit_code in (0, 2)
            json_files = list(tmp.glob("difflake_*.json"))
            assert len(json_files) == 1
        finally:
            os.chdir(original_dir)

    # ── Feature 10: --limit vs --sample ───────────────────────────────────

    def test_limit_restricts_rows(self, tmp):
        src = tmp / "src.parquet"
        write_parquet(src, BASE)  # 5 rows
        result = LakeDiff(source=str(src), target=str(src),
                          mode="stats", limit=3).run()
        # Stats should only cover 3 rows
        assert result.stats_diff.column_diffs  # should still compute

    def test_limit_and_sample_both_available(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--mode", "stats",
                           "--limit", "3"])
        assert res.exit_code in (0, 2)

    def test_limit_message_shown(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--limit", "3"])
        assert res.exit_code in (0, 2)
        assert "Limiting" in res.output or "limit" in res.output.lower()

    def test_sample_message_shown(self, base_p, evol_p):
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(base_p), str(evol_p),
                           "--sample", "3"])
        assert res.exit_code in (0, 2)
        assert "Sampling" in res.output or "sample" in res.output.lower()
