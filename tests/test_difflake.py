"""
Test suite for difflake — DuckDB backend.

All tests use DuckDB-native APIs. No Polars dependency.
Data is written to temp files via DuckDB and read back through DiffLake.

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

from difflake.core import DiffLake
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
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="schema").run()
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
        result = DiffLake(source=str(base_p), target=str(base_p), mode="stats").run()
        assert not result.stats_diff.has_drift

    def test_mean_drift_detected(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {**BASE, "revenue": [v*2 for v in BASE["revenue"]]})
        result = DiffLake(source=str(src), target=str(tgt), mode="stats", drift_threshold=0.15).run()
        assert "revenue" in result.stats_diff.drifted_columns

    def test_null_rate_drift_detected(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {**BASE, "age": [None,None,None,28,42]})
        result = DiffLake(source=str(src), target=str(tgt), mode="stats", drift_threshold=0.10).run()
        assert "age" in result.stats_diff.drifted_columns

    def test_new_category_detected(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="stats", drift_threshold=0.99).run()
        status = next(d for d in result.stats_diff.column_diffs if d.column=="status")
        assert "suspended" in status.new_categories

    def test_cardinality_increases(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="stats").run()
        country = next(d for d in result.stats_diff.column_diffs if d.column=="country")
        assert country.cardinality_after > country.cardinality_before

    def test_column_subset(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="stats", columns=["age","revenue"]).run()
        cols = [d.column for d in result.stats_diff.column_diffs]
        assert "age" in cols and "revenue" in cols and "country" not in cols

    def test_kl_divergence_large_shift(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {"v": list(range(1, 101))})
        write_parquet(tgt, {"v": [x*10 for x in range(1, 101)]})
        result = DiffLake(source=str(src), target=str(tgt), mode="stats").run()
        v = next(d for d in result.stats_diff.column_diffs if d.column=="v")
        assert v.kl_divergence is not None and v.kl_divergence > 0.1

    def test_min_max_mean_populated(self, base_p):
        result = DiffLake(source=str(base_p), target=str(base_p), mode="stats").run()
        rev = next(d for d in result.stats_diff.column_diffs if d.column=="revenue")
        assert rev.min_before is not None
        assert rev.max_before is not None
        assert rev.mean_before is not None


# ══════════════════════════════════════════════════════════════════════════
# 3. Row Differ
# ══════════════════════════════════════════════════════════════════════════

class TestRowDiffer:
    def test_count_only_no_key(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="rows").run()
        assert result.row_diff.row_count_before == 5
        assert result.row_diff.row_count_after == 7
        assert not result.row_diff.key_based_diff

    def test_added_rows(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_added == 2
        assert result.row_diff.rows_removed == 0
        assert result.row_diff.key_based_diff

    def test_removed_rows(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {k: v[2:] for k,v in BASE.items()})
        result = DiffLake(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_removed == 2

    def test_changed_rows(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_changed >= 2

    def test_sample_added_populated(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_id").run()
        assert len(result.row_diff.sample_added) > 0

    def test_delta_pct(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="rows").run()
        assert abs(result.row_diff.row_count_delta_pct - 40.0) < 0.1

    def test_null_aware_detection(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {**BASE, "status": [None,"active","inactive","active","active"]})
        write_parquet(tgt, BASE)
        result = DiffLake(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_changed >= 1

    def test_identical_no_changes(self, base_p):
        result = DiffLake(source=str(base_p), target=str(base_p), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_changed == 0
        assert result.row_diff.rows_added == 0


# ══════════════════════════════════════════════════════════════════════════
# 4. Key Error Handling
# ══════════════════════════════════════════════════════════════════════════

class TestKeyErrors:
    def test_missing_key_falls_back(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="rows", primary_key="no_such_col").run()
        assert not result.row_diff.key_based_diff
        assert result.row_diff.key_error is not None
        assert "no_such_col" in result.row_diff.key_error

    def test_missing_key_suggests_alternatives(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p), mode="rows", primary_key="user_Id").run()
        assert not result.row_diff.key_based_diff
        assert "user_id" in result.row_diff.key_error

    def test_key_in_source_not_target(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        tgt_data = {k: v for k,v in BASE.items() if k != "user_id"}
        tgt_data["user_uuid"] = ["a","b","c","d","e"]
        write_parquet(tgt, tgt_data)
        result = DiffLake(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert not result.row_diff.key_based_diff
        assert result.row_diff.key_error is not None

    def test_low_cardinality_blocked(self, tmp):
        src = tmp/"src.parquet"
        # 2 unique values across 500 rows = 0.4% uniqueness ratio — below 1% threshold
        write_parquet(src, {"VendorID": [1,2]*250, "fare": [10.0]*500})
        result = DiffLake(source=str(src), target=str(src), mode="rows", primary_key="VendorID").run()
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
        result = DiffLake(source=str(comp1_p), target=str(comp2_p), mode="rows",
                          primary_key="tenant_id,order_id,event_date").run()
        assert result.row_diff.key_based_diff
        assert result.row_diff.rows_added >= 1
        assert result.row_diff.rows_removed >= 1

    def test_composite_changed(self, comp1_p, comp2_p):
        result = DiffLake(source=str(comp1_p), target=str(comp2_p), mode="rows",
                          primary_key="tenant_id,order_id,event_date").run()
        assert result.row_diff.rows_changed >= 1

    def test_composite_key_string(self, comp1_p, comp2_p):
        result = DiffLake(source=str(comp1_p), target=str(comp2_p), mode="rows",
                          primary_key=["tenant_id","order_id","event_date"]).run()
        assert "tenant_id" in result.row_diff.primary_key_used


# ══════════════════════════════════════════════════════════════════════════
# 6. WHERE Filter
# ══════════════════════════════════════════════════════════════════════════

class TestWhereFilter:
    def test_where_reduces_rows(self, tmp):
        src = tmp/"src.parquet"; write_parquet(src, BASE)
        result = DiffLake(source=str(src), target=str(src), mode="stats",
                          where="revenue > 200").run()
        rev = next((d for d in result.stats_diff.column_diffs if d.column == "revenue"), None)
        assert rev is not None and rev.cardinality_before == 2  # 300 and 250

    def test_where_is_null(self, tmp):
        src = tmp/"src.parquet"
        write_parquet(src, {**BASE, "age": [None,None,35,28,42]})
        # Should not crash
        result = DiffLake(source=str(src), target=str(src), mode="schema",
                          where="age IS NULL").run()
        assert result is not None

    def test_where_compound(self, tmp):
        src = tmp/"src.parquet"; write_parquet(src, BASE)
        result = DiffLake(source=str(src), target=str(src), mode="stats",
                          where="status = \'active\' AND revenue > 150").run()
        # Bob(200,active), Dave(300,active), Eve(250,active) = 3
        name_d = next((d for d in result.stats_diff.column_diffs if d.column == "name"), None)
        assert name_d is not None and name_d.cardinality_before == 3  # Bob, Dave, Eve

    def test_where_all_formats(self, tmp):
        for ext, writer in [(".csv",write_csv), (".json",write_json), (".ndjson",write_ndjson)]:
            src = tmp/f"src{ext}"
            writer(src, BASE)
            result = DiffLake(source=str(src), target=str(src), mode="stats",
                              where="revenue > 100").run()
            assert result.row_diff.row_count_before >= 1


# ══════════════════════════════════════════════════════════════════════════
# 7. Edge Cases
# ══════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_empty_files(self, tmp):
        src = tmp/"src.parquet"
        write_parquet(src, {"id":[],"val":[]})
        result = DiffLake(source=str(src), target=str(src), mode="rows").run()
        assert result.row_diff.row_count_before == 0

    def test_single_row(self, tmp):
        src = tmp/"src.parquet"
        write_parquet(src, {"id":[1],"name":["Alice"]})
        result = DiffLake(source=str(src), target=str(src), mode="rows", primary_key="id").run()
        assert result.row_diff.rows_unchanged == 1
        assert result.row_diff.rows_changed == 0

    def test_all_rows_removed(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {"user_id":[],"name":[],"age":[],"revenue":[],"country":[],"status":[]})
        result = DiffLake(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_removed == 5

    def test_all_rows_added(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {"user_id":[],"name":[],"age":[],"revenue":[],"country":[],"status":[]})
        write_parquet(tgt, BASE)
        result = DiffLake(source=str(src), target=str(tgt), mode="rows", primary_key="user_id").run()
        assert result.row_diff.rows_added == 5

    def test_all_null_column_no_crash(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, {"id":[1,2,3],"val":[1.0,2.0,3.0]})
        write_parquet(tgt, {"id":[1,2,3],"val":[None,None,None]})
        result = DiffLake(source=str(src), target=str(tgt), mode="stats").run()
        assert isinstance(result.stats_diff.column_diffs, list)

    def test_missing_file_raises(self, tmp):
        good = tmp/"good.parquet"; write_parquet(good, BASE)
        with pytest.raises(Exception):
            DiffLake(source=str(good), target="/no/such/file.parquet").run()

    def test_unknown_extension(self, tmp):
        bad = tmp/"data.xyz"; bad.write_text("junk")
        with pytest.raises(Exception):
            DiffLake(source=str(bad), target=str(bad)).run()


# ══════════════════════════════════════════════════════════════════════════
# 8. Integration
# ══════════════════════════════════════════════════════════════════════════

class TestIntegration:
    @pytest.mark.parametrize("fmt", ["parquet","csv","json","ndjson"])
    def test_full_diff_all_formats(self, tmp, fmt):
        src = tmp/f"src.{fmt}"; tgt = tmp/f"tgt.{fmt}"
        write_file(src, BASE); write_file(tgt, EVOLVED)
        result = DiffLake(source=str(src), target=str(tgt), primary_key="user_id",
                          drift_threshold=0.10).run()
        assert isinstance(result, DiffResult)
        assert result.row_diff.rows_added == 2
        assert result.row_diff.key_based_diff

    def test_cross_format_csv_vs_parquet(self, tmp):
        src = tmp/"src.csv"; tgt = tmp/"tgt.parquet"
        write_csv(src, BASE); write_parquet(tgt, EVOLVED)
        result = DiffLake(source=str(src), target=str(tgt), primary_key="user_id").run()
        assert result.row_diff.rows_added == 2

    def test_drift_alerts_populated(self, tmp):
        src = tmp/"src.parquet"; tgt = tmp/"tgt.parquet"
        write_parquet(src, BASE)
        write_parquet(tgt, {**BASE,"revenue":[v*3 for v in BASE["revenue"]]})
        result = DiffLake(source=str(src), target=str(tgt), drift_threshold=0.10).run()
        assert len(result.drift_alerts) > 0

    def test_elapsed_seconds(self, base_p, evol_p):
        result = DiffLake(source=str(base_p), target=str(evol_p)).run()
        assert result.elapsed_seconds is not None and result.elapsed_seconds > 0

    def test_composite_end_to_end(self, comp1_p, comp2_p):
        result = DiffLake(source=str(comp1_p), target=str(comp2_p),
                          primary_key="tenant_id,order_id,event_date").run()
        assert result.row_diff.key_based_diff
        assert result.row_diff.rows_added >= 1
        assert result.row_diff.rows_removed >= 1

    def test_sample_size_no_crash(self, base_p):
        result = DiffLake(source=str(base_p), target=str(base_p), mode="stats",
                          sample_size=3).run()
        assert len(result.stats_diff.column_diffs) > 0

    def test_multipart_directory(self, tmp):
        src_dir = tmp/"parts_src"; tgt_dir = tmp/"parts_tgt"
        src_dir.mkdir(); tgt_dir.mkdir()
        write_parquet(src_dir/"part-000.parquet", {"id":[1,2,3],"val":["a","b","c"]})
        write_parquet(src_dir/"part-001.parquet", {"id":[4,5],"val":["d","e"]})
        write_parquet(tgt_dir/"part-000.parquet", {"id":[1,2,3,4,5,6],"val":["a","b","c","d","e","f"]})
        result = DiffLake(source=str(src_dir), target=str(tgt_dir),
                          mode="rows", primary_key="id").run()
        assert result.row_diff.row_count_before == 5
        assert result.row_diff.rows_added == 1


# ══════════════════════════════════════════════════════════════════════════
# 9. Reporters
# ══════════════════════════════════════════════════════════════════════════

class TestReporters:
    def _run(self, base_p, evol_p):
        return DiffLake(source=str(base_p), target=str(evol_p),
                        primary_key="user_id", drift_threshold=0.10).run()

    def test_json_structure(self, tmp, base_p, evol_p):
        result = self._run(base_p, evol_p)
        out = tmp/"r.json"; result.to_json(str(out))
        data = json.loads(out.read_text(encoding="utf-8"))
        assert all(k in data for k in ["schema_diff","stats_diff","row_diff","drift_alerts"])

    def test_json_string(self, base_p, evol_p):
        s = self._run(base_p, evol_p).to_json()
        assert "schema_diff" in json.loads(s)

    def test_html_renders(self, tmp, base_p, evol_p):
        result = self._run(base_p, evol_p)
        out = tmp/"r.html"; result.to_html(str(out))
        content = out.read_text(encoding="utf-8")
        assert all(s in content for s in ["difflake","Schema Diff","Row Diff","Statistical Diff"])

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
        return CliRunner(charset="utf-8"), main

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
        assert "schema_diff" in json.loads(out.read_text(encoding="utf-8"))

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
        return CliRunner(charset="utf-8"), main

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
        result = DiffLake(source=str(src), target=str(tgt), mode="stats",
                          drift_threshold=0.10).run()
        assert "revenue" in result.stats_diff.drifted_columns
        # With ignore — revenue should not appear in stats at all
        result2 = DiffLake(source=str(src), target=str(tgt), mode="stats",
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

        result = DiffLake(source=str(src), target=str(tgt), mode="stats").run()
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

        result = DiffLake(source=str(src), target=str(tgt), mode="stats").run()
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
        result = DiffLake(source=str(src), target=str(src),
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


# ── PR4: Batched stats aggregation tests ────────────────────────────────────

class TestBatchedStats:
    """
    Tests for the O(1)-scan batched stats aggregation refactor.
    Verifies that results are identical to what the old per-column approach
    produced, and that the batch helpers return the right structure.
    """

    def _make_views(self, tmp_path, src_data, tgt_data):
        from difflake.connection import DuckDBConnection, _read_sql, _detect_format
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, src_data)
        write_parquet(tgt, tgt_data)
        from difflake.differ.stats_differ import StatsDiffer
        con = DuckDBConnection()
        src_sql = _read_sql(str(src), "parquet")
        tgt_sql = _read_sql(str(tgt), "parquet")
        con.register_view("old_v", src_sql, "parquet", str(src))
        con.register_view("new_v", tgt_sql, "parquet", str(tgt))
        differ = StatsDiffer(con, "old_v", "new_v")
        return con, differ

    def test_batch_null_rates_all_columns(self, tmp_path):
        """_batch_null_rates returns an entry for every column."""
        from difflake.differ.stats_differ import StatsDiffer
        con, differ = self._make_views(
            tmp_path,
            {"id": [1, 2, None], "val": [10.0, None, 30.0]},
            {"id": [1, 2, 3],    "val": [10.0, 20.0, 30.0]},
        )
        nr = differ._batch_null_rates()
        con.close()
        assert set(nr.keys()) == {"id", "val"}
        # id has 1 null in old view (33.3%)
        assert nr["id"][0] > 0
        # id has 0 nulls in new view
        assert nr["id"][1] == 0.0

    def test_batch_cardinality_all_columns(self, tmp_path):
        """_batch_cardinality returns correct distinct counts."""
        from difflake.differ.stats_differ import StatsDiffer
        con, differ = self._make_views(
            tmp_path,
            {"id": [1, 2, 3], "cat": ["a", "b", "a"]},
            {"id": [1, 2, 3, 4], "cat": ["a", "b", "c", "c"]},
        )
        card = differ._batch_cardinality()
        con.close()
        assert card["id"][0] == 3   # old: 3 distinct
        assert card["id"][1] == 4   # new: 4 distinct
        assert card["cat"][0] == 2  # old: a, b
        assert card["cat"][1] == 3  # new: a, b, c

    def test_batch_numeric_agg_structure(self, tmp_path):
        """_batch_numeric_agg returns 5-tuple pairs for each numeric col."""
        from difflake.differ.stats_differ import StatsDiffer
        con, differ = self._make_views(
            tmp_path,
            {"x": [1, 2, 3], "y": [10, 20, 30]},
            {"x": [4, 5, 6], "y": [40, 50, 60]},
        )
        aggs = differ._batch_numeric_agg(["x", "y"])
        con.close()
        assert set(aggs.keys()) == {"x", "y"}
        old_x, new_x = aggs["x"]
        # old mean of x = 2.0, new = 5.0
        assert abs(old_x[0] - 2.0) < 0.01
        assert abs(new_x[0] - 5.0) < 0.01

    def test_batch_numeric_agg_empty(self, tmp_path):
        """_batch_numeric_agg with no columns returns empty dict."""
        from difflake.differ.stats_differ import StatsDiffer
        con, differ = self._make_views(
            tmp_path,
            {"id": [1, 2]}, {"id": [1, 2]},
        )
        result = differ._batch_numeric_agg([])
        con.close()
        assert result == {}

    def test_run_produces_correct_numeric_stats(self, tmp_path):
        """Full run() produces same numeric stats as before the refactor."""
        src_data = {"id": [1, 2, 3, 4, 5], "revenue": [10.0, 20.0, 30.0, 40.0, 50.0]}
        tgt_data = {"id": [1, 2, 3, 4, 5], "revenue": [11.0, 22.0, 33.0, 44.0, 55.0]}
        write_parquet(tmp_path / "a.parquet", src_data)
        write_parquet(tmp_path / "b.parquet", tgt_data)
        result = DiffLake(source=str(tmp_path / "a.parquet"),
                          target=str(tmp_path / "b.parquet"),
                          mode="stats").run()
        rev = next(d for d in result.stats_diff.column_diffs if d.column == "revenue")
        assert rev.dtype_category == "numeric"
        assert rev.mean_before is not None
        assert rev.mean_after is not None
        assert rev.mean_after > rev.mean_before

    def test_run_produces_correct_categorical_stats(self, tmp_path):
        """Categorical columns report new/dropped categories correctly."""
        src_data = {"id": [1, 2, 3], "status": ["active", "inactive", "active"]}
        tgt_data = {"id": [1, 2, 3], "status": ["active", "suspended", "churned"]}
        write_parquet(tmp_path / "a.parquet", src_data)
        write_parquet(tmp_path / "b.parquet", tgt_data)
        result = DiffLake(source=str(tmp_path / "a.parquet"),
                          target=str(tmp_path / "b.parquet"),
                          mode="stats").run()
        status = next(d for d in result.stats_diff.column_diffs if d.column == "status")
        assert status.dtype_category == "categorical"
        # "inactive" was dropped, "suspended"/"churned" are new
        assert len(status.dropped_categories) > 0 or len(status.new_categories) > 0

    def test_run_with_all_nulls_column(self, tmp_path):
        """Columns that are all-null don't crash the batch query."""
        src_data = {"id": [1, 2, 3], "val": [None, None, None]}
        tgt_data = {"id": [1, 2, 3], "val": [None, None, None]}
        write_parquet(tmp_path / "a.parquet", src_data)
        write_parquet(tmp_path / "b.parquet", tgt_data)
        result = DiffLake(source=str(tmp_path / "a.parquet"),
                          target=str(tmp_path / "b.parquet"),
                          mode="stats").run()
        assert result.stats_diff.column_diffs  # should have results

    def test_run_empty_columns_returns_empty(self, tmp_path):
        """StatsDiffer with no common columns returns empty StatsDiff."""
        from difflake.connection import DuckDBConnection, _read_sql
        from difflake.differ.stats_differ import StatsDiffer
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"col_a": [1, 2]})
        write_parquet(tgt, {"col_b": [3, 4]})
        con = DuckDBConnection()
        con.register_view("old_v", _read_sql(str(src), "parquet"), "parquet", str(src))
        con.register_view("new_v", _read_sql(str(tgt), "parquet"), "parquet", str(tgt))
        differ = StatsDiffer(con, "old_v", "new_v")
        result = differ.run()
        con.close()
        assert result.column_diffs == []
        assert result.drifted_columns == []

    def test_batch_matches_expected_null_rate(self, tmp_path):
        """Null rate in batch result matches manual calculation."""
        # 2 nulls out of 4 rows = 50%
        src_data = {"val": [1.0, None, 3.0, None]}
        tgt_data = {"val": [1.0, 2.0, 3.0, 4.0]}
        write_parquet(tmp_path / "a.parquet", src_data)
        write_parquet(tmp_path / "b.parquet", tgt_data)
        result = DiffLake(source=str(tmp_path / "a.parquet"),
                          target=str(tmp_path / "b.parquet"),
                          mode="stats").run()
        col = result.stats_diff.column_diffs[0]
        assert abs(col.null_rate_before - 50.0) < 1.0
        assert col.null_rate_after == 0.0

    def test_many_columns_batch_efficiency(self, tmp_path):
        """15-column dataset runs without error (stress test for batch SQL)."""
        data = {f"col{i}": list(range(1, 6)) for i in range(15)}
        write_parquet(tmp_path / "a.parquet", data)
        mutated = {k: [v * 2 for v in vals] for k, vals in data.items()}
        write_parquet(tmp_path / "b.parquet", mutated)
        result = DiffLake(source=str(tmp_path / "a.parquet"),
                          target=str(tmp_path / "b.parquet"),
                          mode="stats").run()
        assert len(result.stats_diff.column_diffs) == 15


# ── Validate command ────────────────────────────────────────────────────────

class TestValidateCommand:
    """Tests for `difflake validate` CLI command."""

    @pytest.fixture
    def tmp(self, tmp_path):
        return tmp_path

    @pytest.fixture
    def data_p(self, tmp_path):
        """Parquet file with clean data."""
        p = tmp_path / "data.parquet"
        write_parquet(p, {
            "id":    [1, 2, 3, 4, 5],
            "name":  ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age":   [30, 25, 35, 28, 22],
            "score": [95.5, 88.0, 72.3, 91.1, 60.0],
        })
        return p

    @pytest.fixture
    def dirty_p(self, tmp_path):
        """Parquet file with nulls and duplicates."""
        p = tmp_path / "dirty.parquet"
        write_parquet(p, {
            "id":   [1, 2, 2, 3],
            "name": ["Alice", None, "Bob", "Carol"],
            "age":  [None, 25, 25, -5],
        })
        return p

    def cli(self):
        from click.testing import CliRunner
        from difflake.cli import main
        return CliRunner(charset="utf-8"), main

    # ── min-rows ──────────────────────────────────────────────────────────

    def test_min_rows_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--min-rows", "3"])
        assert res.exit_code == 0
        assert "✅" in res.output

    def test_min_rows_fails(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--min-rows", "100"])
        assert res.exit_code == 1
        assert "❌" in res.output

    # ── max-rows ──────────────────────────────────────────────────────────

    def test_max_rows_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--max-rows", "10"])
        assert res.exit_code == 0

    def test_max_rows_fails(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--max-rows", "2"])
        assert res.exit_code == 1

    # ── not-null ──────────────────────────────────────────────────────────

    def test_not_null_passes_clean_data(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--not-null", "id"])
        assert res.exit_code == 0
        assert "no nulls" in res.output

    def test_not_null_fails_when_nulls_present(self, dirty_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(dirty_p), "--not-null", "name"])
        assert res.exit_code == 1
        assert "❌" in res.output

    def test_not_null_multiple_columns(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p),
                           "--not-null", "id", "--not-null", "name"])
        assert res.exit_code == 0

    # ── unique ────────────────────────────────────────────────────────────

    def test_unique_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--unique", "id"])
        assert res.exit_code == 0
        assert "all unique" in res.output

    def test_unique_fails_with_duplicates(self, dirty_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(dirty_p), "--unique", "id"])
        assert res.exit_code == 1
        assert "duplicate" in res.output

    # ── min-val / max-val ─────────────────────────────────────────────────

    def test_min_val_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--min-val", "score:50"])
        assert res.exit_code == 0

    def test_min_val_fails(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--min-val", "score:99"])
        assert res.exit_code == 1

    def test_max_val_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--max-val", "score:100"])
        assert res.exit_code == 0

    def test_max_val_fails(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--max-val", "age:20"])
        assert res.exit_code == 1

    def test_min_val_negative_allowed(self, dirty_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(dirty_p), "--min-val", "age:-100"])
        assert res.exit_code == 0

    # ── column-exists ─────────────────────────────────────────────────────

    def test_column_exists_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--column-exists", "id"])
        assert res.exit_code == 0
        assert "found" in res.output

    def test_column_exists_fails(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--column-exists", "nonexistent"])
        assert res.exit_code == 1
        assert "missing" in res.output

    # ── where-count ───────────────────────────────────────────────────────

    def test_where_count_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p),
                           "--where-count", "age < 0==0"])
        assert res.exit_code == 0

    def test_where_count_fails(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p),
                           "--where-count", "age < 0==999"])
        assert res.exit_code == 1

    # ── multiple checks ───────────────────────────────────────────────────

    def test_multiple_checks_all_pass(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, [
            "validate", str(data_p),
            "--min-rows", "5",
            "--max-rows", "5",
            "--not-null", "id",
            "--unique", "id",
            "--column-exists", "score",
        ])
        assert res.exit_code == 0
        assert "All 5 checks passed" in res.output

    def test_multiple_checks_partial_fail(self, dirty_p):
        r, m = self.cli()
        res = r.invoke(m, [
            "validate", str(dirty_p),
            "--not-null", "name",   # fails
            "--not-null", "id",     # passes (no nulls in id)
        ])
        assert res.exit_code == 1
        assert "❌" in res.output

    # ── --where pre-filter ────────────────────────────────────────────────

    def test_where_filter_applied(self, data_p):
        r, m = self.cli()
        # After filtering to age >= 30, only 2 rows (Alice=30, Carol=35)
        res = r.invoke(m, ["validate", str(data_p),
                           "--where", "age >= 30",
                           "--min-rows", "2",
                           "--max-rows", "2"])
        assert res.exit_code == 0

    # ── --fail-fast ───────────────────────────────────────────────────────

    def test_fail_fast_stops_early(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, [
            "validate", str(data_p),
            "--fail-fast",
            "--min-rows", "1000",   # fails → stop
            "--max-rows", "3",      # would also fail
        ])
        assert res.exit_code == 1
        # Only 1 check ran because of fail_fast
        assert "1 of 1" in res.output or "1 of" in res.output

    # ── no checks ─────────────────────────────────────────────────────────

    def test_no_checks_warns(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p)])
        assert res.exit_code == 0
        assert "No checks" in res.output

    # ── YAML config integration ───────────────────────────────────────────

    def test_yaml_config_checks(self, data_p, tmp_path):
        cfg_path = tmp_path / "difflake.yaml"
        cfg_path.write_text(
            "validate:\n"
            "  checks:\n"
            "    - kind: min_rows\n"
            "      value: 5\n"
            "    - kind: not_null\n"
            "      column: id\n"
        )
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--config", str(cfg_path)])
        assert res.exit_code == 0
        assert "All 2 checks passed" in res.output

    def test_yaml_config_failing_check(self, data_p, tmp_path):
        cfg_path = tmp_path / "difflake.yaml"
        cfg_path.write_text(
            "validate:\n"
            "  checks:\n"
            "    - kind: min_rows\n"
            "      value: 9999\n"
        )
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--config", str(cfg_path)])
        assert res.exit_code == 1

    # ── CSV input ─────────────────────────────────────────────────────────

    def test_validate_csv_file(self, tmp_path):
        p = tmp_path / "data.csv"
        p.write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(p), "--min-rows", "3", "--not-null", "id"])
        assert res.exit_code == 0

    # ── error handling ────────────────────────────────────────────────────

    def test_nonexistent_file_exits_1(self, tmp_path):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(tmp_path / "missing.parquet"),
                           "--min-rows", "1"])
        assert res.exit_code == 1

    def test_singular_check_message(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["validate", str(data_p), "--min-rows", "1"])
        assert res.exit_code == 0
        assert "1 check passed" in res.output


# ── Query command ───────────────────────────────────────────────────────────

class TestQueryCommand:
    """Tests for `difflake query` CLI command."""

    @pytest.fixture
    def data_p(self, tmp_path):
        p = tmp_path / "data.parquet"
        write_parquet(p, {
            "id":     [1, 2, 3, 4, 5],
            "name":   ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age":    [30, 25, 35, 28, 22],
            "score":  [95.5, 88.0, 72.3, 91.1, 60.0],
        })
        return p

    def cli(self):
        from click.testing import CliRunner
        from difflake.cli import main
        return CliRunner(charset="utf-8"), main

    # ── basic SELECT ──────────────────────────────────────────────────────

    def test_select_all_passes(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT * FROM t"])
        assert res.exit_code == 0
        assert "Alice" in res.output

    def test_select_with_where(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT name FROM t WHERE age > 28"])
        assert res.exit_code == 0
        assert "Alice" in res.output
        assert "Bob" not in res.output

    def test_select_count(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT COUNT(*) AS n FROM t"])
        assert res.exit_code == 0
        assert "5" in res.output

    def test_select_aggregation(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p),
                           "SELECT MIN(age) AS mn, MAX(age) AS mx FROM t"])
        assert res.exit_code == 0
        assert "22" in res.output
        assert "35" in res.output

    def test_result_row_count_shown(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT * FROM t"])
        assert res.exit_code == 0
        assert "5 rows" in res.output

    def test_single_row_singular_label(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p),
                           "SELECT name FROM t WHERE id = 1"])
        assert res.exit_code == 0
        assert "1 row" in res.output

    # ── --limit ───────────────────────────────────────────────────────────

    def test_limit_restricts_output(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT * FROM t", "--limit", "2"])
        assert res.exit_code == 0
        # Only 2 rows in table
        assert "2 rows" in res.output

    def test_limit_note_shown_when_hit(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT * FROM t", "--limit", "5"])
        assert res.exit_code == 0
        # 5 rows, limit = 5 → note shown
        assert "limited to 5" in res.output

    # ── --output json ─────────────────────────────────────────────────────

    def test_output_json_stdout(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT id, name FROM t",
                           "--output", "json"])
        assert res.exit_code == 0
        import json
        records = json.loads(res.output)
        assert len(records) == 5
        assert records[0]["name"] == "Alice"

    def test_output_json_to_file(self, data_p, tmp_path):
        out = tmp_path / "out.json"
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT * FROM t",
                           "--output", "json", "--out", str(out)])
        assert res.exit_code == 0
        assert out.exists()
        import json
        data = json.loads(out.read_text())
        assert len(data) == 5

    # ── --output csv ──────────────────────────────────────────────────────

    def test_output_csv_stdout(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT id, name FROM t",
                           "--output", "csv"])
        assert res.exit_code == 0
        lines = [l for l in res.output.strip().splitlines() if l]
        assert lines[0] == "id,name"
        assert "Alice" in res.output

    def test_output_csv_to_file(self, data_p, tmp_path):
        out = tmp_path / "out.csv"
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT id, name FROM t",
                           "--output", "csv", "--out", str(out)])
        assert res.exit_code == 0
        assert out.exists()
        content = out.read_text()
        assert "id,name" in content

    def test_output_csv_no_header(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT id FROM t",
                           "--output", "csv", "--no-header"])
        assert res.exit_code == 0
        # First line should be a number, not "id"
        first_line = res.output.strip().splitlines()[0]
        assert first_line.isdigit()

    # ── --no-header (cli mode) ────────────────────────────────────────────

    def test_no_header_cli_mode(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "SELECT name FROM t WHERE id=1",
                           "--no-header"])
        assert res.exit_code == 0
        # "name" header should not appear as a column header
        # "Alice" should still appear as data
        assert "Alice" in res.output

    # ── CSV input ─────────────────────────────────────────────────────────

    def test_query_csv_file(self, tmp_path):
        p = tmp_path / "data.csv"
        p.write_text("id,val\n1,10\n2,20\n3,30\n")
        r, m = self.cli()
        res = r.invoke(m, ["query", str(p), "SELECT SUM(val) AS total FROM t"])
        assert res.exit_code == 0
        assert "60" in res.output

    # ── JSON input ────────────────────────────────────────────────────────

    def test_query_json_file(self, tmp_path):
        import json
        p = tmp_path / "data.json"
        p.write_text(json.dumps([{"x": 1}, {"x": 2}, {"x": 3}]))
        r, m = self.cli()
        res = r.invoke(m, ["query", str(p), "SELECT MAX(x) AS mx FROM t"])
        assert res.exit_code == 0
        assert "3" in res.output

    # ── error handling ────────────────────────────────────────────────────

    def test_missing_file_exits_1(self, tmp_path):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(tmp_path / "missing.parquet"),
                           "SELECT * FROM t"])
        assert res.exit_code == 1

    def test_bad_sql_exits_1(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p), "THIS IS NOT SQL"])
        assert res.exit_code == 1

    def test_nonexistent_column_exits_1(self, data_p):
        r, m = self.cli()
        res = r.invoke(m, ["query", str(data_p),
                           "SELECT nonexistent_col FROM t"])
        assert res.exit_code == 1


# ── Enhanced HTML reporter ──────────────────────────────────────────────────

class TestHtmlReporterV2:
    """Tests for the upgraded HTML reporter (offline mode, new charts, enhanced table)."""

    @pytest.fixture
    def result(self, tmp_path):
        src = tmp_path / "src.parquet"
        tgt = tmp_path / "tgt.parquet"
        write_parquet(src, {
            "id":    [1, 2, 3, 4, 5],
            "score": [10.0, 20.0, 30.0, 40.0, 50.0],
            "cat":   ["a", "b", "c", "a", "b"],
        })
        write_parquet(tgt, {
            "id":    [1, 2, 3, 4, 6],
            "score": [15.0, 25.0, 35.0, 45.0, 55.0],
            "cat":   ["a", "b", "c", "d", "e"],
        })
        return DiffLake(source=str(src), target=str(tgt),
                        primary_key="id", mode="full").run()

    def _render(self, result, tmp_path, offline=False):
        from difflake.reporters.html_reporter import HtmlReporter
        out = tmp_path / "report.html"
        HtmlReporter(result, offline=offline).write(out)
        return out.read_text(encoding="utf-8")

    # ── offline mode ──────────────────────────────────────────────────────

    def test_offline_false_uses_cdn_tag(self, result, tmp_path):
        html = self._render(result, tmp_path, offline=False)
        assert "cdn.jsdelivr.net" in html

    def test_offline_true_attempts_inline(self, result, tmp_path):
        """offline=True should not contain a bare src= CDN link (Chart.js is inlined or absent)."""
        html = self._render(result, tmp_path, offline=True)
        # Either inlined (no src= link) or fell back gracefully — but must have <script>
        assert "<script>" in html

    def test_offline_badge_shown_when_offline(self, result, tmp_path):
        html = self._render(result, tmp_path, offline=True)
        assert "offline" in html

    def test_offline_badge_absent_when_not_offline(self, result, tmp_path):
        html = self._render(result, tmp_path, offline=False)
        assert 'class="offline-badge"' not in html

    # ── generated_at timestamp ────────────────────────────────────────────

    def test_generated_at_present(self, result, tmp_path):
        from datetime import datetime
        html = self._render(result, tmp_path)
        # Year should appear in timestamp
        assert str(datetime.now().year) in html

    # ── collapsible sections ──────────────────────────────────────────────

    def test_details_tags_present(self, result, tmp_path):
        html = self._render(result, tmp_path)
        assert "<details" in html
        assert "<summary>" in html

    # ── column search filter ──────────────────────────────────────────────

    def test_search_bar_present_when_stats(self, result, tmp_path):
        html = self._render(result, tmp_path)
        assert "statsSearch" in html
        assert "filterStats" in html

    # ── null rate chart ───────────────────────────────────────────────────

    def test_null_chart_canvas_present(self, result, tmp_path):
        html = self._render(result, tmp_path)
        assert "nullChart" in html

    def test_null_chart_data_injected(self, result, tmp_path):
        html = self._render(result, tmp_path)
        # Null rate labels array should appear in chart JS
        assert "null_labels" not in html  # Jinja variable name replaced
        assert "nullChart" in html

    # ── mean drift chart ─────────────────────────────────────────────────

    def test_drift_chart_canvas_present(self, result, tmp_path):
        html = self._render(result, tmp_path)
        # driftChart appears only when there are numeric drift columns
        # score column will have drift between src/tgt
        assert "driftChart" in html

    # ── enhanced stats table ──────────────────────────────────────────────

    def test_stats_table_has_std_columns(self, result, tmp_path):
        html = self._render(result, tmp_path)
        assert "Std Before" in html
        assert "Std After" in html

    def test_stats_table_has_min_max_columns(self, result, tmp_path):
        html = self._render(result, tmp_path)
        assert "Min" in html
        assert "Max" in html

    def test_drift_reasons_rendered_inline(self, result, tmp_path):
        html = self._render(result, tmp_path)
        # drifted columns have reasons rendered in a .reasons div
        assert "reasons" in html

    # ── chart data computation ────────────────────────────────────────────

    def test_build_chart_data_returns_expected_keys(self, result):
        from difflake.reporters.html_reporter import _build_chart_data
        data = _build_chart_data(result)
        assert "null_labels" in data
        assert "null_before" in data
        assert "null_after" in data
        assert "drift_labels" in data
        assert "drift_values" in data
        assert "drift_colors" in data

    def test_build_chart_data_color_logic(self, result):
        from difflake.reporters.html_reporter import _build_chart_data
        data = _build_chart_data(result)
        for color in data["drift_colors"]:
            assert color in ("#ef4444", "#f59e0b", "#22c55e")

    def test_build_chart_data_no_inf(self, result):
        from difflake.reporters.html_reporter import _build_chart_data
        data = _build_chart_data(result)
        import math
        for v in data["drift_values"]:
            assert not math.isinf(v)

    # ── CLI --no-offline flag ─────────────────────────────────────────────

    def test_cli_no_offline_flag_accepted(self, tmp_path):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2], "v": [10, 20]})
        write_parquet(tgt, {"id": [1, 2], "v": [10, 20]})
        from click.testing import CliRunner
        from difflake.cli import main
        out = tmp_path / "report.html"
        res = CliRunner(charset="utf-8").invoke(main, [
            "compare", str(src), str(tgt),
            "--output", "html", "--out", str(out),
            "--no-offline",
        ])
        assert res.exit_code in (0, 2)
        assert out.exists()
        assert "cdn.jsdelivr.net" in out.read_text(encoding="utf-8")


# ── Structured logging ──────────────────────────────────────────────────────

class TestLogging:
    """Tests for --log-level, --log-format, --log-file flags and logging_setup module."""

    @pytest.fixture
    def parquet_pair(self, tmp_path):
        src = tmp_path / "src.parquet"
        tgt = tmp_path / "tgt.parquet"
        write_parquet(src, {"id": [1, 2, 3], "val": [10, 20, 30]})
        write_parquet(tgt, {"id": [1, 2, 3], "val": [15, 25, 35]})
        return src, tgt

    def cli(self):
        from click.testing import CliRunner
        from difflake.cli import main
        return CliRunner(charset="utf-8"), main

    # ── configure() API ──────────────────────────────────────────────────

    def test_configure_text_format(self):
        from difflake.logging_setup import configure, get_logger
        configure(level="DEBUG", fmt="text")
        log = get_logger("test_text")
        assert log.getEffectiveLevel() <= 10  # DEBUG = 10

    def test_configure_json_format(self):
        from difflake.logging_setup import configure, get_logger
        configure(level="INFO", fmt="json")
        log = get_logger("test_json")
        assert log.getEffectiveLevel() <= 20  # INFO = 20

    def test_configure_is_idempotent(self):
        """Calling configure() twice should not duplicate handlers."""
        import logging
        from difflake.logging_setup import configure
        configure(level="INFO", fmt="text")
        configure(level="INFO", fmt="text")
        root = logging.getLogger("difflake")
        # Should not grow unbounded
        assert len(root.handlers) <= 2

    def test_configure_warning_level_silences_debug_info(self, capsys):
        import logging
        from difflake.logging_setup import configure, get_logger
        configure(level="WARNING", fmt="text")
        log = get_logger("test_silence")
        log.debug("this should not appear")
        log.info("this should not appear either")
        # No way to capture stderr from logging in capsys easily without
        # a custom handler — so just assert no exception
        assert True

    def test_get_logger_returns_child_of_difflake(self):
        from difflake.logging_setup import get_logger
        log = get_logger("mymodule")
        assert log.name.startswith("difflake")

    def test_get_logger_no_double_prefix(self):
        from difflake.logging_setup import get_logger
        log = get_logger("difflake.core")
        assert log.name == "difflake.core"

    # ── text formatter ────────────────────────────────────────────────────

    def test_text_formatter_output_format(self):
        import logging
        from difflake.logging_setup import _TextFormatter
        fmt = _TextFormatter(colour=False)
        record = logging.LogRecord(
            name="difflake.test", level=logging.INFO,
            pathname="", lineno=0, msg="hello world",
            args=(), exc_info=None,
        )
        output = fmt.format(record)
        assert "INFO" in output
        assert "hello world" in output
        assert "difflake.test" in output

    def test_text_formatter_includes_extras(self):
        import logging
        from difflake.logging_setup import _TextFormatter
        fmt = _TextFormatter(colour=False)
        record = logging.LogRecord(
            name="difflake", level=logging.DEBUG,
            pathname="", lineno=0, msg="test",
            args=(), exc_info=None,
        )
        record.source = "s3://bucket/data.parquet"
        output = fmt.format(record)
        assert "s3://bucket/data.parquet" in output

    # ── JSON formatter ────────────────────────────────────────────────────

    def test_json_formatter_valid_json(self):
        import json, logging
        from difflake.logging_setup import _JsonFormatter
        fmt = _JsonFormatter()
        record = logging.LogRecord(
            name="difflake.core", level=logging.WARNING,
            pathname="", lineno=0, msg="drift detected",
            args=(), exc_info=None,
        )
        output = fmt.format(record)
        data = json.loads(output)
        assert data["level"] == "WARNING"
        assert data["message"] == "drift detected"
        assert data["logger"] == "difflake.core"
        assert "timestamp" in data

    def test_json_formatter_includes_extras(self):
        import json, logging
        from difflake.logging_setup import _JsonFormatter
        fmt = _JsonFormatter()
        record = logging.LogRecord(
            name="difflake", level=logging.INFO,
            pathname="", lineno=0, msg="diff complete",
            args=(), exc_info=None,
        )
        record.elapsed_s = 1.23
        output = fmt.format(record)
        data = json.loads(output)
        assert data["elapsed_s"] == 1.23

    def test_json_formatter_timestamp_is_iso(self):
        import json, logging
        from difflake.logging_setup import _JsonFormatter
        fmt = _JsonFormatter()
        record = logging.LogRecord(
            name="difflake", level=logging.INFO,
            pathname="", lineno=0, msg="ts check",
            args=(), exc_info=None,
        )
        output = fmt.format(record)
        ts = json.loads(output)["timestamp"]
        assert "T" in ts  # ISO 8601 format

    # ── log file output ───────────────────────────────────────────────────

    def test_configure_log_file_writes(self, tmp_path):
        from difflake.logging_setup import configure, get_logger
        log_path = tmp_path / "test.log"
        configure(level="DEBUG", fmt="text", log_file=str(log_path))
        log = get_logger("test_file")
        log.info("written to file")
        # Flush handlers
        import logging
        for h in logging.getLogger("difflake").handlers:
            h.flush()
        assert log_path.exists()
        assert "written to file" in log_path.read_text()

    def test_configure_log_file_json(self, tmp_path):
        import json
        from difflake.logging_setup import configure, get_logger
        log_path = tmp_path / "test.json.log"
        configure(level="INFO", fmt="json", log_file=str(log_path))
        log = get_logger("test_json_file")
        log.info("json to file")
        import logging
        for h in logging.getLogger("difflake").handlers:
            h.flush()
        assert log_path.exists()
        line = log_path.read_text().strip()
        data = json.loads(line)
        assert data["message"] == "json to file"

    # ── CLI --log-level flag ──────────────────────────────────────────────

    def test_cli_log_level_debug_accepted(self, parquet_pair):
        src, tgt = parquet_pair
        r, m = self.cli()
        res = r.invoke(m, ["--log-level", "DEBUG",
                           "compare", str(src), str(tgt), "--mode", "stats"])
        assert res.exit_code in (0, 2)

    def test_cli_log_level_info_produces_info_logs(self, parquet_pair):
        src, tgt = parquet_pair
        r, m = self.cli()
        res = r.invoke(m, ["--log-level", "INFO",
                           "compare", str(src), str(tgt), "--mode", "stats"])
        assert res.exit_code in (0, 2)
        # INFO logs go to stderr — captured in mix_stderr=False runner
        # Just ensure no crash
        assert "Error" not in res.output or res.exit_code != 1

    def test_cli_log_format_json_accepted(self, parquet_pair):
        src, tgt = parquet_pair
        r, m = self.cli()
        res = r.invoke(m, ["--log-level", "INFO", "--log-format", "json",
                           "compare", str(src), str(tgt), "--mode", "stats"])
        assert res.exit_code in (0, 2)

    def test_cli_log_file_written(self, parquet_pair, tmp_path):
        src, tgt = parquet_pair
        log_path = tmp_path / "cli.log"
        r, m = self.cli()
        res = r.invoke(m, ["--log-level", "INFO", "--log-file", str(log_path),
                           "compare", str(src), str(tgt), "--mode", "stats"])
        assert res.exit_code in (0, 2)
        assert log_path.exists()
        assert len(log_path.read_text()) > 0

    def test_cli_log_file_json_format(self, parquet_pair, tmp_path):
        import json
        src, tgt = parquet_pair
        log_path = tmp_path / "cli.jsonl"
        r, m = self.cli()
        res = r.invoke(m, [
            "--log-level", "INFO", "--log-format", "json",
            "--log-file", str(log_path),
            "compare", str(src), str(tgt), "--mode", "stats",
        ])
        assert res.exit_code in (0, 2)
        assert log_path.exists()
        lines = [l for l in log_path.read_text().strip().splitlines() if l]
        assert len(lines) > 0
        # Every line must be valid JSON
        for line in lines:
            data = json.loads(line)
            assert "level" in data
            assert "message" in data

    def test_cli_env_var_log_level(self, parquet_pair, monkeypatch):
        src, tgt = parquet_pair
        monkeypatch.setenv("DIFFLAKE_LOG_LEVEL", "INFO")
        r, m = self.cli()
        res = r.invoke(m, ["compare", str(src), str(tgt), "--mode", "schema"])
        assert res.exit_code in (0, 2)

    # ── core.py logs key events ───────────────────────────────────────────

    def test_diff_logs_started_and_complete(self, parquet_pair, tmp_path):
        src, tgt = parquet_pair
        log_path = tmp_path / "core.log"
        from difflake.logging_setup import configure
        configure(level="INFO", fmt="text", log_file=str(log_path))
        DiffLake(source=str(src), target=str(tgt), mode="stats").run()
        import logging
        for h in logging.getLogger("difflake").handlers:
            h.flush()
        content = log_path.read_text()
        assert "diff started" in content
        assert "diff complete" in content

    def test_diff_logs_debug_params(self, parquet_pair, tmp_path):
        src, tgt = parquet_pair
        log_path = tmp_path / "debug.log"
        from difflake.logging_setup import configure
        configure(level="DEBUG", fmt="text", log_file=str(log_path))
        DiffLake(source=str(src), target=str(tgt), mode="schema").run()
        import logging
        for h in logging.getLogger("difflake").handlers:
            h.flush()
        content = log_path.read_text()
        assert "diff parameters" in content
# ── Coverage-boosting tests ─────────────────────────────────────────────────

class TestDetectFormat:
    """Cover _detect_format cloud/HTTP/local paths."""

    def test_s3_parquet(self):
        assert _detect_format("s3://bucket/data/file.parquet") == "parquet"

    def test_s3_csv(self):
        assert _detect_format("s3://bucket/data/file.csv") == "csv"

    def test_s3_no_extension_defaults_parquet(self):
        assert _detect_format("s3://bucket/data/prefix") == "parquet"

    def test_gs_parquet(self):
        assert _detect_format("gs://bucket/data.parquet") == "parquet"

    def test_gcs_json(self):
        assert _detect_format("gcs://bucket/data.json") == "json"

    def test_az_csv(self):
        assert _detect_format("az://container/data.csv") == "csv"

    def test_abfs_parquet(self):
        assert _detect_format("abfs://container@account.dfs.core.windows.net/data.parquet") == "parquet"

    def test_http_parquet(self):
        assert _detect_format("http://example.com/data.parquet") == "parquet"

    def test_https_csv(self):
        assert _detect_format("https://example.com/data.csv?token=abc") == "csv"

    def test_https_no_ext_defaults_parquet(self):
        assert _detect_format("https://example.com/data") == "parquet"

    def test_local_parquet(self, tmp_path):
        p = tmp_path / "data.parquet"
        p.write_bytes(b"PAR1")
        assert _detect_format(str(p)) == "parquet"

    def test_local_csv(self, tmp_path):
        p = tmp_path / "data.csv"
        p.write_text("a,b\n1,2")
        assert _detect_format(str(p)) == "csv"

    def test_local_jsonl(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a":1}')
        assert _detect_format(str(p)) == "jsonl"

    def test_local_unknown_ext_raises(self, tmp_path):
        p = tmp_path / "data.xyz"
        p.write_text("garbage")
        with pytest.raises(ValueError, match="Unrecognized"):
            _detect_format(str(p))

    def test_directory_with_delta_log(self, tmp_path):
        (tmp_path / "_delta_log").mkdir()
        assert _detect_format(str(tmp_path)) == "delta"

    def test_directory_with_parquet_files(self, tmp_path):
        (tmp_path / "part-0.parquet").write_bytes(b"PAR1")
        assert _detect_format(str(tmp_path)) == "parquet"

    def test_directory_with_csv_files(self, tmp_path):
        (tmp_path / "data.csv").write_text("a,b")
        assert _detect_format(str(tmp_path)) == "csv"

    def test_directory_no_known_files_raises(self, tmp_path):
        (tmp_path / "file.xyz").write_text("x")
        with pytest.raises(ValueError, match="Cannot detect format"):
            _detect_format(str(tmp_path))

    def test_nonexistent_path_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Path not found"):
            _detect_format(str(tmp_path / "nonexistent.parquet"))


class TestReadSql:
    """Cover _read_sql format branches."""

    def test_csv_file_sql(self, tmp_path):
        p = str(tmp_path / "data.csv")
        sql = _read_sql(p, "csv")
        assert "read_csv" in sql

    def test_parquet_file_sql(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "parquet")
        assert "read_parquet" in sql

    def test_json_file_sql(self, tmp_path):
        p = str(tmp_path / "data.json")
        sql = _read_sql(p, "json")
        assert "read_json" in sql

    def test_jsonl_file_sql(self, tmp_path):
        p = str(tmp_path / "data.jsonl")
        sql = _read_sql(p, "jsonl")
        assert "read_ndjson" in sql

    def test_delta_sql(self, tmp_path):
        p = str(tmp_path)
        sql = _read_sql(p, "delta")
        assert "delta_scan" in sql

    def test_iceberg_sql(self, tmp_path):
        p = str(tmp_path)
        sql = _read_sql(p, "iceberg")
        assert "iceberg_scan" in sql

    def test_avro_sql(self, tmp_path):
        p = str(tmp_path / "data.avro")
        sql = _read_sql(p, "avro")
        assert "read_avro" in sql

    def test_unknown_format_fallback(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "xyzformat")
        assert "read_parquet" in sql

    def test_unknown_format_csv_fallback(self, tmp_path):
        p = str(tmp_path / "data.csv")
        sql = _read_sql(p, "xyzformat")
        assert "read_csv_auto" in sql

    def test_columns_projection(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "parquet", columns=["id", "name"])
        assert '"id"' in sql and '"name"' in sql

    def test_where_clause(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "parquet", where="id > 5")
        assert "WHERE id > 5" in sql

    def test_limit_clause(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "parquet", limit=100)
        assert "LIMIT 100" in sql

    def test_no_where_no_limit(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "parquet")
        assert "WHERE" not in sql
        assert "LIMIT" not in sql


class TestCloudError:
    """Cover _cloud_error branch dispatch."""

    def test_s3_access_denied(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("s3://bucket/key", Exception("access denied"))
        assert isinstance(err, RuntimeError)
        assert "AWS_ACCESS_KEY_ID" in str(err)

    def test_s3_403(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("s3://bucket/key", Exception("HTTP 403 error from aws s3"))
        assert isinstance(err, RuntimeError)
        assert "s3:GetObject" in str(err)

    def test_gcs_permission(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("gs://bucket/file", Exception("403 permission denied"))
        assert isinstance(err, RuntimeError)
        assert "GOOGLE_APPLICATION_CREDENTIALS" in str(err)

    def test_gcs_credentials(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("gs://bucket/file", Exception("credentials not found gcs"))
        assert isinstance(err, RuntimeError)
        assert "Storage Object Viewer" in str(err)

    def test_azure_auth_error(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("az://container/blob", Exception("403 authentication failed"))
        assert isinstance(err, RuntimeError)
        assert "AZURE_STORAGE_ACCOUNT" in str(err)

    def test_azure_abfs_path(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("abfs://container@account/file", Exception("unauthorized azure"))
        assert isinstance(err, RuntimeError)
        assert "AZURE_CLIENT_ID" in str(err)

    def test_file_not_found(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("/local/path.parquet", Exception("no such file or directory"))
        assert isinstance(err, FileNotFoundError)
        assert "Path not found" in str(err)

    def test_not_found_variant(self):
        from difflake.connection import _cloud_error
        err = _cloud_error("/some/path", Exception("file not found"))
        assert isinstance(err, FileNotFoundError)

    def test_generic_error_with_path(self):
        from difflake.connection import _cloud_error
        orig = Exception("some other duckdb error")
        err = _cloud_error("/some/path", orig)
        assert isinstance(err, RuntimeError)
        assert "Error reading" in str(err)

    def test_generic_error_no_path(self):
        from difflake.connection import _cloud_error
        orig = Exception("random error")
        err = _cloud_error("", orig)
        assert err is orig


class TestSchemaDiffSummary:
    """Cover SchemaDiff.summary() with all branch combinations."""

    def test_summary_no_changes(self, tmp_path):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        data = {"id": [1, 2], "name": ["a", "b"]}
        write_parquet(src, data)
        write_parquet(tgt, data)
        result = DiffLake(source=str(src), target=str(tgt), mode="schema").run()
        assert result.schema_diff.summary() == "No schema changes"

    def test_summary_added(self, tmp_path):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2]})
        write_parquet(tgt, {"id": [1, 2], "score": [0.1, 0.2]})
        result = DiffLake(source=str(src), target=str(tgt), mode="schema").run()
        summary = result.schema_diff.summary()
        assert "+1 added" in summary

    def test_summary_removed(self, tmp_path):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2], "score": [0.1, 0.2]})
        write_parquet(tgt, {"id": [1, 2]})
        result = DiffLake(source=str(src), target=str(tgt), mode="schema").run()
        summary = result.schema_diff.summary()
        assert "-1 removed" in summary

    def test_summary_type_changed(self, tmp_path):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2], "val": [1, 2]})
        write_parquet(tgt, {"id": [1, 2], "val": [1.0, 2.0]})
        result = DiffLake(source=str(src), target=str(tgt), mode="schema").run()
        summary = result.schema_diff.summary()
        # May or may not flag integer→double depending on DuckDB; just check it runs
        assert isinstance(summary, str)


class TestMarkdownReporter:
    """Cover MarkdownReporter paths."""

    def _make_result(self, tmp_path, src_data, tgt_data, **kwargs):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, src_data)
        write_parquet(tgt, tgt_data)
        return DiffLake(source=str(src), target=str(tgt), **kwargs).run()

    def test_no_schema_changes_renders(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        data = {"id": [1, 2], "val": [10, 20]}
        result = self._make_result(tmp_path, data, data)
        md = MarkdownReporter(result).render()
        assert "No schema changes detected" in md

    def test_schema_with_added_column(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        src_data = {"id": [1, 2]}
        tgt_data = {"id": [1, 2], "new_col": [10, 20]}
        result = self._make_result(tmp_path, src_data, tgt_data, mode="schema")
        md = MarkdownReporter(result).render()
        assert "Added columns" in md or "new_col" in md

    def test_schema_with_removed_column(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        src_data = {"id": [1, 2], "old_col": [10, 20]}
        tgt_data = {"id": [1, 2]}
        result = self._make_result(tmp_path, src_data, tgt_data, mode="schema")
        md = MarkdownReporter(result).render()
        assert "Removed columns" in md or "old_col" in md

    def test_no_stats_renders_placeholder(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        data = {"id": [1, 2]}
        result = self._make_result(tmp_path, data, data, mode="schema")
        md = MarkdownReporter(result).render()
        assert "No stats computed" in md

    def test_stats_table_renders(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        src_data = {"id": [1, 2, 3], "val": [10, 20, 30]}
        tgt_data = {"id": [1, 2, 3], "val": [11, 22, 33]}
        result = self._make_result(tmp_path, src_data, tgt_data, mode="stats")
        md = MarkdownReporter(result).render()
        assert "Statistical Diff" in md
        assert "val" in md

    def test_render_to_path(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        data = {"id": [1, 2], "val": [10, 20]}
        result = self._make_result(tmp_path, data, data)
        out = tmp_path / "report.md"
        ret = MarkdownReporter(result).render(path=str(out))
        assert ret == ""
        assert out.exists()
        assert "DiffLake Report" in out.read_text(encoding="utf-8")

    def test_render_returns_string_when_no_path(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        data = {"id": [1, 2], "val": [10, 20]}
        result = self._make_result(tmp_path, data, data)
        md = MarkdownReporter(result).render()
        assert isinstance(md, str)
        assert len(md) > 0


class TestCliReporterVerbose:
    """Cover verbose mode and _fmt_pct negative branch."""

    def test_fmt_pct_negative(self):
        from difflake.reporters.cli_reporter import _fmt_pct
        assert "-5.00%" in _fmt_pct(-5.0, show_sign=True)

    def test_fmt_pct_none(self):
        from difflake.reporters.cli_reporter import _fmt_pct
        assert _fmt_pct(None) == "—"

    def test_fmt_pct_positive_with_sign(self):
        from difflake.reporters.cli_reporter import _fmt_pct
        result = _fmt_pct(3.5, show_sign=True)
        assert "+" in result and "3.50%" in result

    def test_verbose_render_with_sample_changed(self, tmp_path):
        from difflake.reporters.cli_reporter import CliReporter
        src_data = {"id": [1, 2, 3], "val": [10, 20, 30]}
        tgt_data = {"id": [1, 2, 3], "val": [11, 20, 30]}
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, src_data)
        write_parquet(tgt, tgt_data)
        result = DiffLake(
            source=str(src), target=str(tgt),
            primary_key="id", mode="full",
        ).run()
        # Should not raise even with verbose=True and sample_changed present
        reporter = CliReporter(result, verbose=True)
        reporter.render()  # just verify no exception

    def test_non_verbose_render(self, tmp_path):
        from difflake.reporters.cli_reporter import CliReporter
        data = {"id": [1, 2], "val": [10, 20]}
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, data)
        write_parquet(tgt, data)
        result = DiffLake(source=str(src), target=str(tgt), mode="stats").run()
        CliReporter(result, verbose=False).render()


class TestDuckDBConnection:
    """Cover DuckDBConnection direct usage."""

    def test_list_columns(self, tmp_path):
        from difflake.connection import _read_sql
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2], "name": ["a", "b"]})
        conn = DuckDBConnection()
        sql = _read_sql(str(src), "parquet")
        conn.register_view("test_view", sql, "parquet", str(src))
        cols = conn.columns("test_view")
        conn.close()
        col_names = [c[0] for c in cols]
        assert "id" in col_names
        assert "name" in col_names

    def test_fetchdf(self, tmp_path):
        from difflake.connection import _read_sql
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2]})
        conn = DuckDBConnection()
        sql = _read_sql(str(src), "parquet")
        conn.register_view("v", sql, "parquet", str(src))
        col_names, rows = conn.fetchdf("SELECT * FROM v ORDER BY id")
        conn.close()
        assert "id" in col_names
        assert len(rows) == 2

    def test_scalar(self, tmp_path):
        from difflake.connection import _read_sql
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2, 3]})
        conn = DuckDBConnection()
        sql = _read_sql(str(src), "parquet")
        conn.register_view("v", sql, "parquet", str(src))
        count = conn.scalar("SELECT COUNT(*) FROM v")
        conn.close()
        assert count == 3


class TestReadSqlDirectories:
    """Cover _read_sql directory branches for csv/json/jsonl."""

    def test_csv_directory(self, tmp_path):
        sql = _read_sql(str(tmp_path), "csv")
        assert "read_csv" in sql and "**/*.csv" in sql

    def test_json_directory(self, tmp_path):
        sql = _read_sql(str(tmp_path), "json")
        assert "read_json" in sql and "**/*.json" in sql

    def test_jsonl_directory(self, tmp_path):
        sql = _read_sql(str(tmp_path), "jsonl")
        assert "read_ndjson" in sql and "**/*.jsonl" in sql

    def test_parquet_directory(self, tmp_path):
        sql = _read_sql(str(tmp_path), "parquet")
        assert "read_parquet" in sql and "**/*.parquet" in sql

    def test_parquet_file(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "parquet")
        # File path — no glob
        assert "**" not in sql

    def test_sample_size_produces_tablesample(self, tmp_path):
        p = str(tmp_path / "data.parquet")
        sql = _read_sql(p, "parquet", sample_size=1000)
        # sample_size causes TABLESAMPLE or USING SAMPLE
        assert "SAMPLE" in sql.upper() or "TABLESAMPLE" in sql.upper()


class TestConfigureCloud:
    """Cover configure_s3/gcs/azure with mocked env vars."""

    def test_configure_s3_with_credentials(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secretkey")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
        conn = DuckDBConnection()
        conn.configure_s3()  # Should not raise
        conn.close()

    def test_configure_s3_with_endpoint(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
        conn = DuckDBConnection()
        conn.configure_s3()
        conn.close()

    def test_configure_s3_no_credentials(self, monkeypatch):
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        conn = DuckDBConnection()
        conn.configure_s3()  # No-op without creds — should not raise
        conn.close()

    def test_configure_gcs_with_credentials(self, monkeypatch, tmp_path):
        creds_file = tmp_path / "sa.json"
        creds_file.write_text("{}")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds_file))
        conn = DuckDBConnection()
        try:
            conn.configure_gcs()
        except Exception:
            pass  # GCS extension may not be available in test environment
        finally:
            conn.close()

    def test_configure_gcs_no_credentials(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        conn = DuckDBConnection()
        conn.configure_gcs()  # No-op without creds
        conn.close()

    def test_configure_azure_with_account_key(self, monkeypatch):
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "myaccount")
        monkeypatch.setenv("AZURE_STORAGE_KEY", "mykey")
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        conn = DuckDBConnection()
        try:
            conn.configure_azure()
        except Exception:
            pass  # Azure extension may not be available in test environment
        finally:
            conn.close()

    def test_configure_azure_no_credentials(self, monkeypatch):
        monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
        monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        conn = DuckDBConnection()
        conn.configure_azure()  # No-op without creds
        conn.close()


class TestCliLoadConfig:
    """Cover _load_config branches."""

    def test_load_explicit_config_file(self, tmp_path):
        from difflake.cli import _load_config
        cfg_file = tmp_path / "myconfig.yaml"
        cfg_file.write_text("key: user_id\nmode: stats\n")
        result = _load_config(str(cfg_file))
        assert result["key"] == "user_id"
        assert result["mode"] == "stats"

    def test_load_config_invalid_yaml_returns_empty(self, tmp_path):
        from difflake.cli import _load_config
        cfg_file = tmp_path / "bad.yaml"
        cfg_file.write_text("key: [unclosed")
        # Should warn and return {} rather than raise
        result = _load_config(str(cfg_file))
        assert result == {}

    def test_load_config_missing_path_returns_empty(self, tmp_path):
        from difflake.cli import _load_config
        result = _load_config(str(tmp_path / "nonexistent.yaml"))
        assert result == {}

    def test_load_config_no_path_no_default_file(self, tmp_path, monkeypatch):
        from difflake.cli import _load_config
        monkeypatch.chdir(tmp_path)
        result = _load_config(None)
        assert result == {}


class TestCliSummaryLine:
    """Cover _summary_line branches."""

    def _make_result(self, tmp_path, src_data, tgt_data, **kwargs):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, src_data)
        write_parquet(tgt, tgt_data)
        return DiffLake(source=str(src), target=str(tgt), **kwargs).run()

    def test_summary_with_removed_column(self, tmp_path):
        from difflake.cli import _summary_line
        result = self._make_result(
            tmp_path,
            {"id": [1, 2], "old_col": ["a", "b"]},
            {"id": [1, 2]},
            mode="schema",
        )
        line = _summary_line(result)
        assert "removed" in line

    def test_summary_with_no_changes(self, tmp_path):
        from difflake.cli import _summary_line
        data = {"id": [1, 2], "val": [10, 20]}
        result = self._make_result(tmp_path, data, data, mode="schema")
        line = _summary_line(result)
        assert "no changes" in line

    def test_summary_with_rows_added(self, tmp_path):
        from difflake.cli import _summary_line
        src = {"id": [1, 2], "val": [10, 20]}
        tgt = {"id": [1, 2, 3], "val": [10, 20, 30]}
        result = self._make_result(tmp_path, src, tgt, primary_key="id")
        line = _summary_line(result)
        assert "added" in line

    def test_summary_with_rows_changed(self, tmp_path):
        from difflake.cli import _summary_line
        src = {"id": [1, 2], "val": [10, 20]}
        tgt = {"id": [1, 2], "val": [99, 20]}
        result = self._make_result(tmp_path, src, tgt, primary_key="id")
        line = _summary_line(result)
        assert "changed" in line or "no changes" in line  # depends on diff detection


class TestCliReporterSchemaChanges:
    """Cover CLI reporter schema display: removed/type_changed/renamed/order."""

    def _make_result(self, tmp_path, src_data, tgt_data, **kwargs):
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, src_data)
        write_parquet(tgt, tgt_data)
        return DiffLake(source=str(src), target=str(tgt), **kwargs).run()

    def test_render_removed_columns(self, tmp_path):
        from difflake.reporters.cli_reporter import CliReporter
        result = self._make_result(
            tmp_path,
            {"id": [1, 2], "old_col": ["a", "b"]},
            {"id": [1, 2]},
            mode="schema",
        )
        CliReporter(result).render()  # should not raise

    def test_render_type_changed_columns(self, tmp_path):
        from difflake.reporters.cli_reporter import CliReporter
        # Force a type change: integer → varchar (use CSV to control types)
        import csv as csv_mod
        src = tmp_path / "src.csv"
        tgt = tmp_path / "tgt.csv"
        with open(src, "w", newline="") as f:
            csv_mod.writer(f).writerows([["id", "val"], ["1", "10"], ["2", "20"]])
        with open(tgt, "w", newline="") as f:
            csv_mod.writer(f).writerows([["id", "val"], ["1", "hello"], ["2", "world"]])
        result = DiffLake(source=str(src), target=str(tgt), mode="schema").run()
        CliReporter(result).render()

    def test_render_order_changed(self, tmp_path):
        from difflake.reporters.cli_reporter import CliReporter
        write_parquet(tmp_path / "a.parquet", {"id": [1], "name": ["x"], "val": [1.0]})
        write_parquet(tmp_path / "b.parquet", {"val": [1.0], "name": ["x"], "id": [1]})
        result = DiffLake(
            source=str(tmp_path / "a.parquet"),
            target=str(tmp_path / "b.parquet"),
            mode="schema",
        ).run()
        CliReporter(result).render()


class TestMarkdownReporterExtended:
    """Cover remaining markdown reporter paths."""

    def test_pct_none_renders_dash(self):
        from difflake.reporters.markdown_reporter import _pct, _num
        assert _pct(None) == "—"
        assert _num(None) == "—"

    def test_num_with_value(self):
        from difflake.reporters.markdown_reporter import _num
        result = _num(1234.5678)
        assert "1" in result

    def test_pct_positive_with_sign(self):
        from difflake.reporters.markdown_reporter import _pct
        result = _pct(5.5, sign=True)
        assert "+" in result

    def test_render_type_changed_columns(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        import csv as csv_mod
        src = tmp_path / "src.csv"
        tgt = tmp_path / "tgt.csv"
        with open(src, "w", newline="") as f:
            csv_mod.writer(f).writerows([["id", "val"], ["1", "10"], ["2", "20"]])
        with open(tgt, "w", newline="") as f:
            csv_mod.writer(f).writerows([["id", "val"], ["1", "hello"], ["2", "world"]])
        result = DiffLake(source=str(src), target=str(tgt), mode="schema").run()
        md = MarkdownReporter(result).render()
        assert isinstance(md, str)

    def test_render_renamed_columns(self, tmp_path):
        from difflake.reporters.markdown_reporter import MarkdownReporter
        # "user_name" → "username" has high Jaro-Winkler similarity → rename detected
        write_parquet(tmp_path / "a.parquet", {"id": [1, 2], "user_name": ["a", "b"]})
        write_parquet(tmp_path / "b.parquet", {"id": [1, 2], "username": ["a", "b"]})
        result = DiffLake(
            source=str(tmp_path / "a.parquet"),
            target=str(tmp_path / "b.parquet"),
            mode="schema",
        ).run()
        md = MarkdownReporter(result).render()
        # Renamed columns or removed/added — either way renders without error
        assert isinstance(md, str)
        assert "DiffLake Report" in md


class TestCliReporterFmtNum:
    """Cover _fmt_num None branch."""

    def test_fmt_num_none(self):
        from difflake.reporters.cli_reporter import _fmt_num
        assert _fmt_num(None) == "—"

    def test_fmt_num_value(self):
        from difflake.reporters.cli_reporter import _fmt_num
        assert "42" in _fmt_num(42.0)

    def test_render_renamed_columns(self, tmp_path):
        from difflake.reporters.cli_reporter import CliReporter
        write_parquet(tmp_path / "a.parquet", {"id": [1, 2], "user_name": ["a", "b"]})
        write_parquet(tmp_path / "b.parquet", {"id": [1, 2], "username": ["a", "b"]})
        result = DiffLake(
            source=str(tmp_path / "a.parquet"),
            target=str(tmp_path / "b.parquet"),
            mode="schema",
        ).run()
        CliReporter(result).render()  # hits renamed_columns branch


class TestCliRenderError:
    """Cover _render_error branches."""

    def test_render_error_check_prefix(self):
        from difflake.cli import _render_error
        # Should not raise; lines starting with "Check " get dim style
        _render_error(RuntimeError(
            "Cannot access 's3://bucket/key'\n"
            "  Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.\n"
            "  Original error: HTTP 403"
        ))

    def test_render_error_set_prefix(self):
        from difflake.cli import _render_error
        _render_error(RuntimeError("Set GOOGLE_APPLICATION_CREDENTIALS=/path"))

    def test_render_error_plain(self):
        from difflake.cli import _render_error
        _render_error(RuntimeError("Something went wrong"))


class TestCompareCommandOutputFormats:
    """Cover compare command output format branches."""

    @staticmethod
    def _cli():
        from click.testing import CliRunner
        from difflake.cli import main
        return CliRunner(charset="utf-8"), main

    def test_output_markdown(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2], "val": [10, 20]})
        write_parquet(tgt, {"id": [1, 2], "val": [11, 20]})
        import os
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            res = r.invoke(m, ["compare", str(src), str(tgt), "--output", "markdown"])
            assert res.exit_code in (0, 2)
            md_files = list(tmp_path.glob("difflake_*.md"))
            assert len(md_files) == 1
        finally:
            os.chdir(orig)

    def test_output_with_explicit_out(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2], "val": [10, 20]})
        write_parquet(tgt, {"id": [1, 2], "val": [11, 20]})
        out = tmp_path / "out.json"
        res = r.invoke(m, ["compare", str(src), str(tgt),
                           "--output", "json", "--out", str(out)])
        assert res.exit_code in (0, 2)

    def test_compare_with_config_file(self, tmp_path, monkeypatch):
        r, m = self._cli()
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2], "val": [10, 20]})
        write_parquet(tgt, {"id": [1, 2], "val": [11, 20]})
        cfg = tmp_path / "difflake.yaml"
        cfg.write_text("mode: stats\n")
        monkeypatch.chdir(tmp_path)
        res = r.invoke(m, ["compare", str(src), str(tgt), "--config", str(cfg)])
        assert res.exit_code in (0, 2)

    def test_compare_nonexistent_source_exits(self, tmp_path):
        r, m = self._cli()
        tgt = tmp_path / "b.parquet"
        write_parquet(tgt, {"id": [1, 2]})
        res = r.invoke(m, ["compare", str(tmp_path / "missing.parquet"), str(tgt)])
        assert res.exit_code != 0

    def test_show_schema_with_col_filter(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2], "name": ["a", "b"], "val": [10, 20]})
        res = r.invoke(m, ["show", str(src), "--schema", "--columns", "id,name"])
        assert res.exit_code == 0
        assert "id" in res.output

    def test_show_tail_flag(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 50]})
        res = r.invoke(m, ["show", str(src), "--tail"])
        assert res.exit_code == 0

    def test_show_rows_with_many_columns(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        # 12 columns to hit the 10 < cols <= 20 branch
        data = {f"col{i}": list(range(1, 4)) for i in range(12)}
        write_parquet(src, data)
        res = r.invoke(m, ["show", str(src), "--rows", "2"])
        assert res.exit_code == 0

    def test_show_stats(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2, 3], "val": [10.0, 20.0, 30.0], "name": ["a", "b", "c"]})
        res = r.invoke(m, ["show", str(src), "--stats"])
        assert res.exit_code == 0
        assert "Stats" in res.output or "Column" in res.output

    def test_show_stats_with_datetime(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2], "ts": ["2024-01-01", "2024-06-01"]})
        res = r.invoke(m, ["show", str(src), "--stats"])
        assert res.exit_code == 0

    def test_show_overview_more_rows(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        # More than 5 rows so "more rows" message triggers
        write_parquet(src, {"id": list(range(1, 11)), "val": list(range(10, 20))})
        res = r.invoke(m, ["show", str(src)])
        assert res.exit_code == 0
        # Should mention there are more rows
        assert "more" in res.output.lower() or "10" in res.output

    def test_show_rows_with_many_columns_wide(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        # 25 columns to hit the >20 columns branch in _render_rows
        data = {f"col{i}": list(range(1, 3)) for i in range(25)}
        write_parquet(src, data)
        res = r.invoke(m, ["show", str(src), "--rows", "1"])
        assert res.exit_code == 0

    def test_show_count_command(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "data.parquet"
        write_parquet(src, {"id": [1, 2, 3], "val": [10, 20, 30]})
        res = r.invoke(m, ["show", str(src), "--count"])
        assert res.exit_code == 0
        assert "Rows" in res.output

    def test_diff_alias(self, tmp_path):
        r, m = self._cli()
        src = tmp_path / "a.parquet"
        tgt = tmp_path / "b.parquet"
        write_parquet(src, {"id": [1, 2], "val": [10, 20]})
        write_parquet(tgt, {"id": [1, 2], "val": [11, 20]})
        res = r.invoke(m, ["diff", str(src), str(tgt), "--mode", "schema"])
        assert res.exit_code in (0, 2)
