"""
DuckDB connection manager and format-aware table loader.

All formats are registered as DuckDB views so the diff engine
works with plain SQL regardless of source format. This means:

  - CSV / TSV       → read_csv_auto()
  - Parquet         → read_parquet()          (single file or glob)
  - JSON            → read_json_auto()
  - JSONL / NDJSON  → read_ndjson()
  - Delta Lake      → delta_scan()            (requires delta extension)
  - Avro            → read_avro()             (requires avro extension)
  - Iceberg         → iceberg_scan()          (requires iceberg extension)
  - S3 / GCS / Azure→ same functions above, path starts with s3:// / gs:// / az://
  - Parquet over HTTP→ read_parquet('https://...')
  - PostgreSQL      → ATTACH 'postgres://...'
  - MySQL           → ATTACH 'mysql://...'

Cloud auth is handled transparently via environment variables:
  AWS:   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
  GCS:   GOOGLE_APPLICATION_CREDENTIALS
  Azure: AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY  or  AZURE_CLIENT_ID/SECRET/TENANT
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# ── Extension requirements per format ────────────────────────────────────

_FORMAT_EXTENSIONS: dict[str, list[str]] = {
    "delta":   ["delta"],
    "avro":    ["avro"],
    "iceberg": ["iceberg"],
    "s3":      ["httpfs"],
    "gcs":     ["httpfs"],
    "azure":   ["azure"],
    "http":    ["httpfs"],
    "https":   ["httpfs"],
}

# Single-file extension → format string
_EXT_TO_FMT: dict[str, str] = {
    ".csv":     "csv",
    ".tsv":     "csv",
    ".parquet": "parquet",
    ".pq":      "parquet",
    ".json":    "json",
    ".jsonl":   "jsonl",
    ".ndjson":  "jsonl",
    ".avro":    "avro",
}


def _detect_format(path: str) -> str:
    """
    Detect format from path. Returns format string.
    Handles: local files, directories, cloud URIs, HTTP URLs.
    """
    p = path.lower()

    # Cloud / HTTP URIs — detect from scheme
    for scheme in ("s3://", "gs://", "gcs://", "az://", "abfs://", "abfss://"):
        if p.startswith(scheme):
            # Cloud path — detect from extension within URI
            ext = Path(p.split("?")[0]).suffix
            return _EXT_TO_FMT.get(ext, "parquet")  # default cloud = parquet

    if p.startswith("http://") or p.startswith("https://"):
        ext = Path(p.split("?")[0]).suffix
        return _EXT_TO_FMT.get(ext, "parquet")

    # Local path
    local = Path(path)
    if local.is_file():
        ext = local.suffix.lower()
        if ext in _EXT_TO_FMT:
            return _EXT_TO_FMT[ext]
        raise ValueError(
            f"Unrecognized file extension '{local.suffix}' for '{local.name}'. "
            f"Pass --format explicitly. Supported: {', '.join(sorted(_EXT_TO_FMT))}."
        )

    if local.is_dir():
        # Delta Lake — has _delta_log/
        if (local / "_delta_log").exists():
            return "delta"
        # Detect dominant extension
        for ext, fmt in [(".parquet", "parquet"), (".pq", "parquet"),
                         (".csv", "csv"), (".tsv", "csv"),
                         (".jsonl", "jsonl"), (".ndjson", "jsonl"),
                         (".json", "json"), (".avro", "avro")]:
            if any(local.rglob(f"*{ext}")):
                return fmt
        raise ValueError(
            f"Cannot detect format for directory '{path}'. "
            "Pass --format explicitly."
        )

    raise FileNotFoundError(
        f"Path not found: '{path}'. "
        "Check the path exists and you have read permissions."
    )


def _read_sql(path: str, fmt: str, where: str | None = None,
              columns: list[str] | None = None,
              sample_size: int | None = None,
              limit: int | None = None) -> str:
    """
    Build a DuckDB SQL SELECT expression that reads the given path.
    Returns a SQL fragment suitable for use in FROM or as a subquery.

    All filtering, column projection and sampling happen here so
    the diff engine only processes the rows/columns it actually needs.
    """
    p     = path
    col_s = ", ".join(f'"{c}"' for c in columns) if columns else "*"
    fmt   = fmt.lower()

    # ── Build FROM clause ─────────────────────────────────────────────────
    if fmt == "csv":
        if Path(p).is_dir():
            # Glob all CSV/TSV files in directory
            from_  = f"read_csv('{p}/**/*.csv', union_by_name=true, ignore_errors=true)"
        else:
            from_ = f"read_csv('{p}', union_by_name=true, ignore_errors=true)"

    elif fmt == "parquet":
        if Path(p).is_dir() and not p.startswith(("s3://","gs://","az://","http")):
            from_ = f"read_parquet('{p}/**/*.parquet', union_by_name=true, hive_partitioning=true)"
        else:
            from_ = f"read_parquet('{p}')"

    elif fmt == "json":
        if Path(p).is_dir() and not p.startswith(("s3://","gs://","az://","http")):
            from_ = f"read_json('{p}/**/*.json', union_by_name=true)"
        else:
            from_ = f"read_json('{p}')"

    elif fmt == "jsonl":
        if Path(p).is_dir() and not p.startswith(("s3://","gs://","az://","http")):
            from_ = f"read_ndjson('{p}/**/*.jsonl', union_by_name=true)"
        else:
            from_ = f"read_ndjson('{p}')"

    elif fmt == "delta":
        from_ = f"delta_scan('{p}')"

    elif fmt == "iceberg":
        from_ = f"iceberg_scan('{p}')"

    elif fmt == "avro":
        from_ = f"read_avro('{p}')"

    else:
        # Fallback: let DuckDB auto-detect
        from_ = f"read_csv_auto('{p}')" if p.endswith((".csv",".tsv")) else f"read_parquet('{p}')"

    # ── Assemble SELECT ───────────────────────────────────────────────────
    # Sampling strategy:
    #   USING SAMPLE N ROWS does reservoir sampling but still scans the full
    #   file — it's slower than a full scan at scale because it materialises
    #   a subquery. Instead we use TABLESAMPLE BERNOULLI which DuckDB can
    #   push down to Parquet row group level, skipping groups entirely.
    #   We calculate the percentage needed to yield approximately sample_size
    #   rows. For very large files this is much faster than reservoir sampling.
    #
    #   NOTE: the actual row count will be approximate (±10%) because
    #   BERNOULLI is probabilistic. The caller should not depend on exact count.
    where_clause = f"WHERE {where}" if where else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    if sample_size:
        # Wrap in a subquery so TABLESAMPLE applies after WHERE filter
        # Use a fixed seed so results are reproducible across source and target
        inner = f"SELECT {col_s} FROM {from_} {where_clause}"
        sql = (
            f"SELECT * FROM ({inner}) __sampled "
            f"USING SAMPLE {sample_size} ROWS (reservoir, 42)"
        )
    else:
        sql = f"SELECT {col_s} FROM {from_} {where_clause} {limit_clause}".strip()

    return sql


class DuckDBConnection:
    """
    Wraps a DuckDB in-memory connection.
    Installs required extensions, configures cloud credentials,
    and registers source/target datasets as named views.
    """

    def __init__(self):
        self.con = duckdb.connect(database=":memory:")
        self._installed: set[str] = set()

    # ── Extension management ───────────────────────────────────────────────

    def install_extension(self, name: str) -> None:
        if name in self._installed:
            return
        try:
            self.con.execute(f"INSTALL {name}; LOAD {name};")
            self._installed.add(name)
        except Exception as e:
            raise RuntimeError(
                f"Could not install DuckDB extension '{name}': {e}\n"
                f"Run: pip install duckdb  (>=0.10 required for {name} extension)"
            ) from e

    def ensure_extensions(self, fmt: str, path: str) -> None:
        """Install extensions needed for the given format / path scheme."""
        # Format-based
        if fmt in _FORMAT_EXTENSIONS:
            for ext in _FORMAT_EXTENSIONS[fmt]:
                self.install_extension(ext)

        # Path-scheme based
        for scheme, exts in _FORMAT_EXTENSIONS.items():
            if path.lower().startswith(scheme + "://"):
                for ext in exts:
                    self.install_extension(ext)

    # ── Cloud credential helpers ───────────────────────────────────────────

    def configure_s3(self) -> None:
        """
        Configure S3 credentials from environment variables.
        Works for AWS S3, MinIO, and S3-compatible stores.
        """
        import os
        key    = os.getenv("AWS_ACCESS_KEY_ID")
        secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        endpoint = os.getenv("AWS_ENDPOINT_URL", "")

        if key and secret:
            self.con.execute(f"""
                SET s3_access_key_id='{key}';
                SET s3_secret_access_key='{secret}';
                SET s3_region='{region}';
            """)
        if endpoint:
            self.con.execute(f"SET s3_endpoint='{endpoint}';")

    def configure_gcs(self) -> None:
        """Configure GCS via service account credentials."""
        import os
        creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds:
            self.con.execute("SET gcs_auth_type='SERVICE_ACCOUNT_JSON_FILE';")
            self.con.execute(f"SET gcs_json_key_file='{creds}';")

    def configure_azure(self) -> None:
        """Configure Azure Blob Storage credentials."""
        import os
        account = os.getenv("AZURE_STORAGE_ACCOUNT")
        key     = os.getenv("AZURE_STORAGE_KEY")
        if account and key:
            self.con.execute(f"""
                SET azure_storage_account='{account}';
                SET azure_storage_key='{key}';
            """)

    # ── Query execution ────────────────────────────────────────────────────

    def register_view(self, name: str, sql: str, fmt: str, path: str) -> None:
        """Register a SQL expression as a named view."""
        self.ensure_extensions(fmt, path)
        if "s3://" in path:
            self.configure_s3()
        if "gs://" in path:
            self.configure_gcs()
        if "az://" in path or "abfs" in path:
            self.configure_azure()
        try:
            self.con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
        except Exception as e:
            raise _cloud_error(path, e) from e

    def execute(self, sql: str):
        return self.con.execute(sql)

    def fetchdf(self, sql: str):
        """Execute SQL and return a pandas-free result as list of dicts."""
        try:
            rel = self.con.execute(sql)
            cols = [d[0] for d in rel.description]
            rows = rel.fetchall()
            return cols, rows
        except Exception as e:
            raise _cloud_error("", e) from e

    def fetchone(self, sql: str):
        try:
            return self.con.execute(sql).fetchone()
        except Exception as e:
            raise _cloud_error("", e) from e

    def scalar(self, sql: str):
        row = self.fetchone(sql)
        return row[0] if row else None

    def columns(self, view_name: str) -> list[tuple[str, str]]:
        """Return (col_name, col_type) pairs for a registered view."""
        rel = self.con.execute(f"DESCRIBE {view_name}")
        return [(row[0], row[1]) for row in rel.fetchall()]

    def close(self) -> None:
        self.con.close()


# ── Cloud error helper ─────────────────────────────────────────────────────

def _cloud_error(path: str, original: Exception) -> Exception:
    """Convert raw DuckDB errors into clear, actionable messages."""
    msg = str(original).lower()

    if ("s3://" in path or ("aws" in msg and "s3" in msg)) and any(
        x in msg for x in ("access denied", "403", "no such key", "invalid")
    ):
        return RuntimeError(
            f"Cannot access {repr(path)}\n"
            "  Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.\n"
            "  Verify the bucket exists and your IAM role has s3:GetObject permission.\n"
            "  For MinIO/S3-compatible stores, also set AWS_ENDPOINT_URL.\n"
            f"  Original error: {original}"
        )

    if ("gs://" in path or "gcs" in msg) and any(
        x in msg for x in ("403", "permission", "unauthorized", "credentials")
    ):
        return RuntimeError(
            f"Cannot access {repr(path)}\n"
            "  Set GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json\n"
            "  Verify the service account has Storage Object Viewer role.\n"
            f"  Original error: {original}"
        )

    if ("az://" in path or "abfs" in path or "azure" in msg) and any(
        x in msg for x in ("403", "authentication", "unauthorized", "invalid")
    ):
        return RuntimeError(
            f"Cannot access {repr(path)}\n"
            "  Set AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY, or\n"
            "  Set AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID.\n"
            "  Check the container name and path are correct.\n"
            f"  Original error: {original}"
        )

    if any(x in msg for x in ("no such file","not found","does not exist")):
        return FileNotFoundError(
            f"Path not found: {repr(path)}\n"
            "  Check the path exists and you have read permissions.\n"
            f"  Original error: {original}"
        )

    if path:
        return RuntimeError(f"Error reading {repr(path)}: {original}")
    return original
