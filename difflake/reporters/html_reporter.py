"""
HTML Reporter — generates a self-contained HTML diff report.

Enhancements over v1:
  - Offline mode: Chart.js is downloaded and embedded inline at generation
    time so the report works without internet access. Falls back to CDN
    gracefully if the download fails.
  - Two additional charts:
      • Null-rate comparison bar chart (before vs after per column)
      • Mean-drift bar chart for numeric columns
  - Enhanced stats table with min/max/std columns and inline drift reasons.
  - Collapsible sections (pure CSS/JS, no dependencies).
  - Column search/filter for the stats table.
  - Generated-at timestamp in the header.
"""

from __future__ import annotations

import json as _json
from datetime import datetime
from pathlib import Path

from jinja2 import BaseLoader, Environment

from difflake.models import DiffResult

# ── Chart.js CDN fallback ─────────────────────────────────────────────────
_CHARTJS_CDN = (
    "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"
)


def _fetch_chartjs() -> str | None:
    """Download Chart.js and return the minified source, or None on failure."""
    try:
        import urllib.request
        with urllib.request.urlopen(_CHARTJS_CDN, timeout=5) as resp:
            return str(resp.read().decode("utf-8"))
    except Exception:
        return None


def _chartjs_tag(offline: bool) -> str:
    """Return the <script> tag for Chart.js (inline if offline embed works)."""
    if offline:
        src = _fetch_chartjs()
        if src:
            return f"<script>{src}</script>"
    return f'<script src="{_CHARTJS_CDN}"></script>'


# ── Jinja2 template ───────────────────────────────────────────────────────
_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>difflake Report — {{ r.source_path|truncate(40, True, '…') }}</title>
{{ chartjs_tag }}
<style>
  :root {
    --green:#22c55e; --red:#ef4444; --yellow:#f59e0b;
    --blue:#3b82f6; --gray:#6b7280; --bg:#0f172a; --surface:#1e293b;
    --border:#334155; --text:#f1f5f9; --muted:#94a3b8;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;padding:2rem}
  h1{font-size:1.8rem;margin-bottom:.25rem}
  .subtitle{color:var(--muted);font-size:.9rem;margin-bottom:1.5rem}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem;margin-bottom:2rem}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.25rem}
  .card .label{font-size:.75rem;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}
  .card .value{font-size:1.6rem;font-weight:700;margin-top:.25rem}
  .green{color:var(--green)} .red{color:var(--red)} .yellow{color:var(--yellow)} .blue{color:var(--blue)}
  /* Sections */
  details{border:1px solid var(--border);border-radius:8px;margin-bottom:1.5rem;overflow:hidden}
  details[open] summary{border-bottom:1px solid var(--border)}
  summary{background:var(--surface);padding:.75rem 1.25rem;cursor:pointer;font-size:1.1rem;font-weight:600;list-style:none;display:flex;align-items:center;gap:.5rem;user-select:none}
  summary::after{content:"▸";margin-left:auto;transition:.2s}
  details[open] summary::after{content:"▾"}
  summary:hover{background:#263448}
  .section-body{padding:1.25rem}
  /* Tables */
  table{width:100%;border-collapse:collapse;font-size:.875rem}
  th{text-align:left;padding:.6rem .75rem;background:var(--surface);color:var(--muted);font-weight:600;font-size:.75rem;text-transform:uppercase;position:sticky;top:0}
  td{padding:.6rem .75rem;border-bottom:1px solid var(--border)}
  tr:hover td{background:var(--surface)}
  /* Badges */
  .badge{display:inline-block;padding:.1rem .5rem;border-radius:999px;font-size:.7rem;font-weight:600}
  .badge-green{background:#14532d;color:var(--green)}
  .badge-red{background:#450a0a;color:var(--red)}
  .badge-yellow{background:#451a03;color:var(--yellow)}
  .badge-blue{background:#172554;color:var(--blue)}
  /* Charts */
  .charts-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:1rem;margin-bottom:1.5rem}
  .chart-wrap{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.25rem;height:280px}
  .chart-wrap h3{font-size:.85rem;color:var(--muted);margin-bottom:.75rem;text-transform:uppercase;letter-spacing:.05em}
  /* Alert */
  .alert-box{background:#450a0a;border:1px solid var(--red);border-radius:8px;padding:1rem 1.25rem;margin-bottom:.5rem;color:var(--red)}
  /* Drift reasons */
  .reasons{font-size:.75rem;color:var(--muted);margin-top:.25rem;padding-left:.5rem;border-left:2px solid var(--red)}
  /* Search */
  .search-bar{width:100%;padding:.5rem .75rem;background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:.875rem;margin-bottom:.75rem}
  .search-bar:focus{outline:none;border-color:var(--blue)}
  /* Tags */
  .tag-added::before{content:"➕ "} .tag-removed::before{content:"➖ "}
  .tag-changed::before{content:"🔁 "} .tag-renamed::before{content:"↔ "}
  code{background:var(--border);padding:.1rem .4rem;border-radius:4px;font-size:.85em}
  .footer{margin-top:3rem;color:var(--muted);font-size:.75rem;text-align:center}
  .offline-badge{display:inline-block;padding:.1rem .4rem;border-radius:4px;background:#172554;color:var(--blue);font-size:.7rem;margin-left:.5rem}
</style>
</head>
<body>
<h1>🔍 difflake Report {% if offline %}<span class="offline-badge">offline</span>{% endif %}</h1>
<p class="subtitle">
  <code>{{ r.source_path }}</code> vs <code>{{ r.target_path }}</code>
  {% if r.elapsed_seconds %} · {{ "%.2f"|format(r.elapsed_seconds) }}s{% endif %}
  · Generated {{ generated_at }}
</p>

<!-- Summary Cards -->
<div class="grid">
  <div class="card">
    <div class="label">Source Rows</div>
    <div class="value">{{ "{:,}".format(r.row_diff.row_count_before) }}</div>
  </div>
  <div class="card">
    <div class="label">Target Rows</div>
    <div class="value">{{ "{:,}".format(r.row_diff.row_count_after) }}</div>
  </div>
  <div class="card">
    <div class="label">Row Δ</div>
    <div class="value {{ 'green' if r.row_diff.row_count_delta >= 0 else 'red' }}">
      {{ "%+d"|format(r.row_diff.row_count_delta) }}
    </div>
  </div>
  <div class="card">
    <div class="label">Schema Changes</div>
    <div class="value {{ 'red' if r.schema_diff.has_changes else 'green' }}">
      {{ "Yes" if r.schema_diff.has_changes else "None" }}
    </div>
  </div>
  <div class="card">
    <div class="label">Drifted Columns</div>
    <div class="value {{ 'red' if r.stats_diff.drifted_columns else 'green' }}">
      {{ r.stats_diff.drifted_columns|length }}
    </div>
  </div>
  <div class="card">
    <div class="label">Drift Alerts</div>
    <div class="value {{ 'red' if r.drift_alerts else 'green' }}">
      {{ r.drift_alerts|length }}
    </div>
  </div>
</div>

<!-- Drift Alerts -->
{% if r.drift_alerts %}
<details open>
  <summary>⚠️ Drift Alerts ({{ r.drift_alerts|length }})</summary>
  <div class="section-body">
    {% for alert in r.drift_alerts %}
    <div class="alert-box">🔴 {{ alert }}</div>
    {% endfor %}
  </div>
</details>
{% endif %}

<!-- Charts Row -->
{% if r.row_diff.key_based_diff or null_labels %}
<details open>
  <summary>📈 Charts</summary>
  <div class="section-body">
    <div class="charts-grid">
      {% if r.row_diff.key_based_diff %}
      <div class="chart-wrap">
        <h3>Row Composition</h3>
        <canvas id="rowChart"></canvas>
      </div>
      {% endif %}
      {% if null_labels %}
      <div class="chart-wrap">
        <h3>Null Rate — Before vs After (%)</h3>
        <canvas id="nullChart"></canvas>
      </div>
      {% endif %}
      {% if drift_labels %}
      <div class="chart-wrap">
        <h3>Mean Drift % (numeric columns)</h3>
        <canvas id="driftChart"></canvas>
      </div>
      {% endif %}
    </div>
  </div>
</details>
{% endif %}

<!-- Schema Diff -->
<details {% if r.schema_diff.has_changes %}open{% endif %}>
  <summary>📋 Schema Diff</summary>
  <div class="section-body">
  {% if not r.schema_diff.has_changes %}
  <p style="color:var(--green)">✅ No schema changes detected.</p>
  {% else %}
  <table>
    <thead><tr><th>Column</th><th>Change</th><th>Old Type</th><th>New Type</th><th>Note</th></tr></thead>
    <tbody>
    {% for col in r.schema_diff.added_columns %}
    <tr><td class="tag-added green">{{ col.name }}</td><td><span class="badge badge-green">added</span></td><td>—</td><td>{{ col.new_dtype }}</td><td></td></tr>
    {% endfor %}
    {% for col in r.schema_diff.removed_columns %}
    <tr><td class="tag-removed red">{{ col.name }}</td><td><span class="badge badge-red">removed</span></td><td>{{ col.old_dtype }}</td><td>—</td><td></td></tr>
    {% endfor %}
    {% for col in r.schema_diff.type_changed_columns %}
    <tr><td class="tag-changed yellow">{{ col.name }}</td><td><span class="badge badge-yellow">type changed</span></td><td>{{ col.old_dtype }}</td><td>{{ col.new_dtype }}</td><td></td></tr>
    {% endfor %}
    {% for col in r.schema_diff.renamed_columns %}
    <tr><td class="tag-renamed blue">{{ col.name }}</td><td><span class="badge badge-blue">renamed</span></td><td>{{ col.rename_from }}</td><td>{{ col.name }}</td><td>{{ "%.0f%%"|format(col.rename_similarity * 100) }} similar</td></tr>
    {% endfor %}
    </tbody>
  </table>
  {% endif %}
  </div>
</details>

<!-- Row Diff -->
<details open>
  <summary>🔢 Row Diff</summary>
  <div class="section-body">
  <table style="max-width:420px">
    <thead><tr><th>Metric</th><th>Value</th></tr></thead>
    <tbody>
    <tr><td>Rows Added</td><td class="green">+{{ "{:,}".format(r.row_diff.rows_added) }}</td></tr>
    <tr><td>Rows Removed</td><td class="red">-{{ "{:,}".format(r.row_diff.rows_removed) }}</td></tr>
    {% if r.row_diff.key_based_diff %}
    <tr><td>Rows Changed</td><td class="yellow">{{ "{:,}".format(r.row_diff.rows_changed) }}</td></tr>
    <tr><td>Rows Unchanged</td><td>{{ "{:,}".format(r.row_diff.rows_unchanged) }}</td></tr>
    <tr><td>Primary Key</td><td><code>{{ r.row_diff.primary_key_used }}</code></td></tr>
    {% else %}
    <tr><td colspan="2" style="color:var(--muted);font-size:.8rem;">ℹ️ No primary key — count diff only</td></tr>
    {% endif %}
    </tbody>
  </table>
  </div>
</details>

<!-- Stats Diff -->
<details open>
  <summary>📊 Statistical Diff
    {% if r.stats_diff.drifted_columns %}
    <span class="badge badge-red" style="margin-left:.5rem">{{ r.stats_diff.drifted_columns|length }} drifted</span>
    {% endif %}
  </summary>
  <div class="section-body">
  {% if r.stats_diff.column_diffs %}
  <input class="search-bar" id="statsSearch" type="text" placeholder="🔍 Filter columns…" oninput="filterStats(this.value)">
  <table id="statsTable">
    <thead>
      <tr>
        <th>Column</th><th>Type</th>
        <th>Null % Before</th><th>Null % After</th>
        <th>Mean Before</th><th>Mean After</th><th>Mean Drift</th>
        <th>Std Before</th><th>Std After</th>
        <th>Min</th><th>Max</th>
        <th>Card. Δ</th><th>KL Div</th>
        <th>Status</th>
      </tr>
    </thead>
    <tbody>
    {% for col in r.stats_diff.column_diffs %}
    <tr data-col="{{ col.column|lower }}">
      <td><strong>{{ col.column }}</strong>
        {% if col.drift_reasons %}
        <div class="reasons">{{ col.drift_reasons|join(" · ") }}</div>
        {% endif %}
        {% if col.new_categories %}
        <div style="font-size:.75rem;color:var(--green);margin-top:.2rem">+{{ col.new_categories[:5]|join(", ") }}</div>
        {% endif %}
        {% if col.dropped_categories %}
        <div style="font-size:.75rem;color:var(--red);margin-top:.2rem">-{{ col.dropped_categories[:5]|join(", ") }}</div>
        {% endif %}
      </td>
      <td style="color:var(--muted)">{{ col.dtype_category }}</td>
      <td>{% if col.null_rate_before is not none %}{{ "%.1f%%"|format(col.null_rate_before) }}{% else %}—{% endif %}</td>
      <td {% if col.null_rate_before is not none and col.null_rate_after is not none and (col.null_rate_after - col.null_rate_before)|abs > 15 %}class="red"{% endif %}>
        {% if col.null_rate_after is not none %}{{ "%.1f%%"|format(col.null_rate_after) }}{% else %}—{% endif %}
      </td>
      <td>{% if col.mean_before is not none %}{{ "%.4g"|format(col.mean_before) }}{% else %}—{% endif %}</td>
      <td>{% if col.mean_after is not none %}{{ "%.4g"|format(col.mean_after) }}{% else %}—{% endif %}</td>
      <td class="{{ 'red' if col.mean_drift_pct and (col.mean_drift_pct > 15 or col.mean_drift_pct < -15) else ('yellow' if col.mean_drift_pct and (col.mean_drift_pct > 5 or col.mean_drift_pct < -5) else '') }}">
        {% if col.mean_drift_pct is not none %}{{ "%+.2f%%"|format(col.mean_drift_pct) }}{% else %}—{% endif %}
      </td>
      <td>{% if col.std_before is not none %}{{ "%.4g"|format(col.std_before) }}{% else %}—{% endif %}</td>
      <td>{% if col.std_after is not none %}{{ "%.4g"|format(col.std_after) }}{% else %}—{% endif %}</td>
      <td style="font-size:.8rem;color:var(--muted)">
        {% if col.min_before is not none %}{{ "%.4g"|format(col.min_before) }}{% else %}—{% endif %}
      </td>
      <td style="font-size:.8rem;color:var(--muted)">
        {% if col.max_before is not none %}{{ "%.4g"|format(col.max_before) }}{% else %}—{% endif %}
      </td>
      <td>
        {% if col.cardinality_before is not none %}
          {{ "%+d"|format(col.cardinality_after - col.cardinality_before) }}
        {% else %}—{% endif %}
      </td>
      <td>{{ "%.3f"|format(col.kl_divergence) if col.kl_divergence is not none else "—" }}</td>
      <td>
        {% if col.is_drifted %}
          <span class="badge badge-red">🔴 drifted</span>
        {% else %}
          <span class="badge badge-green">✅ ok</span>
        {% endif %}
      </td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p style="color:var(--muted)">No stats computed.</p>
  {% endif %}
  </div>
</details>

<div class="footer">Generated by <strong>difflake</strong> · <em>git diff, but for your data</em></div>

<script>
// ── Column search filter ──────────────────────────────────────────────────
function filterStats(q) {
  q = q.toLowerCase();
  document.querySelectorAll('#statsTable tbody tr').forEach(function(tr) {
    tr.style.display = (!q || (tr.dataset.col||'').includes(q)) ? '' : 'none';
  });
}

// ── Charts ────────────────────────────────────────────────────────────────
{% if r.row_diff.key_based_diff %}
new Chart(document.getElementById('rowChart'), {
  type: 'doughnut',
  data: {
    labels: ['Unchanged', 'Changed', 'Added', 'Removed'],
    datasets: [{
      data: [{{ r.row_diff.rows_unchanged }}, {{ r.row_diff.rows_changed }},
             {{ r.row_diff.rows_added }}, {{ r.row_diff.rows_removed }}],
      backgroundColor: ['#334155', '#f59e0b', '#22c55e', '#ef4444'],
      borderWidth: 0,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { color: '#f1f5f9', font: { size: 11 } } } }
  }
});
{% endif %}

{% if null_labels %}
new Chart(document.getElementById('nullChart'), {
  type: 'bar',
  data: {
    labels: {{ null_labels|tojson }},
    datasets: [
      {
        label: 'Before %',
        data: {{ null_before|tojson }},
        backgroundColor: '#3b82f6aa',
        borderColor: '#3b82f6',
        borderWidth: 1,
      },
      {
        label: 'After %',
        data: {{ null_after|tojson }},
        backgroundColor: '#ef444499',
        borderColor: '#ef4444',
        borderWidth: 1,
      }
    ]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { color: '#f1f5f9', font: { size: 11 } } } },
    scales: {
      x: { ticks: { color: '#94a3b8', font: { size: 10 }, maxRotation: 45 }, grid: { color: '#334155' } },
      y: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' }, title: { display: true, text: '%', color: '#94a3b8' } }
    }
  }
});
{% endif %}

{% if drift_labels %}
new Chart(document.getElementById('driftChart'), {
  type: 'bar',
  data: {
    labels: {{ drift_labels|tojson }},
    datasets: [{
      label: 'Mean Drift %',
      data: {{ drift_values|tojson }},
      backgroundColor: {{ drift_colors|tojson }},
      borderWidth: 0,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: '#94a3b8', font: { size: 10 }, maxRotation: 45 }, grid: { color: '#334155' } },
      y: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' }, title: { display: true, text: '%', color: '#94a3b8' } }
    }
  }
});
{% endif %}
</script>
</body>
</html>
"""


def _build_chart_data(result: DiffResult) -> dict:
    """Pre-compute chart data arrays for the Jinja template."""
    col_diffs = result.stats_diff.column_diffs

    # Null rate chart — all columns with non-null null rates, capped at 20
    null_cols = [
        c for c in col_diffs
        if c.null_rate_before is not None and c.null_rate_after is not None
    ][:20]
    null_labels = [c.column for c in null_cols]
    null_before  = [round(c.null_rate_before or 0, 2) for c in null_cols]
    null_after   = [round(c.null_rate_after  or 0, 2) for c in null_cols]

    # Mean drift chart — numeric columns with drift %, capped at 20
    drift_cols = [
        c for c in col_diffs
        if c.mean_drift_pct is not None
        and c.dtype_category == "numeric"
        and abs(c.mean_drift_pct) < 1e9   # exclude inf
    ][:20]
    drift_labels = [c.column for c in drift_cols]
    drift_values = [round(c.mean_drift_pct or 0.0, 4) for c in drift_cols]
    drift_colors = [
        "#ef4444" if abs(v) > 15 else "#f59e0b" if abs(v) > 5 else "#22c55e"
        for v in drift_values
    ]

    return dict(
        null_labels=null_labels,
        null_before=null_before,
        null_after=null_after,
        drift_labels=drift_labels,
        drift_values=drift_values,
        drift_colors=drift_colors,
    )


class HtmlReporter:
    def __init__(self, result: DiffResult, offline: bool = True):
        self.result  = result
        self.offline = offline

    def write(self, path: str | Path) -> None:
        env = Environment(loader=BaseLoader(), autoescape=False)
        env.filters["abs"]     = abs
        env.filters["tojson"]  = _json.dumps
        env.filters["commafy"] = lambda v: f"{int(v):,}" if v is not None else "—"

        chart_data   = _build_chart_data(self.result)
        chartjs_tag  = _chartjs_tag(self.offline)
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        html = env.from_string(_TEMPLATE).render(
            r=self.result,
            offline=self.offline,
            chartjs_tag=chartjs_tag,
            generated_at=generated_at,
            **chart_data,
        )
        Path(path).write_text(html, encoding="utf-8")
