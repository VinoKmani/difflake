"""
HTML Reporter — generates a self-contained HTML diff report with Chart.js charts.
Uses inline CSS and JS so the file is fully portable (no external server needed).
"""

from __future__ import annotations

from pathlib import Path

from jinja2 import BaseLoader, Environment

from difflake.models import DiffResult

_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DiffLake Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --green: #22c55e; --red: #ef4444; --yellow: #f59e0b;
    --blue: #3b82f6; --gray: #6b7280; --bg: #0f172a; --surface: #1e293b;
    --border: #334155; --text: #f1f5f9; --muted: #94a3b8;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; padding: 2rem; }
  h1 { font-size: 1.8rem; margin-bottom: 0.25rem; }
  h2 { font-size: 1.2rem; color: var(--muted); margin: 2rem 0 1rem; border-bottom: 1px solid var(--border); padding-bottom: 0.5rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 1.25rem; }
  .card .label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
  .card .value { font-size: 1.6rem; font-weight: 700; margin-top: 0.25rem; }
  .green { color: var(--green); } .red { color: var(--red); } .yellow { color: var(--yellow); }
  table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
  th { text-align: left; padding: 0.6rem 0.75rem; background: var(--surface); color: var(--muted); font-weight: 600; font-size: 0.75rem; text-transform: uppercase; }
  td { padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--border); }
  tr:hover td { background: var(--surface); }
  .badge { display: inline-block; padding: 0.1rem 0.5rem; border-radius: 999px; font-size: 0.7rem; font-weight: 600; }
  .badge-green { background: #14532d; color: var(--green); }
  .badge-red { background: #450a0a; color: var(--red); }
  .badge-yellow { background: #451a03; color: var(--yellow); }
  .badge-blue { background: #172554; color: var(--blue); }
  .chart-wrap { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 1.5rem; max-width: 600px; height: 260px; }
  .alert-box { background: #450a0a; border: 1px solid var(--red); border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: 0.5rem; color: var(--red); }
  code { background: var(--border); padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.85em; }
  .tag-added::before { content: "➕ "; }
  .tag-removed::before { content: "➖ "; }
  .tag-changed::before { content: "🔁 "; }
  .tag-renamed::before { content: "↔ "; }
  .footer { margin-top: 3rem; color: var(--muted); font-size: 0.75rem; text-align: center; }
</style>
</head>
<body>
<h1>🔍 DiffLake Report</h1>
<p style="color:var(--muted); margin-bottom:1.5rem;">
  <code>{{ r.source_path }}</code> vs <code>{{ r.target_path }}</code>
  {% if r.elapsed_seconds %} · {{ "%.2f"|format(r.elapsed_seconds) }}s{% endif %}
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
<h2>⚠️ Drift Alerts</h2>
{% for alert in r.drift_alerts %}
<div class="alert-box">🔴 {{ alert }}</div>
{% endfor %}
{% endif %}

<!-- Schema Diff -->
<h2>📋 Schema Diff</h2>
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
  <tr><td class="tag-renamed" style="color:var(--blue)">{{ col.name }}</td><td><span class="badge badge-blue">renamed</span></td><td>{{ col.rename_from }}</td><td>{{ col.name }}</td><td>{{ "%.0f%%"|format(col.rename_similarity * 100) }} similar</td></tr>
  {% endfor %}
  </tbody>
</table>
{% endif %}

<!-- Row Diff -->
<h2>🔢 Row Diff</h2>
<div style="display:flex; gap:2rem; flex-wrap:wrap; align-items:flex-start;">
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
  <tr><td colspan="2" style="color:var(--muted); font-size:0.8rem;">ℹ️ No primary key used — count diff only</td></tr>
  {% endif %}
  </tbody>
</table>
{% if r.row_diff.key_based_diff %}
<div class="chart-wrap"><canvas id="rowChart"></canvas></div>
{% endif %}
</div>

<!-- Stats Diff -->
<h2>📊 Statistical Diff</h2>
{% if r.stats_diff.column_diffs %}
<table>
  <thead>
    <tr>
      <th>Column</th><th>Type</th>
      <th>Null % (before → after)</th>
      <th>Mean Drift</th>
      <th>Cardinality Δ</th>
      <th>KL Div</th>
      <th>Status</th>
    </tr>
  </thead>
  <tbody>
  {% for col in r.stats_diff.column_diffs %}
  <tr>
    <td><strong>{{ col.column }}</strong></td>
    <td style="color:var(--muted)">{{ col.dtype_category }}</td>
    <td>
      {% if col.null_rate_before is not none %}
        {{ "%.1f%%"|format(col.null_rate_before) }} → {{ "%.1f%%"|format(col.null_rate_after) }}
      {% else %}—{% endif %}
    </td>
    <td class="{{ 'red' if col.mean_drift_pct and (col.mean_drift_pct > 15 or col.mean_drift_pct < -15) else ('yellow' if col.mean_drift_pct and (col.mean_drift_pct > 5 or col.mean_drift_pct < -5) else '') }}">
      {% if col.mean_drift_pct is not none %}
        {{ "%+.2f%%"|format(col.mean_drift_pct) }}
      {% else %}—{% endif %}
    </td>
    <td>
      {% if col.cardinality_before is not none %}
        {{ col.cardinality_before }} → {{ col.cardinality_after }}
        ({{ "%+d"|format(col.cardinality_after - col.cardinality_before) }})
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
  {% if col.new_categories or col.dropped_categories %}
  <tr style="background:var(--surface)">
    <td colspan="7" style="font-size:0.8rem; color:var(--muted); padding-left:1.5rem;">
      {% if col.new_categories %}<span class="green">+new: {{ col.new_categories[:8]|join(", ") }}</span>  {% endif %}
      {% if col.dropped_categories %}<span class="red">-dropped: {{ col.dropped_categories[:8]|join(", ") }}</span>{% endif %}
    </td>
  </tr>
  {% endif %}
  {% endfor %}
  </tbody>
</table>
{% else %}
<p style="color:var(--muted)">No stats computed.</p>
{% endif %}

<div class="footer">Generated by <strong>difflake</strong> · <em>git diff, but for your data</em></div>

<script>
{% if r.row_diff.key_based_diff %}
new Chart(document.getElementById('rowChart'), {
  type: 'doughnut',
  data: {
    labels: ['Unchanged', 'Changed', 'Added', 'Removed'],
    datasets: [{
      data: [{{ r.row_diff.rows_unchanged }}, {{ r.row_diff.rows_changed }}, {{ r.row_diff.rows_added }}, {{ r.row_diff.rows_removed }}],
      backgroundColor: ['#334155', '#f59e0b', '#22c55e', '#ef4444'],
      borderWidth: 0,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { color: '#f1f5f9', font: { size: 12 } } } }
  }
});
{% endif %}
</script>
</body>
</html>
"""


class HtmlReporter:
    def __init__(self, result: DiffResult):
        self.result = result

    def write(self, path: str | Path) -> None:
        env = Environment(loader=BaseLoader(), autoescape=False)
        env.filters["abs"] = abs
        env.filters["commafy"] = lambda v: f"{int(v):,}" if v is not None else "—"
        template = env.from_string(_TEMPLATE)
        html = template.render(r=self.result)
        Path(path).write_text(html, encoding="utf-8")
