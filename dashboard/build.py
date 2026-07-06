"""Build the calibration dashboard: query the marts, emit one static,
self-contained HTML file (inline SVG, inline CSS/JS, no external assets).

Run by the pipeline after `dbt build`; the output is published to GitHub
Pages. Locally:

    python dashboard/build.py            # writes site/index.html
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import os
from pathlib import Path

from google.cloud import bigquery

HORIZON_LABELS = {24: "1 day before resolution", 168: "1 week before", 720: "30 days before"}

# Chart geometry (SVG user units == px).
PLOT_W, PLOT_H = 300, 220   # calibration plot area
BARS_H = 44                 # sample-size strip under it
M_LEFT, M_TOP = 44, 18
M_RIGHT, M_BOT = 14, 34
GAP_PLOT_BARS = 26
SVG_W = M_LEFT + PLOT_W + M_RIGHT
SVG_H = M_TOP + PLOT_H + GAP_PLOT_BARS + BARS_H + M_BOT


def fetch(project: str, dataset: str):
    client = bigquery.Client(project=project)
    cal = [dict(r) for r in client.query(
        f"SELECT * FROM {dataset}.mart_calibration ORDER BY horizon_hours, price_bucket"
    ).result()]
    stats = dict(list(client.query(f"""
        SELECT
          (SELECT COUNT(*) FROM {dataset}.dim_markets)      AS markets,
          (SELECT COUNT(*) FROM {dataset}.fct_resolutions)  AS resolutions,
          (SELECT COUNT(*) FROM {dataset}.fct_prices)       AS prices,
          (SELECT MAX(ingested_at) FROM {dataset}.fct_prices) AS last_ingested
    """).result())[0])
    return cal, stats


def compact(n: float) -> str:
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= div:
            return f"{n / div:.1f}{suffix}"
    return f"{n:,.0f}"


def x_px(price: float) -> float:
    return M_LEFT + price * PLOT_W


def y_px(rate: float) -> float:
    return M_TOP + (1 - rate) * PLOT_H


def panel_svg(horizon: int, rows: list[dict]) -> str:
    """One small-multiple: calibration dots + Wilson CI whiskers + the
    perfect-calibration diagonal, with a sample-size bar strip beneath."""
    bars_top = M_TOP + PLOT_H + GAP_PLOT_BARS
    bars_base = bars_top + BARS_H
    max_n = max((r["n_markets"] for r in rows), default=1)

    parts = [
        f'<svg viewBox="0 0 {SVG_W} {SVG_H}" role="img" '
        f'aria-label="Calibration, {HORIZON_LABELS[horizon]}">'
    ]

    # Gridlines + tick labels (0 / 0.5 / 1 on both axes), hairline solid.
    for v in (0.0, 0.5, 1.0):
        gx, gy = x_px(v), y_px(v)
        parts.append(
            f'<line x1="{M_LEFT}" y1="{gy:.1f}" x2="{M_LEFT + PLOT_W}" y2="{gy:.1f}" '
            f'stroke="var(--grid)" stroke-width="1"/>'
            f'<line x1="{gx:.1f}" y1="{M_TOP}" x2="{gx:.1f}" y2="{M_TOP + PLOT_H}" '
            f'stroke="var(--grid)" stroke-width="1"/>'
            f'<text x="{M_LEFT - 7}" y="{gy + 4:.1f}" text-anchor="end" class="tick">{v:g}</text>'
            f'<text x="{gx:.1f}" y="{M_TOP + PLOT_H + 16}" text-anchor="middle" class="tick">{v:g}</text>'
        )
    # Perfect-calibration diagonal (reference, recessive).
    parts.append(
        f'<line x1="{x_px(0):.1f}" y1="{y_px(0):.1f}" x2="{x_px(1):.1f}" y2="{y_px(1):.1f}" '
        f'stroke="var(--baseline)" stroke-width="1"/>'
    )
    # Axis titles.
    parts.append(
        f'<text x="{M_LEFT + PLOT_W / 2:.0f}" y="{M_TOP + PLOT_H + 30}" '
        f'text-anchor="middle" class="axis">market price</text>'
        f'<text x="12" y="{M_TOP + PLOT_H / 2:.0f}" text-anchor="middle" class="axis" '
        f'transform="rotate(-90 12 {M_TOP + PLOT_H / 2:.0f})">observed frequency</text>'
    )

    # Sample-size bars: band per decile, ≤24px thick, 4px rounded top,
    # square at the baseline, 2px surface gap between neighbors.
    band = PLOT_W / 10
    bar_w = min(24.0, band - 2)
    for r in rows:
        b = r["price_bucket"]
        h = max(1.5, (r["n_markets"] / max_n) * (BARS_H - 4))
        x0 = M_LEFT + b * band + (band - bar_w) / 2
        y0 = bars_base - h
        rad = min(4.0, h)
        parts.append(
            f'<path d="M{x0:.1f},{bars_base:.1f} L{x0:.1f},{y0 + rad:.1f} '
            f'Q{x0:.1f},{y0:.1f} {x0 + rad:.1f},{y0:.1f} L{x0 + bar_w - rad:.1f},{y0:.1f} '
            f'Q{x0 + bar_w:.1f},{y0:.1f} {x0 + bar_w:.1f},{y0 + rad:.1f} '
            f'L{x0 + bar_w:.1f},{bars_base:.1f} Z" fill="var(--bars)"/>'
        )
    parts.append(
        f'<text x="{M_LEFT - 7}" y="{bars_base:.0f}" text-anchor="end" class="tick">n</text>'
        f'<line x1="{M_LEFT}" y1="{bars_base:.1f}" x2="{M_LEFT + PLOT_W}" y2="{bars_base:.1f}" '
        f'stroke="var(--baseline)" stroke-width="1"/>'
    )

    # CI whiskers + dots, with invisible enlarged hover targets carrying
    # the tooltip data.
    for r in rows:
        cx = x_px(r["avg_price"])
        cy = y_px(r["outcome_rate"])
        y_lo, y_hi = y_px(r["wilson_low"]), y_px(r["wilson_high"])
        lo_c, hi_c = int(r["price_bucket"]) * 10, int(r["price_bucket"]) * 10 + 10
        tip = (
            f"{lo_c}–{hi_c}¢ bucket · n={r['n_markets']}"
            f"|price {r['avg_price']:.3f} → resolved yes {r['outcome_rate']:.1%}"
            f"|95% CI [{r['wilson_low']:.2f}, {r['wilson_high']:.2f}]"
        )
        parts.append(
            f'<line x1="{cx:.1f}" y1="{y_lo:.1f}" x2="{cx:.1f}" y2="{y_hi:.1f}" '
            f'stroke="var(--series)" stroke-width="2" stroke-linecap="round" opacity="0.55"/>'
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="5" fill="var(--series)" '
            f'stroke="var(--surface)" stroke-width="2"/>'
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="13" fill="transparent" '
            f'class="hover-dot" data-tip="{html.escape(tip)}"/>'
        )

    parts.append("</svg>")
    return "".join(parts)


def build_page(cal: list[dict], stats: dict) -> str:
    horizons = sorted({r["horizon_hours"] for r in cal})
    updated = stats["last_ingested"].strftime("%Y-%m-%d %H:%M UTC")

    panels = []
    for hz in horizons:
        rows = [r for r in cal if r["horizon_hours"] == hz]
        n_total = sum(r["n_markets"] for r in rows)
        brier = sum(r["n_markets"] * r["brier_score"] for r in rows) / n_total
        panels.append(
            '<figure class="panel">'
            f"<figcaption><strong>{HORIZON_LABELS[hz]}</strong>"
            f'<span class="sub">Brier score {brier:.3f} &middot; {n_total:,} markets</span>'
            f"</figcaption>{panel_svg(hz, rows)}</figure>"
        )

    tiles = "".join(
        f'<div class="tile"><div class="tile-label">{label}</div>'
        f'<div class="tile-value">{value}</div></div>'
        for label, value in (
            ("Markets cataloged", compact(stats["markets"])),
            ("Resolved &amp; scored", compact(stats["resolutions"])),
            ("Price points", compact(stats["prices"])),
            ("Calibration sample", compact(sum(r["n_markets"] for r in cal))),
        )
    )

    table_rows = "".join(
        f"<tr><td>{r['horizon_hours']}h</td><td>{int(r['price_bucket']) * 10}–"
        f"{int(r['price_bucket']) * 10 + 10}¢</td><td>{r['n_markets']}</td>"
        f"<td>{r['avg_price']:.3f}</td><td>{r['outcome_rate']:.3f}</td>"
        f"<td>[{r['wilson_low']:.3f}, {r['wilson_high']:.3f}]</td>"
        f"<td>{r['brier_score']:.4f}</td></tr>"
        for r in cal
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Is Polymarket calibrated?</title>
<style>
:root {{
  --page: #f9f9f7; --surface: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e;
  --muted: #898781; --grid: #e1e0d9; --baseline: #c3c2b7;
  --series: #2a78d6; --bars: #86b6ef; --border: rgba(11,11,11,0.10);
}}
@media (prefers-color-scheme: dark) {{ :root {{
  --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7;
  --muted: #898781; --grid: #2c2c2a; --baseline: #383835;
  --series: #3987e5; --bars: #184f95; --border: rgba(255,255,255,0.10);
}} }}
:root[data-theme="dark"] {{
  --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7;
  --muted: #898781; --grid: #2c2c2a; --baseline: #383835;
  --series: #3987e5; --bars: #184f95; --border: rgba(255,255,255,0.10);
}}
:root[data-theme="light"] {{
  --page: #f9f9f7; --surface: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e;
  --muted: #898781; --grid: #e1e0d9; --baseline: #c3c2b7;
  --series: #2a78d6; --bars: #86b6ef; --border: rgba(11,11,11,0.10);
}}
* {{ box-sizing: border-box; margin: 0; }}
body {{ background: var(--page); color: var(--ink);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  max-width: 1180px; margin: 0 auto; padding: 32px 20px 48px; }}
header p {{ color: var(--ink-2); max-width: 62ch; margin-top: 6px; }}
.updated {{ color: var(--muted); font-size: 13px; margin-top: 4px; }}
.tiles {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 24px 0; }}
.tile {{ background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 14px 18px; min-width: 150px; }}
.tile-label {{ color: var(--ink-2); font-size: 13px; }}
.tile-value {{ font-size: 30px; font-weight: 600; margin-top: 2px; }}
.panels {{ display: flex; flex-wrap: wrap; gap: 16px; }}
.panel {{ background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 16px; flex: 1 1 320px; max-width: 420px; }}
.panel svg {{ width: 100%; height: auto; display: block; }}
figcaption strong {{ font-size: 15px; }}
figcaption .sub {{ display: block; color: var(--ink-2); font-size: 13px; margin: 2px 0 8px; }}
.tick {{ fill: var(--muted); font-size: 11px; }}
.axis {{ fill: var(--muted); font-size: 11.5px; }}
.note {{ color: var(--ink-2); max-width: 72ch; margin: 24px 0; font-size: 14px; }}
details {{ margin: 20px 0; }}
summary {{ cursor: pointer; color: var(--ink-2); }}
table {{ border-collapse: collapse; margin-top: 10px; font-size: 13.5px;
  font-variant-numeric: tabular-nums; }}
th, td {{ text-align: right; padding: 4px 12px; border-bottom: 1px solid var(--grid); }}
th {{ color: var(--ink-2); font-weight: 600; }}
td:first-child, th:first-child {{ text-align: left; }}
.table-wrap {{ overflow-x: auto; }}
#tip {{ position: fixed; display: none; background: var(--surface); color: var(--ink);
  border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px;
  font-size: 13px; pointer-events: none; box-shadow: 0 4px 14px rgba(0,0,0,0.12);
  z-index: 10; max-width: 260px; }}
footer {{ color: var(--muted); font-size: 13px; margin-top: 28px; }}
footer a {{ color: var(--ink-2); }}
</style>
</head>
<body>
<header>
  <h1>Is Polymarket calibrated?</h1>
  <p>When the market prices an outcome at 70&cent;, does it happen about 70% of
  the time? Each dot compares the market price at a fixed moment before
  resolution (x) with how often those markets actually resolved yes (y),
  across every resolved market above $10k volume. On the diagonal, the
  market was right; whiskers are 95% Wilson intervals; the bars underneath
  show how many markets sit in each price bucket.</p>
  <p class="updated">Data through {updated} &middot; refreshed twice daily by the
  <a href="https://github.com/D-O-G-E/polymarket-data-warehouse">pipeline</a>.</p>
</header>

<div class="tiles">{tiles}</div>

<div class="panels">{"".join(panels)}</div>

<p class="note">Prices are the pre-resolution CLOB midpoints of each market's
Yes token, captured hourly while markets trade (Polymarket prunes fine-grained
history after resolution, so this warehouse is the durable record). Fixed
horizons avoid the trivial calibration of near-certain last-minute prices.
Brier score is the mean squared error of price vs. outcome &mdash; lower is
better; 0.25 is the score of always guessing 50&cent;.</p>

<details>
  <summary>Data table</summary>
  <div class="table-wrap">
  <table>
    <thead><tr><th>Horizon</th><th>Bucket</th><th>n</th><th>Avg price</th>
    <th>Outcome rate</th><th>95% CI</th><th>Brier</th></tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  </div>
</details>

<footer>Built from the <a
href="https://github.com/D-O-G-E/polymarket-data-warehouse">polymarket-data-warehouse</a>
pipeline: Polymarket APIs &rarr; BigQuery &rarr; dbt &rarr; this page.</footer>

<div id="tip" role="tooltip"></div>
<script>
var tip = document.getElementById("tip");
document.querySelectorAll(".hover-dot").forEach(function (el) {{
  el.addEventListener("mouseenter", function () {{
    tip.innerHTML = el.dataset.tip.split("|").map(function (s) {{
      return "<div>" + s + "</div>";
    }}).join("");
    tip.style.display = "block";
  }});
  el.addEventListener("mousemove", function (e) {{
    var pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
    var x = e.clientX + pad, y = e.clientY + pad;
    if (x + w > window.innerWidth - 8) x = e.clientX - w - pad;
    if (y + h > window.innerHeight - 8) y = e.clientY - h - pad;
    tip.style.left = x + "px"; tip.style.top = y + "px";
  }});
  el.addEventListener("mouseleave", function () {{ tip.style.display = "none"; }});
}});
</script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=os.environ.get("PDW_BQ_PROJECT"))
    parser.add_argument("--dataset", default="polymarket_dw")
    parser.add_argument("--out", type=Path, default=Path("site/index.html"))
    args = parser.parse_args()
    if not args.project:
        raise SystemExit("pass --project or set PDW_BQ_PROJECT")

    cal, stats = fetch(args.project, args.dataset)
    page = build_page(cal, stats)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(page, encoding="utf-8")
    print(f"wrote {args.out} ({len(page):,} bytes; "
          f"{len(cal)} calibration rows as of {stats['last_ingested']:%Y-%m-%d %H:%M})")


if __name__ == "__main__":
    main()
