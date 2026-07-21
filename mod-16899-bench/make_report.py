#!/usr/bin/env python3
"""Generate a light MOD-16899 HTML report from results_before.json / results_after.json.
Structure: Setup (dataset/index/queries) -> Loader time -> Execution time -> Results table."""
import json, os, html

SCRATCH = os.path.dirname(os.path.abspath(__file__))
before = json.load(open(os.path.join(SCRATCH, "results_before.json")))
after = json.load(open(os.path.join(SCRATCH, "results_after.json")))

INDEX_DDL = """FT.CREATE idx:entity_events ON JSON PREFIX 1 entity: SCHEMA
  $.recordId    AS recordId    NUMERIC SORTABLE
  $.entityName  AS entityName  TEXT    SORTABLE
  $.event.id    AS eventId     NUMERIC SORTABLE
  $.event.type  AS type        TAG
  $.event.target    AS target      TAG SORTABLE
  $.event.hasNotes  AS hasNotes    TAG
  $.event.processed AS processed   TAG
  $.event.dueDate   AS dueDate     TEXT SORTABLE
  $.event.payload   AS payload     TEXT NOINDEX"""

QUERIES = [
    ("collect", "GROUPBY + COLLECT", "COLLECT",
     "FT.AGGREGATE idx:entity_events * LOAD 6 @entityName @eventId @type @target @processed @dueDate\n  GROUPBY 1 @entityName REDUCE COLLECT 18 FIELDS 5 @eventId @type @target @processed @dueDate\n  SORTBY 6 @target DESC @dueDate ASC @eventId ASC LIMIT 0 50 AS events\n  SORTBY 2 @entityName ASC LIMIT 0 50",
     "The ticket's query. GROUPBY accumulates every matched doc, so the loader runs over all 100K."),
    ("s1_noSort_offset", "Deep offset (no GROUPBY)", "OFFSET",
     "FT.AGGREGATE idx:entity_events * LOAD 2 @type @dueDate LIMIT 50000 100",
     "No GROUPBY. Loader is before the pager; the offset makes it load ~50K while only 100 return."),
    ("s2_sortby_bigReply", "SORTBY + large page", "SORTBY",
     "FT.AGGREGATE idx:entity_events * SORTBY 2 @eventId ASC LOAD 2 @type @dueDate LIMIT 0 10000",
     "No GROUPBY. Loader runs after the sort on the 10K-row page; reply serialization is part of the time."),
]

def spd(b, a): return b / a if a else float("nan")

rows = []
for key, name, short, cmd, desc in QUERIES:
    b, a = before[key], after[key]
    rows.append(dict(name=name, cmd=cmd, desc=desc, short=short,
                     loads=b["loader_count"],
                     lat_b=b["latency_ms"], lat_a=a["latency_ms"], lat_x=spd(b["latency_ms"], a["latency_ms"]),
                     ld_b=b["loader_ms"], ld_a=a["loader_ms"], ld_x=spd(b["loader_ms"], a["loader_ms"])))

def chart(get_b, get_a, unit="ms"):
    maxv = max(max(get_b(r), get_a(r)) for r in rows)
    CW, CH, PT, PB, PL = 720, 210, 16, 30, 46
    plot_h = CH - PT - PB
    gw = (CW - PL) / len(rows); bw = gw * 0.26
    def y(v): return PT + plot_h * (1 - v / maxv)
    out = []
    for frac in (0, .5, 1):
        gv = maxv * frac; gy = y(gv)
        out.append(f'<line x1="{PL}" y1="{gy:.1f}" x2="{CW}" y2="{gy:.1f}" stroke="var(--grid)"/>')
        out.append(f'<text x="{PL-6}" y="{gy+3:.1f}" text-anchor="end">{gv:.0f}</text>')
    for i, r in enumerate(rows):
        gx = PL + i * gw + gw * 0.5
        for j, (v, fill) in enumerate([(get_b(r), "var(--baseline)"), (get_a(r), "var(--s-hyblin)")]):
            x = gx - bw + j * (bw + 7); yy = y(v)
            out.append(f'<rect x="{x:.1f}" y="{yy:.1f}" width="{bw:.1f}" height="{PT+plot_h-yy:.1f}" rx="2" fill="{fill}"/>')
            out.append(f'<text x="{x+bw/2:.1f}" y="{yy-4:.1f}" text-anchor="middle" class="dlabel">{v:.0f}</text>')
        out.append(f'<text x="{gx:.1f}" y="{CH-PB+16:.1f}" text-anchor="middle">{html.escape(r["short"])}</text>')
    return f'<svg viewBox="0 0 {CW} {CH}" width="100%" role="img">{"".join(out)}</svg>'

legend = ('<div class="legend"><span class="li"><span class="sw" style="background:var(--baseline)"></span>before — string <code>get</code></span>'
          '<span class="li"><span class="sw" style="background:var(--s-hyblin)"></span>after — <code>getWithPath</code></span></div>')

def trow(r):
    return (f'<tr><td class="l">{html.escape(r["name"])}</td><td>{r["loads"]:,}</td>'
            f'<td>{r["lat_b"]:.0f}</td><td>{r["lat_a"]:.0f}</td><td class="status ok">{r["lat_x"]:.1f}×</td>'
            f'<td>{r["ld_b"]:.0f}</td><td>{r["ld_a"]:.0f}</td><td class="status ok">{r["ld_x"]:.1f}×</td></tr>')

qcards = "".join(
    f'<div class="q"><div class="qname">{html.escape(r["name"])}</div>'
    f'<pre>{html.escape(r["cmd"])}</pre><div class="note">{html.escape(r["desc"])}</div></div>' for r in rows)

CSS = """
.viz-root{--surface-1:#fcfcfb;--page:#f9f9f7;--ink-1:#0b0b0b;--ink-2:#52514e;--ink-muted:#898781;--grid:#e1e0d9;--baseline:#c3c2b7;--ring:rgba(11,11,11,.10);--s-hyblin:#2a78d6;--good:#0ca30c;--code:#f2f1ec;}
@media(prefers-color-scheme:dark){.viz-root{--surface-1:#1a1a19;--page:#0d0d0d;--ink-1:#fff;--ink-2:#c3c2b7;--ink-muted:#898781;--grid:#2c2c2a;--baseline:#383835;--ring:rgba(255,255,255,.10);--s-hyblin:#3987e5;--code:#111;}}
*{box-sizing:border-box}body.viz-root{margin:0;background:var(--page);color:var(--ink-1);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
.wrap{max-width:900px;margin:0 auto;padding:28px 20px 60px}
h1{font-size:22px;font-weight:650;margin:0 0 4px}.meta{color:var(--ink-muted);font-size:12px;margin-bottom:6px}
.card{background:var(--surface-1);border:1px solid var(--ring);border-radius:10px;padding:18px 20px 16px;margin:14px 0}
.card h2{font-size:15px;font-weight:650;margin:0 0 10px}
.kv{color:var(--ink-2);font-size:12.5px;margin:0 0 12px}.kv b{color:var(--ink-1)}
pre{background:var(--code);border:1px solid var(--ring);border-radius:8px;padding:10px 12px;margin:4px 0;overflow-x:auto;font:12px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--ink-1)}
.q{margin:0 0 14px}.q:last-child{margin-bottom:0}.qname{font-weight:600;font-size:13px;margin-bottom:2px}
.note{color:var(--ink-muted);font-size:11.5px;margin-top:3px}
.legend{display:flex;gap:16px;flex-wrap:wrap;margin:0 0 8px}.legend .li{display:inline-flex;align-items:center;gap:6px;color:var(--ink-2);font-size:12.5px}.legend .sw{width:12px;height:12px;border-radius:3px}
code{background:var(--code);border-radius:4px;padding:0 4px;font:12px ui-monospace,monospace}
svg text{font:11px system-ui,sans-serif;fill:var(--ink-muted);font-variant-numeric:tabular-nums}svg .dlabel{fill:var(--ink-2);font-weight:600}
table{border-collapse:collapse;width:100%;font-size:12.5px}th,td{text-align:right;padding:6px 10px;border-bottom:1px solid var(--grid);font-variant-numeric:tabular-nums}
th{color:var(--ink-2);font-weight:600;border-bottom:1px solid var(--baseline)}th.l,td.l{text-align:left}
.status.ok{color:var(--good);font-weight:600}
"""

HTML = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MOD-16899 — JSONPath compile cache</title><style>{CSS}</style></head>
<body class="viz-root"><div class="wrap">
<h1>MOD-16899 — cache the compiled JSONPath (initial numbers)</h1>
<div class="meta">oss-standalone · single client · exec latency = median of 250 runs · loader = median of 50 <code>FT.PROFILE</code> Threadsafe-Loader samples (mean/median drift &lt; 2.5%) · <b>before</b> = RedisJSON V8 (string <code>get</code>, recompiles the path per doc) · <b>after</b> = V9 <code>getWithPath</code> (compile once per query, reuse). Same redisearch binary; only the JSON module differs.</div>

<div class="card"><h2>Setup</h2>
<div class="kv"><b>Dataset:</b> 100,000 JSON docs, heavy skew by <code>entityName</code>, nested <code>$.event.*</code> object, 512-byte payload.</div>
<div class="kv"><b>Index:</b></div>
<pre>{html.escape(INDEX_DDL)}</pre>
<div class="kv" style="margin-top:14px"><b>Queries:</b></div>
{qcards}</div>

<div class="card"><h2>Loader time (ms) — the part the fix targets</h2>{legend}{chart(lambda r:r["ld_b"], lambda r:r["ld_a"])}</div>

<div class="card"><h2>Execution time — end-to-end query latency (ms)</h2>{legend}{chart(lambda r:r["lat_b"], lambda r:r["lat_a"])}</div>

<div class="card"><h2>Results</h2>
<table><thead><tr><th class="l">query</th><th>docs loaded</th><th>exec before</th><th>exec after</th><th>exec ×</th><th>loader before</th><th>loader after</th><th>loader ×</th></tr></thead>
<tbody>{''.join(trow(r) for r in rows)}</tbody></table></div>
</div></body></html>"""

out = os.path.join(SCRATCH, "mod16899_report.html")
open(out, "w").write(HTML)
print("wrote", out)
