"""Generate mod12930_report.html from results.json (self-contained, no external deps)."""

import json

with open("results.json") as f:
    data = json.load(f)

payload = json.dumps(
    {"meta": data["meta"], "results": data["results"], "gates": data["gates"]}
)
profiles_pretty = json.dumps(data.get("profiles", {}), indent=1)[:200_000]

html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MOD-12930 — FT.HYBRID Fair Benchmark</title>
<style>
.viz-root {
  --surface-1: #fcfcfb; --page: #f9f9f7;
  --ink-1: #0b0b0b; --ink-2: #52514e; --ink-muted: #898781;
  --grid: #e1e0d9; --baseline: #c3c2b7; --ring: rgba(11,11,11,0.10);
  --s-hyblin: #2a78d6; --s-hybrrf: #1baf7a; --s-search: #eda100;
  --s-vsim: #4a3aa7; --s-rerank: #e34948;
  --good: #0ca30c; --critical: #d03b3b;
}
@media (prefers-color-scheme: dark) {
  .viz-root {
    --surface-1: #1a1a19; --page: #0d0d0d;
    --ink-1: #ffffff; --ink-2: #c3c2b7; --ink-muted: #898781;
    --grid: #2c2c2a; --baseline: #383835; --ring: rgba(255,255,255,0.10);
    --s-hyblin: #3987e5; --s-hybrrf: #199e70; --s-search: #c98500;
    --s-vsim: #9085e9; --s-rerank: #e66767;
  }
}
* { box-sizing: border-box; }
body.viz-root {
  margin: 0; background: var(--page); color: var(--ink-1);
  font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif;
}
.wrap { max-width: 1120px; margin: 0 auto; padding: 28px 20px 60px; }
h1 { font-size: 22px; font-weight: 650; margin: 0 0 4px; }
.meta { color: var(--ink-muted); font-size: 12px; margin-bottom: 20px; }
.meta code { font: 12px ui-monospace, monospace; }
.card {
  background: var(--surface-1); border: 1px solid var(--ring); border-radius: 10px;
  padding: 18px 20px 14px; margin: 14px 0;
}
.card h2 { font-size: 15px; font-weight: 650; margin: 0 0 2px; }
.card .sub { color: var(--ink-2); font-size: 12.5px; margin: 0 0 12px; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin: 14px 0; }
.tile { background: var(--surface-1); border: 1px solid var(--ring); border-radius: 10px; padding: 14px 16px; }
.tile .lbl { color: var(--ink-2); font-size: 12.5px; }
.tile .val { font-size: 30px; font-weight: 650; margin-top: 2px; }
.tile .note { color: var(--ink-muted); font-size: 11.5px; margin-top: 2px; }
.filters { display: flex; gap: 18px; align-items: center; margin: 18px 0 6px; flex-wrap: wrap; }
.filters .flabel { color: var(--ink-2); font-size: 12.5px; }
.seg { display: inline-flex; border: 1px solid var(--ring); border-radius: 8px; overflow: hidden; }
.seg button {
  border: 0; background: transparent; color: var(--ink-2); padding: 6px 14px;
  font: inherit; font-size: 13px; cursor: pointer;
}
.seg button.on { background: var(--ink-1); color: var(--surface-1); font-weight: 600; }
.legend { display: flex; gap: 16px; flex-wrap: wrap; margin: 4px 0 10px; }
.legend .li { display: inline-flex; align-items: center; gap: 6px; color: var(--ink-2); font-size: 12.5px; }
.legend .sw { width: 12px; height: 12px; border-radius: 3px; }
.panels { display: flex; gap: 18px; flex-wrap: wrap; }
.panel { flex: 1 1 300px; min-width: 280px; }
.panel .ptitle { text-align: center; color: var(--ink-2); font-size: 12.5px; margin-bottom: 2px; }
.panel .pmatch { text-align: center; color: var(--ink-muted); font-size: 11px; margin-bottom: 6px; }
svg text { font: 11px system-ui, sans-serif; fill: var(--ink-muted); font-variant-numeric: tabular-nums; }
svg .dlabel { fill: var(--ink-2); font-weight: 600; }
.bar { transition: filter .08s; cursor: default; }
.bar:hover, .bar.focus { filter: brightness(1.12); }
#tooltip {
  position: fixed; pointer-events: none; z-index: 10; display: none;
  background: var(--surface-1); border: 1px solid var(--ring); border-radius: 8px;
  box-shadow: 0 4px 14px rgba(0,0,0,.14); padding: 8px 11px; font-size: 12.5px; max-width: 300px;
}
#tooltip .tv { font-weight: 650; font-size: 14px; color: var(--ink-1); }
#tooltip .tk { display: inline-block; width: 12px; height: 3px; border-radius: 2px; margin-right: 6px; vertical-align: middle; }
#tooltip .tr { color: var(--ink-2); margin-top: 2px; }
table { border-collapse: collapse; width: 100%; font-size: 12.5px; }
th, td { text-align: right; padding: 6px 10px; border-bottom: 1px solid var(--grid); font-variant-numeric: tabular-nums; }
th { color: var(--ink-2); font-weight: 600; border-bottom: 1px solid var(--baseline); }
th:first-child, td:first-child, th.l, td.l { text-align: left; }
.status { font-weight: 600; }
.status.ok { color: var(--good); } .status.bad { color: var(--critical); }
details { margin-top: 10px; }
details pre { max-height: 420px; overflow: auto; font-size: 11px; background: var(--page); padding: 10px; border-radius: 8px; }
.footnote { color: var(--ink-muted); font-size: 11.5px; margin-top: 8px; }
</style>
</head>
<body class="viz-root">
<div class="wrap">
  <h1>MOD-12930 — Fair FT.HYBRID benchmark</h1>
  <div class="meta" id="meta"></div>
  <div class="tiles" id="tiles"></div>

  <div class="filters">
    <span class="flabel">Workers</span><span class="seg" id="f-workers"></span>
    <span class="flabel">Fields</span><span class="seg" id="f-fields"></span>
    <span class="flabel">Metric</span><span class="seg" id="f-metric"></span>
  </div>

  <div class="card">
    <h2 id="c1-title"></h2>
    <p class="sub">Grouped by dataset size, one panel per text-selectivity class (per-panel scale). Measured |text matches| shown under each panel.</p>
    <div class="legend" id="legend1"></div>
    <div class="panels" id="chart-main"></div>
  </div>

  <div class="card">
    <h2 id="c2-title">Degradation factor</h2>
    <p class="sub" id="c2-sub">How many times slower each contender gets vs the smallest corpus, at p50 and p90 (single QPS-ratio group when the QPS metric is selected). Hairline at 1× = no degradation.</p>
    <div class="panels" id="chart-ratio"></div>
  </div>

  <div class="card">
    <h2>Accounting identity — FT.HYBRID vs its branches</h2>
    <p class="sub">Mean latency. The stacked column is search-branch + vsim-branch run standalone; ε (orchestration overhead) = FT.HYBRID minus the slowest branch (the tall segment). The stack's total shows the sequential-depletion bound.</p>
    <div class="legend" id="legend3"></div>
    <div class="panels" id="chart-identity"></div>
  </div>

  <div class="card">
    <h2>Gates &amp; semantics</h2>
    <p class="sub">Equivalence gates (FT.HYBRID vs the untimed two-query oracle), tie-aware. A failed gate voids that configuration's numbers.</p>
    <table id="gates-table"></table>
  </div>

  <div class="card">
    <h2>All results</h2>
    <p class="sub">The full table view — every number reachable without hover.</p>
    <table id="results-table"></table>
    <details><summary>FT.PROFILE captures (hybrid LINEAR, workers=0)</summary><pre id="profiles"></pre></details>
  </div>
</div>
<div id="tooltip"></div>

<script id="data" type="application/json">__PAYLOAD__</script>
<script id="profdata" type="application/json">__PROFILES__</script>
<script>
"use strict";
const DATA = JSON.parse(document.getElementById("data").textContent);
const CONTENDERS = [
  { key: "hybrid_linear",    name: "FT.HYBRID (LINEAR)", v: "--s-hyblin" },
  { key: "hybrid_rrf",       name: "FT.HYBRID (RRF)",    v: "--s-hybrrf" },
  { key: "search_branch",    name: "SEARCH branch (FT.SEARCH top-W)", v: "--s-search" },
  { key: "vsim_branch",      name: "VSIM branch",        v: "--s-vsim" },
];
const SELS = ["selective", "medium", "broad"];
const SIZES = [...new Set(DATA.results.map(r => r.size))].sort((a, b) => a - b);
const WORKERS = [...new Set(DATA.results.map(r => r.workers))].sort((a, b) => a - b);
const METRICS = [
  { key: "p50_ms", name: "p50 latency (ms)", better: "lower" },
  { key: "qps",    name: "throughput (QPS)", better: "higher" },
];
const css = v => getComputedStyle(document.body).getPropertyValue(v).trim();
const FIELDS = [...new Set(DATA.results.map(r => r.fields))].filter(Boolean).sort();
const state = { workers: WORKERS[0], metric: "p50_ms", fields: FIELDS[0] || undefined };

const rows = (f) => DATA.results.filter(r =>
  Object.entries(f).every(([k, v]) => r[k] === v));
const row1 = (f) => rows(f)[0];
const fmt = (x, d) => x >= 1000 ? Math.round(x).toLocaleString("en-US")
  : x.toFixed(d !== undefined ? d : (x >= 100 ? 0 : x >= 10 ? 1 : 2));

/* ---------- header meta + tiles ---------- */
(function () {
  const m = DATA.meta;
  const el = document.getElementById("meta");
  const depthStr = m.depth
    ? Object.entries(m.depth).map(([s, d]) => `${s} ${d.k}/${d.window}`).join(" · ")
    : `K=${m.k} WINDOW=${m.window}`;
  el.appendChild(document.createTextNode(
    `module ${String(m.git_sha).slice(0, 10)} · redis ${m.redis_version} · dim=${m.dim} · output top-${m.out_k || 10} · ` +
    `K/WINDOW per selectivity: ${depthStr} · ` +
    `${m.n_query_set} distinct queries, ${m.n_timed} timed reps/cell (after ${m.n_warmup} warm-up)`));

  function tile(label, value, note) {
    const t = document.createElement("div"); t.className = "tile";
    const l = document.createElement("div"); l.className = "lbl"; l.textContent = label;
    const v = document.createElement("div"); v.className = "val"; v.textContent = value;
    const n = document.createElement("div"); n.className = "note"; n.textContent = note;
    t.append(l, v, n); return t;
  }
  const big = SIZES[SIZES.length - 1], small = SIZES[0];
  const tiles = document.getElementById("tiles");
  const cell = (c, sel, size, f) => row1({ contender: c, selectivity: sel, size, workers: 0, fields: f || (FIELDS[0] || undefined) });
  const hb = cell("hybrid_linear", "broad", big), hs = cell("hybrid_linear", "broad", small);
  const hbSel = cell("hybrid_linear", "selective", big), hsSel = cell("hybrid_linear", "selective", small);
  const sb = cell("search_branch", "broad", big), vb = cell("vsim_branch", "broad", big);
  const span = `${sizeName(small).replace(" docs", "")}→${sizeName(big).replace(" docs", "")}`;
  if (hb && hs) tiles.appendChild(tile(`FT.HYBRID broad query, ${span}`,
    "×" + fmt(hb.p50_ms / hs.p50_ms, 1), "p50 degradation on the PERF-473-style all-matching query"));
  if (hbSel && hsSel) tiles.appendChild(tile(`FT.HYBRID selective query, ${span}`,
    "×" + fmt(hbSel.p50_ms / hsSel.p50_ms, 1), "same command, selective text — the corpus-size effect alone"));
  if (hb && sb && vb) {
    const eps = hb.mean_ms - Math.max(sb.mean_ms, vb.mean_ms);
    tiles.appendChild(tile("ε — hybrid machinery overhead",
      fmt(100 * eps / hb.mean_ms, 1) + "%",
      "vs slowest branch (broad, 100K, workers=0). Three components: deep-window fusion in the merger (scales with WINDOW), the O(N) YIELD_SCORE_AS write, and the sequential 2nd branch at workers=0"));
  }
  const hbF = cell("hybrid_linear", "broad", big, "title+text"), sbF = cell("search_branch", "broad", big, "title+text");
  if (hbF && hb && sbF && sb) tiles.appendChild(tile("LOAD amplification (hybrid)",
    "+" + fmt(hbF.mean_ms - hb.mean_ms, 1) + " ms",
    `fields Δ per query at broad/${sizeName(big)}: hybrid loads WINDOW+K docs pre-fusion vs +${fmt(sbF.mean_ms - sb.mean_ms, 2)} ms for the mirror's 10`));
})();

/* ---------- filters ---------- */
function segControl(el, options, get, set) {
  options.forEach(o => {
    const b = document.createElement("button");
    b.textContent = o.name; b.dataset.v = o.value;
    b.addEventListener("click", () => { set(o.value); render(); });
    el.appendChild(b);
  });
  return () => { [...el.children].forEach(b =>
    b.classList.toggle("on", String(get()) === b.dataset.v)); };
}
const syncW = segControl(document.getElementById("f-workers"),
  WORKERS.map(w => ({ name: String(w), value: String(w) })),
  () => state.workers, v => state.workers = Number(v));
const syncF = FIELDS.length ? segControl(document.getElementById("f-fields"),
  FIELDS.map(f => ({ name: f, value: f })),
  () => state.fields, v => state.fields = v) : () => {};
const syncM = segControl(document.getElementById("f-metric"),
  METRICS.map(m => ({ name: m.name, value: m.key })),
  () => state.metric, v => state.metric = v);

/* ---------- tooltip ---------- */
const tip = document.getElementById("tooltip");
function showTip(evt, lines) {
  tip.textContent = "";
  lines.forEach((ln, i) => {
    const d = document.createElement("div");
    d.className = i === 0 ? "tv" : "tr";
    if (ln.color) {
      const k = document.createElement("span"); k.className = "tk";
      k.style.background = ln.color; d.appendChild(k);
    }
    d.appendChild(document.createTextNode(ln.text));
    tip.appendChild(d);
  });
  tip.style.display = "block";
  const pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
  let x = evt.clientX + pad, y = evt.clientY + pad;
  if (x + w > innerWidth - 8) x = evt.clientX - w - pad;
  if (y + h > innerHeight - 8) y = evt.clientY - h - pad;
  tip.style.left = x + "px"; tip.style.top = y + "px";
}
function hideTip() { tip.style.display = "none"; }

/* ---------- generic grouped-column panel ---------- */
const SVGNS = "http://www.w3.org/2000/svg";
function el(n, attrs) {
  const e = document.createElementNS(SVGNS, n);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  return e;
}
function niceTicks(max) {
  // Top gridline must be >= the data max (never let marks overshoot the scale).
  const raw = max / 4, mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map(s => s * mag).find(s => max / s <= 5) || 10 * mag;
  const top = Math.ceil(max / step - 1e-9) * step;
  const out = []; for (let v = 0; v <= top + step / 2; v += step) out.push(v);
  return out;
}
// groups: [{label, cols: [{value, color, stack?, tooltip: [lines], dlabel?}]}]
function columnPanel(groups, opts) {
  const W = opts.width || 320, plotH = 190, axB = 22, axT = 12, axL = 46, axR = 6;
  const H = plotH + axB + axT;
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: "100%", role: "img" });
  const maxV = Math.max(...groups.flatMap(g => g.cols.map(c =>
    c.stack ? c.stack.reduce((a, s) => a + s.value, 0) : c.value)), 1e-9);
  const ticks = niceTicks(maxV);
  const yMax = ticks[ticks.length - 1] * 1.02;
  const y = v => axT + plotH - (v / yMax) * plotH;
  ticks.forEach(t => {
    svg.appendChild(el("line", { x1: axL, x2: W - axR, y1: y(t), y2: y(t),
      stroke: t === 0 ? css("--baseline") : css("--grid"), "stroke-width": 1 }));
    const tx = el("text", { x: axL - 6, y: y(t) + 3.5, "text-anchor": "end" });
    tx.textContent = fmt(t, t >= 10 ? 0 : undefined);
    svg.appendChild(tx);
  });
  if (opts.refLine !== undefined && opts.refLine <= yMax) {
    svg.appendChild(el("line", { x1: axL, x2: W - axR, y1: y(opts.refLine), y2: y(opts.refLine),
      stroke: css("--baseline"), "stroke-width": 1 }));
  }
  const gw = (W - axL - axR) / groups.length;
  const barW = Math.min(24, (gw - 16) / Math.max(...groups.map(g => g.cols.length)) - 2);
  groups.forEach((g, gi) => {
    const n = g.cols.length, total = n * barW + (n - 1) * 2;
    let x = axL + gi * gw + (gw - total) / 2;
    const lx = el("text", { x: axL + gi * gw + gw / 2, y: H - 6, "text-anchor": "middle" });
    lx.textContent = g.label; svg.appendChild(lx);
    g.cols.forEach(c => {
      const segs = c.stack || [{ value: c.value, color: c.color }];
      let acc = 0;
      segs.forEach((s, si) => {
        const y0 = y(acc), y1 = y(acc + s.value);
        const hpx = Math.max(y0 - y1 - (si < segs.length - 1 ? 2 : 0), 0.5);
        const topR = si === segs.length - 1 ? 4 : 0;
        const yTop = y1;
        const p = el("path", { class: "bar", fill: s.color, d:
          `M${x},${y0} L${x},${yTop + topR} Q${x},${yTop} ${x + topR},${yTop} ` +
          `L${x + barW - topR},${yTop} Q${x + barW},${yTop} ${x + barW},${yTop + topR} L${x + barW},${y0} Z` });
        p.setAttribute("tabindex", "0");
        const lines = c.tooltip;
        p.addEventListener("pointermove", e => showTip(e, lines));
        p.addEventListener("pointerleave", hideTip);
        p.addEventListener("focus", e => {
          const r = p.getBoundingClientRect();
          showTip({ clientX: r.right, clientY: r.top }, lines);
          p.classList.add("focus");
        });
        p.addEventListener("blur", () => { hideTip(); p.classList.remove("focus"); });
        svg.appendChild(p);
        acc += s.value;
        void hpx;
      });
      if (c.dlabel) {
        const t = el("text", { x: x + barW / 2, y: Math.max(y(acc) - 4, 9),
          "text-anchor": "middle", class: "dlabel" });
        t.textContent = c.dlabel; svg.appendChild(t);
      }
      x += barW + 2;
    });
  });
  return svg;
}
function panelBox(container, title, sub) {
  const p = document.createElement("div"); p.className = "panel";
  const t = document.createElement("div"); t.className = "ptitle"; t.textContent = title;
  p.appendChild(t);
  if (sub) { const s = document.createElement("div"); s.className = "pmatch"; s.textContent = sub; p.appendChild(s); }
  container.appendChild(p); return p;
}
function legend(elId, items) {
  const box = document.getElementById(elId); box.textContent = "";
  items.forEach(it => {
    const li = document.createElement("span"); li.className = "li";
    const sw = document.createElement("span"); sw.className = "sw"; sw.style.background = css(it.v);
    li.append(sw, document.createTextNode(it.name)); box.appendChild(li);
  });
}

/* ---------- charts ---------- */
function sizeName(s) { return s >= 1000 ? (s / 1000) + "K docs" : s + " docs"; }
function matchNote(sel, size) {
  const g = DATA.gates.find(x => x.selectivity === sel && x.size === size);
  const d = DATA.meta.depth && DATA.meta.depth[sel];
  const dep = d ? ` · K/W ${d.k}/${d.window}` : "";
  return g ? `|matches|≈${fmt(g.matches_mean, 0)} @${sizeName(size)}${size === SIZES[0] ? dep : ""}` : "";
}

function renderMain() {
  const metric = METRICS.find(m => m.key === state.metric);
  document.getElementById("c1-title").textContent =
    `${metric.name} by contender — workers=${state.workers} (${metric.better} is better)`;
  legend("legend1", CONTENDERS);
  const box = document.getElementById("chart-main"); box.textContent = "";
  SELS.forEach(sel => {
    const p = panelBox(box, sel, SIZES.map(s => matchNote(sel, s)).join(" · "));
    const groups = SIZES.map(size => ({
      label: sizeName(size),
      cols: CONTENDERS.map(c => {
        const r = row1({ contender: c.key, selectivity: sel, size, workers: state.workers, fields: state.fields });
        if (!r) return { value: 0, color: css(c.v), tooltip: [{ text: "no data" }] };
        return {
          value: r[state.metric], color: css(c.v),
          dlabel: c.key === "hybrid_linear" ? fmt(r[state.metric]) : null,
          tooltip: [
            { text: `${fmt(r[state.metric])} ${state.metric === "qps" ? "qps" : "ms"}` },
            { text: c.name, color: css(c.v) },
            { text: `${sel} · ${sizeName(size)} · workers=${state.workers}` },
            { text: `p50 ${fmt(r.p50_ms)} · p90 ${fmt(r.p90_ms)} · p99 ${fmt(r.p99_ms)} ms` },
          ],
        };
      }),
    }));
    p.appendChild(columnPanel(groups, {}));
  });
}

function renderRatio() {
  const box = document.getElementById("chart-ratio"); box.textContent = "";
  const small = SIZES[0], targets = SIZES.slice(1);
  document.getElementById("c2-title").textContent =
    `Degradation factor vs ${sizeName(small)}`;
  const specs = state.metric === "qps"
    ? [{ label: "QPS", ratio: (a, b) => a.qps / b.qps }]
    : [{ label: "p50", ratio: (a, b) => b.p50_ms / a.p50_ms },
       { label: "p90", ratio: (a, b) => b.p90_ms / a.p90_ms }];
  SELS.forEach(sel => {
    const p = panelBox(box, sel, "");
    const groups = [];
    targets.forEach(size => specs.forEach(sp => groups.push({
      label: `${sizeName(size).replace(" docs", "")} ${sp.label}`,
      cols: CONTENDERS.map(c => {
        const a = row1({ contender: c.key, selectivity: sel, size: small, workers: state.workers, fields: state.fields });
        const b = row1({ contender: c.key, selectivity: sel, size, workers: state.workers, fields: state.fields });
        if (!a || !b) return { value: 0, color: css(c.v), tooltip: [{ text: "no data" }] };
        const ratio = sp.ratio(a, b);
        return {
          value: ratio, color: css(c.v),
          dlabel: c.key === "hybrid_linear" ? "×" + fmt(ratio, 1) : null,
          tooltip: [
            { text: `×${fmt(ratio, 2)} slower at ${sizeName(size)} (${sp.label})` },
            { text: c.name, color: css(c.v) },
            { text: `${sel} · workers=${state.workers} · vs ${sizeName(small)}` },
          ],
        };
      }),
    })));
    p.appendChild(columnPanel(groups, { refLine: 1, width: groups.length > 2 ? 480 : 320 }));
  });
}

function renderIdentity() {
  legend("legend3", [CONTENDERS[0], CONTENDERS[2], CONTENDERS[3]]);
  const box = document.getElementById("chart-identity"); box.textContent = "";
  SELS.forEach(sel => {
    const p = panelBox(box, sel, "");
    const groups = SIZES.map(size => {
      const hy = row1({ contender: "hybrid_linear", selectivity: sel, size, workers: state.workers, fields: state.fields });
      const se = row1({ contender: "search_branch", selectivity: sel, size, workers: state.workers, fields: state.fields });
      const vs = row1({ contender: "vsim_branch", selectivity: sel, size, workers: state.workers, fields: state.fields });
      if (!hy || !se || !vs) return { label: sizeName(size), cols: [] };
      const eps = hy.mean_ms - Math.max(se.mean_ms, vs.mean_ms);
      const common = `${sel} · ${sizeName(size)} · workers=${state.workers} · mean ms`;
      return {
        label: sizeName(size),
        cols: [
          { value: hy.mean_ms, color: css("--s-hyblin"),
            dlabel: fmt(hy.mean_ms),
            tooltip: [{ text: `${fmt(hy.mean_ms)} ms mean` },
                      { text: "FT.HYBRID (LINEAR)", color: css("--s-hyblin") },
                      { text: common },
                      { text: `ε vs slowest branch: ${fmt(100 * eps / hy.mean_ms, 1)}%` }] },
          { stack: [
              { value: se.mean_ms, color: css("--s-search") },
              { value: vs.mean_ms, color: css("--s-vsim") },
            ],
            tooltip: [{ text: `${fmt(se.mean_ms + vs.mean_ms)} ms summed` },
                      { text: `SEARCH branch ${fmt(se.mean_ms)} ms`, color: css("--s-search") },
                      { text: `VSIM branch ${fmt(vs.mean_ms)} ms`, color: css("--s-vsim") },
                      { text: common }] },
        ],
      };
    });
    p.appendChild(columnPanel(groups, {}));
  });
}

/* ---------- tables ---------- */
function th(tr, txt, cls) { const c = document.createElement("th"); if (cls) c.className = cls; c.textContent = txt; tr.appendChild(c); }
function td(tr, txt, cls) { const c = document.createElement("td"); if (cls) c.className = cls; c.textContent = txt; tr.appendChild(c); return c; }

(function gatesTable() {
  const t = document.getElementById("gates-table");
  const hr = document.createElement("tr");
  ["size", "selectivity", "|matches| mean", "gate LINEAR", "gate RRF"].forEach((h, i) => th(hr, h, i < 2 ? "l" : ""));
  t.appendChild(hr);
  DATA.gates.forEach(g => {
    const tr = document.createElement("tr");
    td(tr, sizeName(g.size), "l"); td(tr, g.selectivity, "l");
    td(tr, fmt(g.matches_mean, 0));
    [g.gate_linear, g.gate_rrf].forEach(v => {
      const [ok, all] = String(v).split("/").map(Number);
      const c = td(tr, (ok === all ? "✓ pass " : "✗ FAIL ") + v);
      c.classList.add("status", ok === all ? "ok" : "bad");
    });
    t.appendChild(tr);
  });
})();

(function resultsTable() {
  const t = document.getElementById("results-table");
  const hr = document.createElement("tr");
  ["contender", "size", "workers", "selectivity", "fields", "QPS", "mean ms", "p50", "p90", "p99", "p99.9"].forEach((h, i) => th(hr, h, i < 5 ? "l" : ""));
  t.appendChild(hr);
  const nameOf = k => (CONTENDERS.find(c => c.key === k) || { name: k }).name;
  [...DATA.results].sort((a, b) =>
    a.size - b.size || a.workers - b.workers ||
    SELS.indexOf(a.selectivity) - SELS.indexOf(b.selectivity) ||
    CONTENDERS.findIndex(c => c.key === a.contender) - CONTENDERS.findIndex(c => c.key === b.contender))
    .forEach(r => {
      const tr = document.createElement("tr");
      td(tr, nameOf(r.contender), "l"); td(tr, sizeName(r.size), "l");
      td(tr, String(r.workers), "l"); td(tr, r.selectivity, "l"); td(tr, r.fields || "", "l");
      td(tr, fmt(r.qps, 0)); td(tr, fmt(r.mean_ms));
      td(tr, fmt(r.p50_ms)); td(tr, fmt(r.p90_ms)); td(tr, fmt(r.p99_ms)); td(tr, fmt(r.p999_ms));
      t.appendChild(tr);
    });
})();

document.getElementById("profiles").textContent =
  JSON.parse(document.getElementById("profdata").textContent || "{}") &&
  document.getElementById("profdata").textContent;

function render() { syncW(); syncF(); syncM(); renderMain(); renderRatio(); renderIdentity(); }
render();
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", render);
</script>
</body>
</html>
"""

html = html.replace("__PAYLOAD__", payload).replace("__PROFILES__", profiles_pretty)
with open("mod12930_report.html", "w") as f:
    f.write(html)
print("wrote mod12930_report.html")
