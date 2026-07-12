"""Generate mod12930_balanced_report.html from results_balanced_full.json.
Separate artifact — does NOT touch mod12930_report.html (the imbalanced suite)."""

import json
import os

with open("results_balanced_full.json") as f:
    data = json.load(f)

sweep = None
if os.path.exists("results_merger_sweep.json"):
    with open("results_merger_sweep.json") as f:
        sweep = json.load(f)

payload = json.dumps({"meta": data["meta"], "results": data["results"], "gates": data["gates"],
                      "sweep": sweep})

html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MOD-12930 — Balanced Full Suite</title>
<style>
.viz-root {
  --surface-1: #fcfcfb; --page: #f9f9f7;
  --ink-1: #0b0b0b; --ink-2: #52514e; --ink-muted: #898781;
  --grid: #e1e0d9; --baseline: #c3c2b7; --ring: rgba(11,11,11,0.10);
  --s-hyblin: #2a78d6; --s-hybrrf: #1baf7a; --s-search: #eda100; --s-vsim: #4a3aa7;
  --sz-1: #86b6ef; --sz-2: #2a78d6; --sz-3: #104281;
  --good: #0ca30c; --critical: #d03b3b;
}
@media (prefers-color-scheme: dark) {
  .viz-root {
    --surface-1: #1a1a19; --page: #0d0d0d;
    --ink-1: #ffffff; --ink-2: #c3c2b7; --ink-muted: #898781;
    --grid: #2c2c2a; --baseline: #383835; --ring: rgba(255,255,255,0.10);
    --s-hyblin: #3987e5; --s-hybrrf: #199e70; --s-search: #c98500; --s-vsim: #9085e9;
    --sz-1: #6da7ec; --sz-2: #3987e5; --sz-3: #184f95;
  }
}
* { box-sizing: border-box; }
body.viz-root { margin: 0; background: var(--page); color: var(--ink-1);
  font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
.wrap { max-width: 1120px; margin: 0 auto; padding: 28px 20px 60px; }
h1 { font-size: 22px; font-weight: 650; margin: 0 0 4px; }
.meta { color: var(--ink-muted); font-size: 12px; margin-bottom: 20px; }
.card { background: var(--surface-1); border: 1px solid var(--ring); border-radius: 10px;
  padding: 18px 20px 14px; margin: 14px 0; }
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
.seg button { border: 0; background: transparent; color: var(--ink-2); padding: 6px 14px;
  font: inherit; font-size: 13px; cursor: pointer; }
.seg button.on { background: var(--ink-1); color: var(--surface-1); font-weight: 600; }
.legend { display: flex; gap: 16px; flex-wrap: wrap; margin: 4px 0 10px; }
.legend .li { display: inline-flex; align-items: center; gap: 6px; color: var(--ink-2); font-size: 12.5px; }
.legend .sw { width: 12px; height: 12px; border-radius: 3px; }
.legend .lk { width: 16px; height: 3px; border-radius: 2px; }
.panels { display: flex; gap: 18px; flex-wrap: wrap; }
.panel { flex: 1 1 300px; min-width: 280px; }
.panel .ptitle { text-align: center; color: var(--ink-2); font-size: 12.5px; margin-bottom: 2px; }
.panel .pmatch { text-align: center; color: var(--ink-muted); font-size: 11px; margin-bottom: 6px; }
svg text { font: 11px system-ui, sans-serif; fill: var(--ink-muted); font-variant-numeric: tabular-nums; }
svg .dlabel { fill: var(--ink-2); font-weight: 600; }
.bar { transition: filter .08s; cursor: default; }
.bar:hover, .bar.focus { filter: brightness(1.12); }
#tooltip { position: fixed; pointer-events: none; z-index: 10; display: none;
  background: var(--surface-1); border: 1px solid var(--ring); border-radius: 8px;
  box-shadow: 0 4px 14px rgba(0,0,0,.14); padding: 8px 11px; font-size: 12.5px; max-width: 320px; }
#tooltip .tv { font-weight: 650; font-size: 14px; color: var(--ink-1); }
#tooltip .tk { display: inline-block; width: 12px; height: 3px; border-radius: 2px; margin-right: 6px; vertical-align: middle; }
#tooltip .tr { color: var(--ink-2); margin-top: 2px; }
table { border-collapse: collapse; width: 100%; font-size: 12.5px; }
th, td { text-align: right; padding: 6px 10px; border-bottom: 1px solid var(--grid); font-variant-numeric: tabular-nums; }
th { color: var(--ink-2); font-weight: 600; border-bottom: 1px solid var(--baseline); }
th.l, td.l { text-align: left; }
.status { font-weight: 600; }
.status.ok { color: var(--good); } .status.bad { color: var(--critical); }
</style>
</head>
<body class="viz-root">
<div class="wrap">
  <h1>MOD-12930 — balanced full suite</h1>
  <div class="meta" id="meta"></div>
  <div class="tiles" id="tiles"></div>

  <div class="filters">
    <span class="flabel">Workers</span><span class="seg" id="f-workers"></span>
    <span class="flabel" id="w-note"></span>
  </div>

  <div class="card">
    <h2 id="merger-title">Merger overhead — C as % of max(search, vsim)</h2>
    <p class="sub">C = hybrid mean − max(search, vsim). Grouped by WINDOW, one bar per corpus size (calibrated |matches| in the tooltip). Hairline = 100%: C as expensive as the slowest subquery. Left: no loader (keys+scores only); right: with loader (documents read from the keyspace).</p>
    <div class="legend" id="legend-c"></div>
    <div class="panels" id="chart-c-top"></div>
  </div>

  <div class="card" id="degradation-card">
    <h2 id="deg-title">Degradation with dataset size — hybrid vs its subqueries</h2>
    <p class="sub" id="deg-sub"></p>
    <div class="legend" id="legend-s"></div>
    <div class="panels" id="chart-deg"></div>
  </div>

  <div class="card">
    <h2 id="t2">p50 latency by contender</h2>
    <p class="sub">One panel per WINDOW, grouped by dataset size. Branches are calibrated to equal latency per (size, window) — the hybrid bar above them is branch work + C.</p>
    <div class="legend" id="legend-m"></div>
    <div class="panels" id="chart-main"></div>
  </div>

  <div class="card">
    <h2>Calibration &amp; gates</h2>
    <p class="sub">balance_ratio = search p50 / vsim p50 at calibration (balanced within ±20%). Gates: FT.HYBRID ≡ untimed oracle fusion, tie-aware.</p>
    <table id="gates-table"></table>
  </div>

  <div class="card">
    <h2>All results</h2>
    <p class="sub" id="table-sub"></p>
    <table id="results-table"></table>
  </div>
</div>
<div id="tooltip"></div>

<script id="data" type="application/json">__PAYLOAD__</script>
<script>
"use strict";
const DATA = JSON.parse(document.getElementById("data").textContent);
const CONTENDERS = [
  { key: "hybrid_linear", name: "FT.HYBRID (LINEAR)", v: "--s-hyblin" },
  { key: "hybrid_rrf",    name: "FT.HYBRID (RRF)",    v: "--s-hybrrf" },
  { key: "search_branch", name: "SEARCH branch",      v: "--s-search" },
  { key: "vsim_branch",   name: "VSIM branch",        v: "--s-vsim" },
];
const SIZES = [...new Set(DATA.results.map(r => r.size))].sort((a, b) => a - b);
const WINDOWS = [...new Set(DATA.results.map(r => r.window))].sort((a, b) => a - b);
const WORKERS = [...new Set(DATA.results.map(r => r.workers))].sort((a, b) => a - b);
const FIELDS = [...new Set(DATA.results.map(r => r.fields))].sort();
const SIZE_V = ["--sz-1", "--sz-2", "--sz-3"];
const css = v => getComputedStyle(document.body).getPropertyValue(v).trim();
// Default view: the highest workers setting only. workers=0 (sequential) is opt-in.
const state = { workers: Math.max(...WORKERS) };
function segControl(el, options, get, set) {
  options.forEach(o => {
    const b = document.createElement("button");
    b.textContent = o.name; b.dataset.v = o.value;
    b.addEventListener("click", () => { set(o.value); render(); });
    el.appendChild(b);
  });
  return () => { [...el.children].forEach(b => b.classList.toggle("on", String(get()) === b.dataset.v)); };
}
const syncW = segControl(document.getElementById("f-workers"),
  [...WORKERS].sort((a, b) => b - a).map(w => ({ name: String(w), value: String(w) })),
  () => state.workers, v => state.workers = Number(v));
document.getElementById("w-note").textContent =
  WORKERS.includes(0) ? "(0 = sequential depletion, opt-in)" : "";
const rows = f => DATA.results.filter(r => Object.entries(f).every(([k, v]) => r[k] === v));
const row1 = f => rows(f)[0];
const fmt = (x, d) => x >= 1000 ? Math.round(x).toLocaleString("en-US")
  : x.toFixed(d !== undefined ? d : (x >= 100 ? 0 : x >= 10 ? 1 : 2));
const loaderName = f => f === "none" ? "loader (keyspace access): no" : "loader (keyspace access): yes";
const sizeName = s => s >= 1000 ? (s / 1000) + "K" : String(s);

function cOf(size, window, workers, fields) {
  const g = k => row1({ contender: k, size, window, workers, fields });
  const hy = g("hybrid_linear"), se = g("search_branch"), vs = g("vsim_branch");
  if (!hy || !se || !vs) return null;
  const maxBr = Math.max(se.mean_ms, vs.mean_ms);
  return { c: hy.mean_ms - maxBr, hy: hy.mean_ms, maxBr };
}

/* ---------- meta + tiles ---------- */
(function () {
  const m = DATA.meta;
  document.getElementById("meta").appendChild(document.createTextNode(
    `text queries engineered per (size, window) so text-subquery latency correlates with the vector subquery's (±${Math.round(100 * m.cal_tol)}%) · ` +
    `output top-${m.out_k} · ${m.n_query_set} distinct queries, ${m.n_timed} timed reps/cell (after ${m.n_warmup} warm-up)`));
  function tile(label, value, note) {
    const t = document.createElement("div"); t.className = "tile";
    const l = document.createElement("div"); l.className = "lbl"; l.textContent = label;
    const v = document.createElement("div"); v.className = "val"; v.textContent = value;
    const n = document.createElement("div"); n.className = "note"; n.textContent = note;
    t.append(l, v, n); return t;
  }
  const tiles = document.getElementById("tiles");
  const big = SIZES[SIZES.length - 1], wBig = WINDOWS[WINDOWS.length - 1];
  const WMAX = Math.max(...WORKERS);
  const cx = cOf(big, wBig, WMAX, "none");
  if (cx) {
    tiles.appendChild(tile(`C at WINDOW=${wBig}`, fmt(cx.c, 1) + " ms",
      `hybrid − max(search, vsim) · ${sizeName(big)} docs, workers=${WMAX}, no loader`));
    tiles.appendChild(tile("C vs slowest branch", fmt(100 * cx.c / cx.maxBr, 0) + "%",
      `at WINDOW=${wBig}, ${sizeName(big)} docs, workers=${WMAX}`));
  }
  const h0 = row1({ contender: "hybrid_linear", size: big, window: wBig, workers: 0, fields: "none" });
  const hx = row1({ contender: "hybrid_linear", size: big, window: wBig, workers: WMAX, fields: "none" });
  if (h0 && hx) tiles.appendChild(tile("Parallelism gain", "×" + fmt(h0.p50_ms / hx.p50_ms, 2),
    `hybrid p50 w0/w${WMAX} at WINDOW=${wBig} — ceiling 2×, Amdahl-capped by C`));
})();

/* ---------- filters / tooltip / panels (shared helpers) ---------- */
const tip = document.getElementById("tooltip");
function showTip(evt, lines) {
  tip.textContent = "";
  lines.forEach((ln, i) => {
    const d = document.createElement("div");
    d.className = i === 0 ? "tv" : "tr";
    if (ln.color) { const k = document.createElement("span"); k.className = "tk"; k.style.background = ln.color; d.appendChild(k); }
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

const SVGNS = "http://www.w3.org/2000/svg";
function el(n, attrs) { const e = document.createElementNS(SVGNS, n); for (const k in attrs) e.setAttribute(k, attrs[k]); return e; }
function niceTicks(max) {
  const raw = max / 4, mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map(s => s * mag).find(s => max / s <= 5) || 10 * mag;
  const top = Math.ceil(max / step - 1e-9) * step;
  const out = []; for (let v = 0; v <= top + step / 2; v += step) out.push(v);
  return out;
}
function columnPanel(groups, opts) {
  const W = opts.width || 320, plotH = 190, axB = 22, axT = 12, axL = 46, axR = 6;
  const H = plotH + axB + axT;
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: "100%", role: "img" });
  const maxV = Math.max(...groups.flatMap(g => g.cols.map(c => c.value)), 1e-9);
  const ticks = niceTicks(maxV);
  const yMax = ticks[ticks.length - 1] * 1.02;
  const y = v => axT + plotH - (v / yMax) * plotH;
  ticks.forEach(t => {
    svg.appendChild(el("line", { x1: axL, x2: W - axR, y1: y(t), y2: y(t),
      stroke: t === 0 ? css("--baseline") : css("--grid"), "stroke-width": 1 }));
    const tx = el("text", { x: axL - 6, y: y(t) + 3.5, "text-anchor": "end" });
    tx.textContent = fmt(t, t >= 10 ? 0 : undefined); svg.appendChild(tx);
  });
  (opts.refLines || []).forEach(rl => {
    if (rl <= yMax) svg.appendChild(el("line", { x1: axL, x2: W - axR, y1: y(rl), y2: y(rl),
      stroke: css("--baseline"), "stroke-width": 1 }));
  });
  const gw = (W - axL - axR) / groups.length;
  const barW = Math.min(24, (gw - 16) / Math.max(...groups.map(g => g.cols.length)) - 2);
  groups.forEach((g, gi) => {
    const n = g.cols.length, total = n * barW + (n - 1) * 2;
    let x = axL + gi * gw + (gw - total) / 2;
    const lx = el("text", { x: axL + gi * gw + gw / 2, y: H - 6, "text-anchor": "middle" });
    lx.textContent = g.label; svg.appendChild(lx);
    g.cols.forEach(c => {
      const y0 = y(0), y1 = y(c.value), topR = 4;
      const p = el("path", { class: "bar", fill: c.color, tabindex: "0", d:
        `M${x},${y0} L${x},${y1 + topR} Q${x},${y1} ${x + topR},${y1} ` +
        `L${x + barW - topR},${y1} Q${x + barW},${y1} ${x + barW},${y1 + topR} L${x + barW},${y0} Z` });
      p.addEventListener("pointermove", e => showTip(e, c.tooltip));
      p.addEventListener("pointerleave", hideTip);
      p.addEventListener("focus", () => { const r = p.getBoundingClientRect();
        showTip({ clientX: r.right, clientY: r.top }, c.tooltip); p.classList.add("focus"); });
      p.addEventListener("blur", () => { hideTip(); p.classList.remove("focus"); });
      svg.appendChild(p);
      if (c.dlabel) {
        const t = el("text", { x: x + barW / 2, y: Math.max(y1 - 4, 9), "text-anchor": "middle", class: "dlabel" });
        t.textContent = c.dlabel; svg.appendChild(t);
      }
      x += barW + 2;
    });
  });
  return svg;
}
// series: [{name, color, values[], }], x categories; crosshair tooltip lists all series.
function linePanel(series, xlabels, opts) {
  const W = opts.width || 340, plotH = 190, axB = 22, axT = 12, axL = 50, axR = 14;
  const H = plotH + axB + axT;
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: "100%", role: "img" });
  const maxV = Math.max(...series.flatMap(s => s.values.filter(v => v != null)), 1e-9);
  const ticks = niceTicks(maxV);
  const yMax = ticks[ticks.length - 1] * 1.02;
  const y = v => axT + plotH - (v / yMax) * plotH;
  const x = i => axL + (i + 0.5) * (W - axL - axR) / xlabels.length;
  ticks.forEach(t => {
    svg.appendChild(el("line", { x1: axL, x2: W - axR, y1: y(t), y2: y(t),
      stroke: t === 0 ? css("--baseline") : css("--grid"), "stroke-width": 1 }));
    const tx = el("text", { x: axL - 6, y: y(t) + 3.5, "text-anchor": "end" });
    tx.textContent = fmt(t, t >= 10 ? 0 : undefined); svg.appendChild(tx);
  });
  xlabels.forEach((xl, i) => {
    const tx = el("text", { x: x(i), y: H - 6, "text-anchor": "middle" });
    tx.textContent = xl; svg.appendChild(tx);
  });
  const cross = el("line", { y1: axT, y2: axT + plotH, stroke: css("--baseline"),
    "stroke-width": 1, visibility: "hidden" });
  svg.appendChild(cross);
  series.forEach(s => {
    const pts = s.values.map((v, i) => v == null ? null : [x(i), y(v)]).filter(Boolean);
    svg.appendChild(el("path", { fill: "none", stroke: s.color, "stroke-width": 2,
      "stroke-linejoin": "round", "stroke-linecap": "round",
      d: pts.map((p, i) => (i ? "L" : "M") + p[0] + "," + p[1]).join(" ") }));
    pts.forEach(p => svg.appendChild(el("circle", { cx: p[0], cy: p[1], r: 4.5,
      fill: s.color, stroke: css("--surface-1"), "stroke-width": 2 })));
    // no end-labels: the size lines converge (C is ~size-independent), so identity
    // rides on the legend + crosshair tooltip instead of colliding labels.
  });
  const hit = el("rect", { x: axL, y: axT, width: W - axL - axR, height: plotH, fill: "transparent" });
  hit.addEventListener("pointermove", e => {
    const r = svg.getBoundingClientRect();
    const px = (e.clientX - r.left) * (W / r.width);
    let best = 0, bd = 1e9;
    xlabels.forEach((_, i) => { const d = Math.abs(x(i) - px); if (d < bd) { bd = d; best = i; } });
    cross.setAttribute("x1", x(best)); cross.setAttribute("x2", x(best));
    cross.setAttribute("visibility", "visible");
    showTip(e, [{ text: opts.xname + " " + xlabels[best] },
      ...series.map(s => ({ color: s.color,
        text: `${s.name}: ${s.values[best] == null ? "n/a" : fmt(s.values[best]) + (opts.unit || "")}` }))]);
  });
  hit.addEventListener("pointerleave", () => { cross.setAttribute("visibility", "hidden"); hideTip(); });
  svg.appendChild(hit);
  return svg;
}
// multi-series log-x line panel: series = [{name, color, points: [{x, y, tooltip[]}]}]
function linesPanelLogX(seriesList, opts) {
  const W = opts.width || 340, plotH = 190, axB = 22, axT = 12, axL = 50, axR = 26;
  const H = plotH + axB + axT;
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: "100%", role: "img" });
  const all = seriesList.flatMap(s => s.points);
  const lx = v => Math.log10(v);
  const xs = all.map(p => lx(p.x));
  const xMin = Math.floor(Math.min(...xs)), xMax = Math.ceil(Math.max(...xs));
  const X = v => axL + (lx(v) - xMin) / (xMax - xMin || 1) * (W - axL - axR);
  const maxV = Math.max(...all.map(p => p.y), 1e-9);
  const ticks = niceTicks(maxV);
  const yMax = ticks[ticks.length - 1] * 1.02;
  const Y = v => axT + plotH - (v / yMax) * plotH;
  ticks.forEach(t => {
    svg.appendChild(el("line", { x1: axL, x2: W - axR, y1: Y(t), y2: Y(t),
      stroke: t === 0 ? css("--baseline") : css("--grid"), "stroke-width": 1 }));
    const tx = el("text", { x: axL - 6, y: Y(t) + 3.5, "text-anchor": "end" });
    tx.textContent = fmt(t, t >= 10 ? 0 : undefined); svg.appendChild(tx);
  });
  for (let d = xMin; d <= xMax; d++) {
    const v = Math.pow(10, d);
    svg.appendChild(el("line", { x1: X(v), x2: X(v), y1: axT, y2: axT + plotH,
      stroke: css("--grid"), "stroke-width": 1 }));
    const tx = el("text", { x: X(v), y: H - 6, "text-anchor": "middle" });
    tx.textContent = v >= 1e6 ? (v / 1e6) + "M" : v >= 1000 ? (v / 1000) + "K" : String(v);
    svg.appendChild(tx);
  }
  seriesList.forEach(s => {
    const pts = [...s.points].sort((a, b) => a.x - b.x);
    svg.appendChild(el("path", { fill: "none", stroke: s.color, "stroke-width": 2,
      "stroke-linejoin": "round", "stroke-linecap": "round",
      d: pts.map((p, i) => (i ? "L" : "M") + X(p.x) + "," + Y(p.y)).join(" ") }));
    pts.forEach(p => {
      const dot = el("circle", { cx: X(p.x), cy: Y(p.y), r: 4, fill: s.color,
        stroke: css("--surface-1"), "stroke-width": 2 });
      const hit = el("circle", { cx: X(p.x), cy: Y(p.y), r: 12, fill: "transparent" });
      [dot, hit].forEach(elm => {
        elm.addEventListener("pointermove", e => showTip(e, p.tooltip));
        elm.addEventListener("pointerleave", hideTip);
      });
      svg.appendChild(dot); svg.appendChild(hit);
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
function legend(elId, items, line) {
  const box = document.getElementById(elId); box.textContent = "";
  items.forEach(it => {
    const li = document.createElement("span"); li.className = "li";
    const sw = document.createElement("span"); sw.className = line ? "lk" : "sw";
    sw.style.background = css(it.v);
    li.append(sw, document.createTextNode(it.name)); box.appendChild(li);
  });
}

/* ---------- charts ---------- */
function matchesOf(size, window) {
  const r = row1({ size, window, contender: "hybrid_linear", workers: 0, fields: "none" });
  return r ? r.matches_mean : null;
}
function renderMerger() {
  document.getElementById("merger-title").textContent =
    `Merger overhead — C as % of max(search, vsim) — workers=${state.workers}`;
  legend("legend-c", SIZES.map((s, i) => ({ name: sizeName(s) + " docs", v: SIZE_V[i] })));
  const top = document.getElementById("chart-c-top"); top.textContent = "";
  FIELDS.forEach(fields => {
    const groups = WINDOWS.map(w => ({
      label: "W=" + w,
      cols: SIZES.map((s, i) => {
        const c = cOf(s, w, state.workers, fields);
        if (!c) return { value: 0, color: css(SIZE_V[i]), tooltip: [{ text: "no data" }] };
        const pct = 100 * c.c / c.maxBr;
        const mm = matchesOf(s, w);
        return { value: pct, color: css(SIZE_V[i]),
          dlabel: s === SIZES[SIZES.length - 1] ? fmt(pct, 0) + "%" : null,
          tooltip: [{ text: fmt(pct, 0) + "% of the slowest branch" },
                    { text: sizeName(s) + " docs", color: css(SIZE_V[i]) },
                    { text: `C ${fmt(c.c)} ms · slowest branch ${fmt(c.maxBr)} ms · hybrid ${fmt(c.hy)} ms` },
                    { text: `W=${w} · ${loaderName(fields)} · |matches|≈${fmt(mm || 0, 0)}` }] };
      }),
    }));
    const p = panelBox(top, loaderName(fields), "");
    p.appendChild(columnPanel(groups, { refLines: [100], width: 420 }));
  });
}


function renderMain() {
  document.getElementById("t2").textContent = `p50 latency by contender — workers=${state.workers}`;
  legend("legend-m", CONTENDERS);
  const box = document.getElementById("chart-main"); box.textContent = "";
  FIELDS.forEach(fields => {
    const hdr = document.createElement("div");
    hdr.style.flexBasis = "100%";
    hdr.style.cssText += "color:var(--ink-2);font-size:12.5px;font-weight:600;margin:8px 0 0";
    hdr.textContent = loaderName(fields);
    box.appendChild(hdr);
    WINDOWS.forEach(w => {
      const cal = DATA.meta.calibration.filter(c => c.window === w)
        .map(c => `${sizeName(c.size)}: ratio ${c.balance_ratio}${c.balanced ? "" : " (unbalanced)"}`).join(" · ");
      const p = panelBox(box, `WINDOW=${w}`, cal);
      const groups = SIZES.map(size => ({
        label: sizeName(size) + " docs",
        cols: CONTENDERS.map(c => {
          const r = row1({ contender: c.key, size, window: w, workers: state.workers, fields });
          if (!r) return { value: 0, color: css(c.v), tooltip: [{ text: "no data" }] };
          return { value: r.p50_ms, color: css(c.v),
            dlabel: c.key === "hybrid_linear" ? fmt(r.p50_ms) : null,
            tooltip: [{ text: `${fmt(r.p50_ms)} ms p50` }, { text: c.name, color: css(c.v) },
                      { text: `${sizeName(size)} docs · W=${w} · workers=${state.workers} · ${loaderName(fields)}` },
                      { text: `calibrated |matches|≈${fmt(r.matches_mean, 0)} · p90 ${fmt(r.p90_ms)} · p99 ${fmt(r.p99_ms)} ms` }] };
        }),
      }));
      p.appendChild(columnPanel(groups, {}));
    });
  });
}


/* ---------- tables ---------- */
function th(tr, txt, cls) { const c = document.createElement("th"); if (cls) c.className = cls; c.textContent = txt; tr.appendChild(c); }
function td(tr, txt, cls) { const c = document.createElement("td"); if (cls) c.className = cls; c.textContent = txt; tr.appendChild(c); return c; }

(function gatesTable() {
  const t = document.getElementById("gates-table");
  const hr = document.createElement("tr");
  ["size", "K/W", "calibrated |matches|", "balance ratio", "gate LINEAR", "gate RRF"].forEach((h, i) => th(hr, h, i < 2 ? "l" : ""));
  t.appendChild(hr);
  DATA.gates.forEach(g => {
    const tr = document.createElement("tr");
    td(tr, sizeName(g.size) + " docs", "l"); td(tr, `${g.k}/${g.window}`, "l");
    td(tr, fmt(g.matches_mean, 0));
    const rc = td(tr, fmt(g.balance_ratio, 2) + (Math.abs(g.balance_ratio - 1) <= 0.2 ? "" : " ✗"));
    if (Math.abs(g.balance_ratio - 1) > 0.2) rc.classList.add("status", "bad");
    [g.gate_linear, g.gate_rrf].forEach(v => {
      const [ok, all] = String(v).split("/").map(Number);
      const c = td(tr, (ok === all ? "✓ pass " : "✗ FAIL ") + v);
      c.classList.add("status", ok === all ? "ok" : "bad");
    });
    t.appendChild(tr);
  });
})();

function renderTable() {
  const t = document.getElementById("results-table");
  t.textContent = "";
  const hr = document.createElement("tr");
  ["contender", "size", "K/W", "workers", "loader", "QPS", "mean ms", "p50", "p90", "p99", "p99.9"].forEach((h, i) => th(hr, h, i < 5 ? "l" : ""));
  t.appendChild(hr);
  const nameOf = k => (CONTENDERS.find(c => c.key === k) || { name: k }).name;
  [...DATA.results].filter(r => r.workers === state.workers).sort((a, b) =>
    a.size - b.size || a.window - b.window || a.workers - b.workers ||
    String(a.fields).localeCompare(String(b.fields)) ||
    CONTENDERS.findIndex(c => c.key === a.contender) - CONTENDERS.findIndex(c => c.key === b.contender))
    .forEach(r => {
      const tr = document.createElement("tr");
      td(tr, nameOf(r.contender), "l"); td(tr, sizeName(r.size), "l"); td(tr, `${r.k}/${r.window}`, "l");
      td(tr, String(r.workers), "l"); td(tr, r.fields === "none" ? "no" : "yes", "l");
      td(tr, fmt(r.qps, 0)); td(tr, fmt(r.mean_ms)); td(tr, fmt(r.p50_ms));
      td(tr, fmt(r.p90_ms)); td(tr, fmt(r.p99_ms)); td(tr, fmt(r.p999_ms));
      t.appendChild(tr);
    });
}

function renderDegradation() {
  const w0 = state.workers;
  document.getElementById("deg-title").textContent =
    `Degradation with dataset size — hybrid vs its subqueries — workers=${w0}, no loader`;
  document.getElementById("deg-sub").textContent =
    `Raw p50 latencies side by side (no derived metrics), x = dataset size (log scale), ` +
    `one panel per K/WINDOW. The text queries are engineered per cell so the text ` +
    `subquery's latency correlates with the vector subquery's (calibrated |matches| is ` +
    `the knob — shown in the tooltip, with the achieved balance ratio).`;
  const SC = [
    { key: "hybrid_linear", name: "FT.HYBRID (LINEAR)", v: "--s-hyblin" },
    { key: "search_branch", name: "SEARCH branch", v: "--s-search" },
    { key: "vsim_branch",   name: "VSIM branch",   v: "--s-vsim" },
  ];
  legend("legend-s", SC);
  const box = document.getElementById("chart-deg"); box.textContent = "";
  WINDOWS.forEach(w => {
    const kOf = (DATA.results.find(r => r.window === w) || {}).k;
    const seriesList = SC.map(c => ({
      name: c.name, color: css(c.v),
      points: SIZES.map(size => {
        const r = row1({ contender: c.key, size, window: w, workers: w0, fields: "none" });
        if (!r) return null;
        return { x: size, y: r.p50_ms,
          tooltip: [{ text: `${fmt(r.p50_ms)} ms p50` },
                    { text: c.name, color: css(c.v) },
                    { text: `${sizeName(size)} docs · K/W=${r.k}/${w} · workers=${w0}` },
                    { text: `calibrated |matches|≈${fmt(r.matches_mean, 0)} · balance ratio ${fmt(r.balance_ratio, 2)}` }] };
      }).filter(Boolean),
    }));
    const p = panelBox(box, `K/WINDOW=${kOf}/${w} — p50 (ms) vs dataset size`, "");
    p.appendChild(linesPanelLogX(seriesList, {}));
  });
}

function render() {
  renderDegradation();
  syncW();
  document.getElementById("table-sub").textContent =
    `The full table view (workers=${state.workers}) — every number reachable without hover.`;
  renderMerger(); renderMain(); renderTable();
}
render();
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", render);
</script>
</body>
</html>
"""

html = html.replace("__PAYLOAD__", payload)
with open("mod12930_balanced_report.html", "w") as f:
    f.write(html)
print("wrote mod12930_balanced_report.html")
