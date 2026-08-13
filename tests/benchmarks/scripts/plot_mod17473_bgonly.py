#!/usr/bin/env python3
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Render publication artifacts from sanitized MOD-17473 benchmark data."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


WIDTH, HEIGHT, SCALE = 1600, 1100, 2
BG, INK, MUTED = "#FFFFFF", "#17202A", "#52616B"
GRID, ZERO, CI = "#D7DEE3", "#65737E", "#111111"
MASTER_FIRST, BGONLY_FIRST = "#0072B2", "#D55E00"
SA_SHADE, CL_SHADE = "#F5F9FC", "#FCF7F3"
LABEL_X = 54
PANEL_LEFT = (405, 1015)
PANEL_RIGHT = (935, 1545)
ROW_Y = (275, 370, 550, 645, 740, 835)
JITTER = (-14, -10, -6, -2, 2, 6, 10, 14, -8, 8)
METRICS = ("throughput_rps", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms")
PANELS = (
    {
        "title": "Throughput change",
        "subtitle": "positive is better →",
        "metric": "throughput_rps",
        "domain": (-35.0, 5.0),
        "ticks": (-30, -20, -10, 0),
    },
    {
        "title": "p95 latency change",
        "subtitle": "positive is worse →",
        "metric": "latency_p95_ms",
        "domain": (-10.0, 55.0),
        "ticks": (-10, 0, 10, 20, 30, 40, 50),
    },
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def map_x(value: float, panel_index: int) -> float:
    low, high = PANELS[panel_index]["domain"]
    return PANEL_LEFT[panel_index] + (value - low) / (high - low) * (
        PANEL_RIGHT[panel_index] - PANEL_LEFT[panel_index]
    )


def svg_text(x: float, y: float, text: str, size: int, *, weight: int = 400,
             fill: str = INK, anchor: str = "start") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="DejaVu Sans, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
        f'text-anchor="{anchor}">{esc(text)}</text>'
    )


def font(size: int, bold: bool = False):
    name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    for root in (Path("/usr/share/fonts/truetype/dejavu"), Path("/usr/share/fonts/dejavu")):
        path = root / name
        if path.is_file():
            return ImageFont.truetype(str(path), size * SCALE)
    return ImageFont.load_default()


def xy(values):
    return tuple(int(round(value * SCALE)) for value in values)


def draw_text(draw, position, text, size, *, color=INK, bold=False, anchor="la"):
    draw.text(xy(position), text, font=font(size, bold), fill=color, anchor=anchor)


def display_topology(case: dict) -> str:
    return (
        "Standalone" if case["topology"] == "standalone"
        else "Native 2-primary cluster"
    )


def interval(metric: dict) -> str:
    low, high = metric["ci95_percent"]
    return f"{metric['effect_percent']:+.2f}% [{low:+.2f}, {high:+.2f}]"


def validate(data: dict) -> None:
    require(data.get("schema") == "mod-17473-master-vs-bgonly-v1", "unsupported data schema")
    require(data.get("validation", {}).get("strict_gates_passed") is True,
            "strict benchmark gates did not pass")
    cases = data.get("cases")
    require(isinstance(cases, list) and len(cases) == 6, "expected six benchmark cases")
    for case in cases:
        require(
            case.get("pairs") == 10
            and case.get("master_first_pairs") == 5
            and case.get("bgonly_first_pairs") == 5
            and case.get("observations") == 20,
            f"case is not balanced N=10/20 observations: {case.get('id')}",
        )
        require(set(case.get("metrics", {})) == set(METRICS),
                f"case metric inventory mismatch: {case.get('id')}")
        raw = case.get("raw_pairs")
        require(isinstance(raw, list) and len(raw) == 10,
                f"case raw pair inventory mismatch: {case.get('id')}")
        require(sum(pair.get("order") == "master_first" for pair in raw) == 5,
                f"case raw pairs are not order-balanced: {case.get('id')}")
        for repetition, pair in enumerate(raw):
            require(pair.get("repetition") == repetition, "raw repetitions are not contiguous")
            require(set(pair.get("metrics", {})) == set(METRICS), "raw metric inventory mismatch")


def render_svg(data: dict, output: Path) -> None:
    cases = data["cases"]
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img" '
        f'aria-labelledby="chart-title chart-desc">',
        '<title id="chart-title">MOD-17473 BG-only serialization versus master</title>',
        '<desc id="chart-desc">Six AWS benchmark cases. Each has ten adjacent pairs, '
        'balanced five master-first and five BG-only-first. Raw paired percentage changes are '
        'shown as opaque dots and triangles. Diamonds are geometric means and bars are '
        'order-stratified bootstrap 95 percent confidence intervals.</desc>',
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="{BG}"/>',
        svg_text(54, 62, "MOD-17473: BG-only serialization vs master", 32, weight=700),
        svg_text(54, 96, "10 adjacent pairs / 20 observations per case · 5 master-first / 5 BG-first · 10,000 rows/reply", 18, fill=MUTED),
        f'<rect x="38" y="209" width="1524" height="213" rx="14" fill="{SA_SHADE}"/>',
        f'<rect x="38" y="490" width="1524" height="402" rx="14" fill="{CL_SHADE}"/>',
        svg_text(54, 193, "STANDALONE", 15, weight=700, fill=MASTER_FIRST),
        svg_text(54, 457, "NATIVE TWO-PRIMARY CLUSTER  ·  SEARCH_THREADS=1", 15, weight=700, fill=BGONLY_FIRST),
        svg_text(54, 480, "WORKERS is per shard; the public coordinator serializes on a BG thread in every cluster case.", 14, fill=MUTED),
    ]
    for panel_index, panel in enumerate(PANELS):
        left, right = PANEL_LEFT[panel_index], PANEL_RIGHT[panel_index]
        svg.append(svg_text((left + right) / 2, 150, panel["title"], 22, weight=700, anchor="middle"))
        svg.append(svg_text((left + right) / 2, 177, panel["subtitle"], 15, fill=MUTED, anchor="middle"))
        for tick in panel["ticks"]:
            x = map_x(tick, panel_index)
            svg.append(
                f'<line x1="{x:.1f}" y1="215" x2="{x:.1f}" y2="892" '
                f'stroke="{ZERO if tick == 0 else GRID}" stroke-width="{2 if tick == 0 else 1}"/>'
            )
            svg.append(svg_text(x, 928, f"{tick:+d}%", 14, fill=MUTED, anchor="middle"))
    for case, y in zip(cases, ROW_Y):
        svg.append(svg_text(LABEL_X, y - 5, f"RESP{case['protocol']}  ·  WORKERS={case['workers']}", 20, weight=700))
        svg.append(svg_text(LABEL_X, y + 21, "N=10 pairs  ·  20 observations", 14, fill=MUTED))
        for panel_index, panel in enumerate(PANELS):
            summary = case["metrics"][panel["metric"]]
            low, high = summary["ci95_percent"]
            y_center = y
            svg.append(
                f'<line x1="{map_x(low, panel_index):.1f}" y1="{y_center}" '
                f'x2="{map_x(high, panel_index):.1f}" y2="{y_center}" '
                f'stroke="{CI}" stroke-width="5" stroke-linecap="round"/>'
            )
            for pair in case["raw_pairs"]:
                point = pair["metrics"][panel["metric"]]["effect_percent"]
                px = map_x(point, panel_index)
                py = y_center + JITTER[pair["repetition"]]
                if pair["order"] == "master_first":
                    svg.append(
                        f'<circle cx="{px:.1f}" cy="{py:.1f}" r="5.2" '
                        f'fill="{MASTER_FIRST}" stroke="white" stroke-width="1"/>'
                    )
                else:
                    points = f"{px:.1f},{py-6:.1f} {px-5.8:.1f},{py+5:.1f} {px+5.8:.1f},{py+5:.1f}"
                    svg.append(
                        f'<polygon points="{points}" fill="{BGONLY_FIRST}" '
                        f'stroke="white" stroke-width="1"/>'
                    )
            effect_x = map_x(summary["effect_percent"], panel_index)
            diamond = (
                f"{effect_x:.1f},{y_center-8} {effect_x+8:.1f},{y_center} "
                f"{effect_x:.1f},{y_center+8} {effect_x-8:.1f},{y_center}"
            )
            svg.append(f'<polygon points="{diamond}" fill="white" stroke="{CI}" stroke-width="3"/>')
    legend_y = 978
    svg.extend([
        f'<circle cx="60" cy="{legend_y}" r="6" fill="{MASTER_FIRST}"/>',
        svg_text(76, legend_y + 5, "master-first, 5 pairs", 15),
        f'<polygon points="267,{legend_y-7} 260,{legend_y+6} 274,{legend_y+6}" fill="{BGONLY_FIRST}"/>',
        svg_text(286, legend_y + 5, "BG-first, 5 pairs", 15),
        f'<line x1="480" y1="{legend_y}" x2="535" y2="{legend_y}" stroke="{CI}" stroke-width="5" stroke-linecap="round"/>',
        f'<polygon points="507,{legend_y-8} 515,{legend_y} 507,{legend_y+8} 499,{legend_y}" fill="white" stroke="{CI}" stroke-width="3"/>',
        svg_text(549, legend_y + 5, "geometric mean + order-stratified bootstrap 95% CI", 15),
        svg_text(54, 1030, "Each marker is (BG-only / master − 1) × 100 for one adjacent pair. Opaque shape and color both encode order.", 15, fill=MUTED),
    ])
    svg.append("</svg>\n")
    output.write_text("\n".join(svg), encoding="utf-8")


def render_png(data: dict, output: Path) -> None:
    cases = data["cases"]
    image = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), BG)
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle(xy((38, 209, 1562, 422)), radius=14*SCALE, fill=SA_SHADE)
    draw.rounded_rectangle(xy((38, 490, 1562, 892)), radius=14*SCALE, fill=CL_SHADE)
    draw_text(draw, (54, 62), "MOD-17473: BG-only serialization vs master", 32, bold=True, anchor="ls")
    draw_text(draw, (54, 96), "10 adjacent pairs / 20 observations per case · 5 master-first / 5 BG-first · 10,000 rows/reply", 18, color=MUTED, anchor="ls")
    draw_text(draw, (54, 193), "STANDALONE", 15, color=MASTER_FIRST, bold=True, anchor="ls")
    draw_text(draw, (54, 457), "NATIVE TWO-PRIMARY CLUSTER  ·  SEARCH_THREADS=1", 15, color=BGONLY_FIRST, bold=True, anchor="ls")
    draw_text(draw, (54, 480), "WORKERS is per shard; the public coordinator serializes on a BG thread in every cluster case.", 14, color=MUTED, anchor="ls")
    for panel_index, panel in enumerate(PANELS):
        left, right = PANEL_LEFT[panel_index], PANEL_RIGHT[panel_index]
        draw_text(draw, ((left + right)/2, 150), panel["title"], 22, bold=True, anchor="ms")
        draw_text(draw, ((left + right)/2, 177), panel["subtitle"], 15, color=MUTED, anchor="ms")
        for tick in panel["ticks"]:
            x = map_x(tick, panel_index)
            draw.line(xy((x, 215, x, 892)), fill=ZERO if tick == 0 else GRID,
                      width=(2 if tick == 0 else 1)*SCALE)
            draw_text(draw, (x, 928), f"{tick:+d}%", 14, color=MUTED, anchor="ms")
    for case, y in zip(cases, ROW_Y):
        draw_text(draw, (LABEL_X, y-5), f"RESP{case['protocol']}  ·  WORKERS={case['workers']}", 20, bold=True, anchor="ls")
        draw_text(draw, (LABEL_X, y+21), "N=10 pairs  ·  20 observations", 14, color=MUTED, anchor="ls")
        for panel_index, panel in enumerate(PANELS):
            summary = case["metrics"][panel["metric"]]
            low, high = summary["ci95_percent"]
            draw.line(xy((map_x(low,panel_index), y, map_x(high,panel_index), y)), fill=CI, width=5*SCALE)
            for pair in case["raw_pairs"]:
                px = map_x(pair["metrics"][panel["metric"]]["effect_percent"], panel_index)
                py = y + JITTER[pair["repetition"]]
                if pair["order"] == "master_first":
                    draw.ellipse(xy((px-5.5,py-5.5,px+5.5,py+5.5)), fill=MASTER_FIRST,
                                 outline="white", width=SCALE)
                else:
                    draw.polygon([xy((px,py-6)),xy((px-5.8,py+5)),xy((px+5.8,py+5))],
                                 fill=BGONLY_FIRST, outline="white")
            ex = map_x(summary["effect_percent"], panel_index)
            draw.polygon([xy((ex,y-8)),xy((ex+8,y)),xy((ex,y+8)),xy((ex-8,y))],
                         fill="white", outline=CI, width=3*SCALE)
    draw.ellipse(xy((54,972,66,984)), fill=MASTER_FIRST)
    draw_text(draw,(76,983),"master-first, 5 pairs",15,anchor="ls")
    draw.polygon([xy((267,971)),xy((260,984)),xy((274,984))], fill=BGONLY_FIRST)
    draw_text(draw,(286,983),"BG-first, 5 pairs",15,anchor="ls")
    draw.line(xy((480,978,535,978)), fill=CI, width=5*SCALE)
    draw.polygon([xy((507,970)),xy((515,978)),xy((507,986)),xy((499,978))],
                 fill="white", outline=CI, width=3*SCALE)
    draw_text(draw,(549,983),"geometric mean + order-stratified bootstrap 95% CI",15,anchor="ls")
    draw_text(draw,(54,1030),"Each marker is (BG-only / master − 1) × 100 for one adjacent pair. Opaque shape and color both encode order.",15,color=MUTED,anchor="ls")
    image.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS).save(
        output, format="PNG", optimize=False, compress_level=9
    )


def render_markdown(data: dict, data_sha: str, output: Path) -> None:
    lines = [
        "# MOD-17473 AWS benchmark: BG-only serialization vs master",
        "",
        "Effects are geometric means of paired BG-only/master ratios; brackets are the 95% "
        "order-stratified bootstrap CI. Positive throughput is better; positive latency is worse.",
        "",
        "**N=10 means 10 adjacent master/BG-only pairs and 20 variant observations per case: "
        "5 master-first pairs and 5 BG-only-first pairs.**",
        "",
        "| Topology | Protocol | Workers | N / observations | Throughput Δ [95% CI] | p50 Δ [95% CI] | p95 Δ [95% CI] | p99 Δ [95% CI] |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for case in data["cases"]:
        m = case["metrics"]
        lines.append(
            f"| {display_topology(case)} | RESP{case['protocol']} | {case['workers']} | "
            f"10 / 20 | {interval(m['throughput_rps'])} | {interval(m['latency_p50_ms'])} | "
            f"{interval(m['latency_p95_ms'])} | {interval(m['latency_p99_ms'])} |"
        )
    accounting = data["run_accounting"]
    validation = data["validation"]
    lines.extend([
        "",
        "## Run accounting",
        "",
        f"- {accounting['cases']} cases × {accounting['pairs_per_case']} adjacent pairs = "
        f"**{accounting['total_pairs']} pairs** and **{accounting['total_variant_observations']} variant observations**.",
        f"- Pair order: {accounting['master_first_pairs']} master-first and "
        f"{accounting['bgonly_first_pairs']} BG-only-first overall.",
        f"- **{accounting['measured_public_calls']:,} measured public calls**, each returning "
        f"{accounting['rows_per_measured_public_call']:,} rows "
        f"(**{accounting['total_measured_rows']:,} rows**).",
        f"- Standalone: {accounting['standalone_redis_process_executions']} Redis process executions. "
        f"Cluster: {accounting['cluster_topology_observations']} two-primary topology observations / "
        f"{accounting['cluster_redis_process_executions']} Redis process executions.",
        f"- Cluster internal aggregate calls: {accounting['measured_internal_cluster_calls']:,}. "
        f"Excluded warmups: {accounting['excluded_warmup_public_calls']:,} public calls.",
        "",
        "## Interpretation and validation scope",
        "",
        "- Cluster `WORKERS` is per shard. Public coordinator serialization runs on a BG thread "
        "in all four cluster cases, including shard `WORKERS=0`.",
        "- Strict full-row, fingerprint, command-count, provenance, and effective-configuration "
        "gates passed for both topologies.",
        f"- Standalone intentional difference: {validation['standalone']['intentional_difference']}.",
        f"- Cluster intentional difference: {validation['native_two_primary_cluster']['intentional_difference']}.",
        "",
        "## Publication artifacts",
        "",
        "- `mod-17473-aws-bgonly-vs-master.svg`",
        "- `mod-17473-aws-bgonly-vs-master.png`",
        f"- Sanitized data SHA256: `{data_sha}`",
    ])
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("data_json", metavar="DATA_JSON", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    data_path = args.data_json.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    require(data_path.is_file(), f"missing DATA_JSON: {data_path}")
    with data_path.open(encoding="utf-8") as source:
        data = json.load(source)
    validate(data)
    output_dir.mkdir(parents=True, exist_ok=True)
    base = output_dir / "mod-17473-aws-bgonly-vs-master"
    render_svg(data, base.with_suffix(".svg"))
    render_png(data, base.with_suffix(".png"))
    render_markdown(data, sha256(data_path), base.with_suffix(".md"))
    for suffix in (".svg", ".png", ".md"):
        path = base.with_suffix(suffix)
        print(f"{sha256(path)}  {path}")


if __name__ == "__main__":
    main()
