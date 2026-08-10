#!/usr/bin/env python3
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Prepare and render the balanced MOD-17473 AWS benchmark comparison plots.

The ``prepare`` command extracts the plotted subset from the offline analyzer
JSON. The ``render`` command turns that compact, reviewable data into SVG and
PNG figures. SVG generation uses only the Python standard library; PNG output
additionally requires Pillow (the committed PNGs used Pillow 10.2.0).
"""

import argparse
import hashlib
import html
import json
import math
import random
import statistics
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont
except ModuleNotFoundError:
    Image = ImageDraw = ImageFont = None


BASELINE_SHORT = "20ba4ed7c2"
CANDIDATE_SHORT = "3ff80fa72"
BOOTSTRAP_ITERATIONS = 10_000
BOOTSTRAP_SEED = 17_473
PERFORMANCE_BUNDLE_SHA256 = (
    "1a1f09fee7fd350b8186ba0a6377daf01b50069eeb058900a0e99803d29fe1c0"
)
MEMORY_BUNDLE_SHA256 = (
    "4c1658576a147ac318655ca5519dc931a1669bcf1486417ea11352f4ea223c8e"
)

PERFORMANCE_CASES = (
    ("full-narrow1000-r2-c32-w1", "Narrow 1k · workers=1 · RESP2", "RESP2"),
    ("full-narrow1000-r3-c32-w1", "Narrow 1k · workers=1 · RESP3", "RESP3"),
    ("full-wide1000-r2-c32-w1", "Wide 1k · workers=1 · RESP2", "RESP2"),
    ("full-wide1000-r3-c32-w1", "Wide 1k · workers=1 · RESP3", "RESP3"),
    ("full-wide10000-r2-c32-w0", "Wide 10k · workers=0 · RESP2", "RESP2"),
    ("full-wide10000-r3-c32-w0", "Wide 10k · workers=0 · RESP3", "RESP3"),
    ("full-wide10000-r2-c32-w1", "Wide 10k · workers=1 · RESP2", "RESP2"),
    ("full-wide10000-r3-c32-w1", "Wide 10k · workers=1 · RESP3", "RESP3"),
    ("full-wide10000-r2-c32-w16", "Wide 10k · workers=16 · RESP2", "RESP2"),
    ("full-wide10000-r3-c32-w16", "Wide 10k · workers=16 · RESP3", "RESP3"),
    ("full-nolimit-wide-r2-c32-w1", "Wide, no LIMIT · workers=1 · RESP2", "RESP2"),
    ("full-nolimit-wide-r3-c32-w1", "Wide, no LIMIT · workers=1 · RESP3", "RESP3"),
)

MEMORY_CASES = (
    ("full-wide10000-r2-c32-w0", "Wide 10k · workers=0 · RESP2", "RESP2"),
    ("full-wide10000-r3-c32-w0", "Wide 10k · workers=0 · RESP3", "RESP3"),
    ("full-wide10000-r2-c32-w16", "Wide 10k · workers=16 · RESP2", "RESP2"),
    ("full-wide10000-r3-c32-w16", "Wide 10k · workers=16 · RESP3", "RESP3"),
)

MEMORY_METRICS = (
    (
        "allocator_peak",
        "redis_allocator_allocated_bytes/peak_minus_before",
        "Allocator peak excursion",
    ),
    (
        "allocator_live_10s",
        "redis_allocator_allocated_bytes/cooldown_residual",
        "Allocator live at 10s",
    ),
    ("rss_10s", "proc_VmRSS_bytes/cooldown_residual", "Process RSS at 10s"),
)

COLORS = {
    "ink": "#172033",
    "sub": "#505B70",
    "muted": "#7B8495",
    "grid": "#D8DEE9",
    "row": "#EEF1F5",
    "zero": "#2D3A4F",
    "band": "#E8F2FF",
    "resp2": "#256FAF",
    "resp2_light": "#9FC5E5",
    "resp3": "#7C3AED",
    "resp3_light": "#C5ADF2",
    "worse": "#C53B36",
    "better": "#168052",
    "white": "#FFFFFF",
}


def load_json(path):
    with Path(path).open(encoding="utf-8") as infile:
        return json.load(infile)


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as infile:
        for chunk in iter(lambda: infile.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def percentile(sorted_values, quantile):
    position = (len(sorted_values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def bootstrap_median_ci(values, identity):
    seed_material = f"{BOOTSTRAP_SEED}:{identity}".encode()
    seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:8], "big")
    rng = random.Random(seed)
    count = len(values)
    estimates = []
    for _ in range(BOOTSTRAP_ITERATIONS):
        estimates.append(statistics.median(values[rng.randrange(count)] for _ in range(count)))
    estimates.sort()
    return [percentile(estimates, 0.025), percentile(estimates, 0.975)]


def metric_from_pair(pair, metric_name):
    matches = [metric for metric in pair["metrics"] if metric["metric"] == metric_name]
    if len(matches) != 1:
        raise ValueError(f"expected one {metric_name!r} metric in {pair['case']!r}")
    return matches[0]


def select_pairs(analysis, case_name, require_equal_work):
    pairs = sorted(
        (pair for pair in analysis["raw_pairs"] if pair["case"] == case_name),
        key=lambda pair: pair["repetition"],
    )
    if len(pairs) != 10:
        raise ValueError(f"{case_name}: expected 10 pairs, found {len(pairs)}")
    orders = [pair["order"] for pair in pairs]
    if orders.count("AB") != 5 or orders.count("BA") != 5:
        raise ValueError(f"{case_name}: expected balanced 5 AB / 5 BA, found {orders}")
    if require_equal_work:
        if any(pair["workload_mode"] != "full-equal-work" for pair in pairs):
            raise ValueError(f"{case_name}: expected full-equal-work observations")
        if not all(pair["rows_directly_comparable"] for pair in pairs):
            raise ValueError(f"{case_name}: result rows are not directly comparable")
    return pairs


def aggregate_for(analysis, case_name, metric_name):
    matches = [
        aggregate
        for aggregate in analysis["aggregates"]
        if aggregate["case"] == case_name and aggregate["metric"] == metric_name
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one aggregate for {case_name!r} / {metric_name!r}")
    return matches[0]


def order_summary(pairs):
    return {
        "pairs": len(pairs),
        "baseline_observations": len(pairs),
        "candidate_observations": len(pairs),
        "process_executions": 2 * len(pairs),
        "ab": sum(pair["order"] == "AB" for pair in pairs),
        "ba": sum(pair["order"] == "BA" for pair in pairs),
    }


def prepare_performance_cases(analysis):
    prepared = []
    for case_name, label, protocol in PERFORMANCE_CASES:
        pairs = select_pairs(analysis, case_name, require_equal_work=True)
        case = {
            "case": case_name,
            "label": label,
            "protocol": protocol,
            "runs": order_summary(pairs),
            "metrics": {},
        }
        for output_name, metric_name in (
            ("throughput", "requests_per_second"),
            ("latency_p95", "latency_p95_ms"),
        ):
            aggregate = aggregate_for(analysis, case_name, metric_name)
            case["metrics"][output_name] = {
                "metric": metric_name,
                "units": aggregate["units"],
                "baseline_median": aggregate["baseline_median"],
                "candidate_median": aggregate["candidate_median"],
                "effect_percent": aggregate["geometric_mean_percent_delta"],
                "effect_ci95": aggregate["geometric_mean_percent_delta_ci95"],
                "paired_percent": [
                    metric_from_pair(pair, metric_name)["percent_delta"] for pair in pairs
                ],
                "orders": [pair["order"] for pair in pairs],
            }
        prepared.append(case)
    return prepared


def prepare_memory_cases(analysis):
    prepared = []
    for case_name, label, protocol in MEMORY_CASES:
        pairs = select_pairs(analysis, case_name, require_equal_work=True)
        case = {
            "case": case_name,
            "label": label,
            "protocol": protocol,
            "runs": order_summary(pairs),
            "metrics": {},
        }
        for output_name, metric_name, title in MEMORY_METRICS:
            aggregate = aggregate_for(analysis, case_name, metric_name)
            deltas = [metric_from_pair(pair, metric_name)["absolute_delta"] for pair in pairs]
            case["metrics"][output_name] = {
                "metric": metric_name,
                "title": title,
                "units": aggregate["units"],
                "baseline_median": aggregate["baseline_median"],
                "candidate_median": aggregate["candidate_median"],
                "median_delta": statistics.median(deltas),
                "median_delta_ci95": bootstrap_median_ci(deltas, f"{case_name}:{metric_name}"),
                "paired_absolute_deltas": deltas,
                "orders": [pair["order"] for pair in pairs],
            }
        prepared.append(case)
    return prepared


def validate_campaign(analysis, expected_pairs, expected_candidate):
    if analysis["valid_pair_count"] != expected_pairs or analysis["excluded_pairs"]:
        raise ValueError(
            f"expected {expected_pairs} valid pairs and no exclusions; got "
            f"{analysis['valid_pair_count']} / {len(analysis['excluded_pairs'])}"
        )
    if not analysis["baseline_revision"].startswith(BASELINE_SHORT):
        raise ValueError(f"unexpected baseline revision: {analysis['baseline_revision']}")
    if not analysis["pr_revision"].startswith(expected_candidate):
        raise ValueError(f"unexpected candidate revision: {analysis['pr_revision']}")


def campaign_counts(analysis):
    pairs = analysis["raw_pairs"]
    return {
        "cases": len({pair["case"] for pair in pairs}),
        "pairs": len(pairs),
        "observations": 2 * len(pairs),
        "ab": sum(pair["order"] == "AB" for pair in pairs),
        "ba": sum(pair["order"] == "BA" for pair in pairs),
    }


def prepare_data(args):
    performance = load_json(args.performance_analysis)
    memory = load_json(args.memory_analysis)
    validate_campaign(performance, expected_pairs=180, expected_candidate=CANDIDATE_SHORT)
    validate_campaign(memory, expected_pairs=80, expected_candidate=CANDIDATE_SHORT)

    performance_bundle_sha256 = file_sha256(args.performance_bundle)
    memory_bundle_sha256 = file_sha256(args.memory_bundle)
    if performance_bundle_sha256 != PERFORMANCE_BUNDLE_SHA256:
        raise ValueError(f"unexpected performance bundle SHA-256: {performance_bundle_sha256}")
    if memory_bundle_sha256 != MEMORY_BUNDLE_SHA256:
        raise ValueError(f"unexpected memory bundle SHA-256: {memory_bundle_sha256}")

    performance_counts = campaign_counts(performance)
    memory_counts = campaign_counts(memory)
    if performance_counts != {"cases": 18, "pairs": 180, "observations": 360, "ab": 90, "ba": 90}:
        raise ValueError(f"unexpected performance campaign counts: {performance_counts}")
    if memory_counts != {"cases": 8, "pairs": 80, "observations": 160, "ab": 40, "ba": 40}:
        raise ValueError(f"unexpected memory campaign counts: {memory_counts}")

    data = {
        "schema": "mod17473-aws-plot-data-v2",
        "baseline_revision": performance["baseline_revision"],
        "candidate_revision": performance["pr_revision"],
        "host": {
            "instance": "3.16.40.116",
            "os": "Ubuntu",
            "cpu": "AMD EPYC 7R32",
            "physical_cpus": 8,
            "logical_cpus": 16,
            "memory_gib": 30,
            "swap_gib": 0,
        },
        "method": {
            "fresh_redis_process_per_observation": True,
            "bootstrap_iterations": BOOTSTRAP_ITERATIONS,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "performance_effect_estimator": "paired geometric mean",
            "memory_delta_estimator": "paired median",
        },
        "performance_campaign": {
            "campaign_id": performance["campaign_id"],
            "source_analysis_sha256": file_sha256(args.performance_analysis),
            "source_bundle_sha256": performance_bundle_sha256,
            **performance_counts,
            "plotted_equal_work_cases": len(PERFORMANCE_CASES),
            "plotted_pairs": len(PERFORMANCE_CASES) * 10,
            "plotted_observations": len(PERFORMANCE_CASES) * 20,
        },
        "memory_campaign": {
            "campaign_id": memory["campaign_id"],
            "source_analysis_sha256": file_sha256(args.memory_analysis),
            "source_bundle_sha256": memory_bundle_sha256,
            **memory_counts,
            "plotted_equal_work_cases": len(MEMORY_CASES),
            "plotted_pairs": len(MEMORY_CASES) * 10,
            "plotted_observations": len(MEMORY_CASES) * 20,
        },
        "performance_cases": prepare_performance_cases(performance),
        "memory_cases": prepare_memory_cases(memory),
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class Canvas:
    """Record drawing operations as accessible SVG and a matching PNG."""

    def __init__(self, width, height, title, description, render_png):
        self.width = width
        self.height = height
        self.title = title
        self.description = description
        self.svg = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            (
                f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
                f'viewBox="0 0 {width} {height}" role="img" '
                'aria-labelledby="plot-title plot-description">'
            ),
            f'<title id="plot-title">{html.escape(title)}</title>',
            f'<desc id="plot-description">{html.escape(description)}</desc>',
            f'<rect width="{width}" height="{height}" fill="{COLORS["white"]}"/>',
        ]
        if render_png and Image is None:
            raise RuntimeError(
                "PNG output requires Pillow; rerun with --svg-only or install Pillow"
            )
        self.image = Image.new("RGB", (width, height), COLORS["white"]) if render_png else None
        self.draw = ImageDraw.Draw(self.image) if render_png else None
        self.fonts = {}

    def font(self, size, bold=False):
        key = (size, bold)
        if key not in self.fonts:
            filename = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
            path = Path("/usr/share/fonts/truetype/dejavu") / filename
            if not path.exists():
                raise RuntimeError(f"required deterministic plot font is missing: {path}")
            self.fonts[key] = ImageFont.truetype(str(path), size=size)
        return self.fonts[key]

    def text(self, x, y, value, size=18, color=None, bold=False, anchor="start"):
        color = color or COLORS["ink"]
        svg_anchor = {"start": "start", "middle": "middle", "end": "end"}[anchor]
        weight = "700" if bold else "400"
        self.svg.append(
            f'<text x="{x:.2f}" y="{y:.2f}" fill="{color}" font-family="DejaVu Sans" '
            f'font-size="{size}" font-weight="{weight}" text-anchor="{svg_anchor}" '
            f'dominant-baseline="hanging">{html.escape(str(value))}</text>'
        )
        pil_anchor = {"start": "lt", "middle": "mt", "end": "rt"}[anchor]
        if self.draw is not None:
            self.draw.text(
                (round(x), round(y)),
                str(value),
                fill=color,
                font=self.font(size, bold=bold),
                anchor=pil_anchor,
            )

    def line(self, x1, y1, x2, y2, color, width=1):
        self.svg.append(
            f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" '
            f'stroke="{color}" stroke-width="{width}" stroke-linecap="round"/>'
        )
        if self.draw is not None:
            self.draw.line(
                (round(x1), round(y1), round(x2), round(y2)), fill=color, width=width
            )

    def rect(self, x, y, width, height, fill, outline=None, stroke_width=1):
        outline_attr = (
            "" if outline is None else f' stroke="{outline}" stroke-width="{stroke_width}"'
        )
        self.svg.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{width:.2f}" height="{height:.2f}" '
            f'fill="{fill}"{outline_attr}/>'
        )
        if self.draw is not None:
            self.draw.rectangle(
                (round(x), round(y), round(x + width), round(y + height)),
                fill=fill,
                outline=outline,
                width=stroke_width,
            )

    def circle(self, x, y, radius, fill, outline=None, stroke_width=1):
        outline_attr = (
            "" if outline is None else f' stroke="{outline}" stroke-width="{stroke_width}"'
        )
        self.svg.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}" fill="{fill}"'
            f'{outline_attr}/>'
        )
        if self.draw is not None:
            self.draw.ellipse(
                (round(x - radius), round(y - radius), round(x + radius), round(y + radius)),
                fill=fill,
                outline=outline,
                width=stroke_width,
            )

    def polygon(self, points, fill, outline=None, stroke_width=1):
        svg_points = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
        outline_attr = (
            "" if outline is None else f' stroke="{outline}" stroke-width="{stroke_width}"'
        )
        self.svg.append(f'<polygon points="{svg_points}" fill="{fill}"{outline_attr}/>')
        if self.draw is not None:
            rounded = [(round(x), round(y)) for x, y in points]
            self.draw.polygon(rounded, fill=fill)
            if outline:
                self.draw.line(rounded + [rounded[0]], fill=outline, width=stroke_width)

    def save(self, svg_path, png_path):
        self.svg.append("</svg>")
        Path(svg_path).write_text("\n".join(self.svg) + "\n", encoding="utf-8")
        if self.image is not None:
            self.image.save(png_path, format="PNG", optimize=True, compress_level=9)


def protocol_colors(protocol):
    if protocol == "RESP2":
        return COLORS["resp2"], COLORS["resp2_light"]
    return COLORS["resp3"], COLORS["resp3_light"]


def x_position(value, minimum, maximum, x, width):
    return x + (value - minimum) / (maximum - minimum) * width


def draw_marker(canvas, x, y, order, color):
    if order == "AB":
        canvas.circle(x, y, 6, color)
    else:
        canvas.rect(x - 6, y - 6, 12, 12, color)


def draw_ci(canvas, x1, x2, y, minimum, maximum, panel_x, panel_width, color):
    left_clipped = x1 < minimum
    right_clipped = x2 > maximum
    left = x_position(max(x1, minimum), minimum, maximum, panel_x, panel_width)
    right = x_position(min(x2, maximum), minimum, maximum, panel_x, panel_width)
    canvas.line(left, y, right, y, color, width=3)
    if left_clipped:
        canvas.polygon([(left, y), (left + 9, y - 6), (left + 9, y + 6)], color)
    else:
        canvas.line(left, y - 6, left, y + 6, color, width=2)
    if right_clipped:
        canvas.polygon([(right, y), (right - 9, y - 6), (right - 9, y + 6)], color)
    else:
        canvas.line(right, y - 6, right, y + 6, color, width=2)


def format_signed(value, digits=1, suffix=""):
    if abs(value) < 0.5 * 10 ** (-digits):
        return f"{0:.{digits}f}{suffix}"
    return f"{value:+.{digits}f}{suffix}"


def draw_panel_axes(canvas, panel, row_top, row_bottom):
    x, width, minimum, maximum, ticks = (
        panel["x"],
        panel["width"],
        panel["minimum"],
        panel["maximum"],
        panel["ticks"],
    )
    if panel.get("equivalence") is not None:
        threshold = panel["equivalence"]
        band_left = x_position(-threshold, minimum, maximum, x, width)
        band_right = x_position(threshold, minimum, maximum, x, width)
        canvas.rect(
            band_left,
            row_top,
            band_right - band_left,
            row_bottom - row_top,
            COLORS["band"],
        )
    for tick in ticks:
        tick_x = x_position(tick, minimum, maximum, x, width)
        canvas.line(tick_x, row_top, tick_x, row_bottom, COLORS["grid"], width=1)
        canvas.text(
            tick_x,
            row_bottom + 12,
            panel["tick_format"](tick),
            16,
            COLORS["sub"],
            anchor="middle",
        )
    zero_x = x_position(0, minimum, maximum, x, width)
    canvas.line(zero_x, row_top, zero_x, row_bottom, COLORS["zero"], width=3)
    canvas.text(
        x + width / 2,
        row_top - 76,
        panel["title"],
        23,
        COLORS["ink"],
        bold=True,
        anchor="middle",
    )
    canvas.text(
        x + width / 2,
        row_top - 44,
        panel["subtitle"],
        16,
        COLORS["sub"],
        anchor="middle",
    )


def draw_performance_plot(data, output_dir, render_png):
    width, height = 1800, 1320
    title = "MOD-17473 — AWS full-output performance"
    description = (
        "Equal-work performance comparison of baseline and array-backed replies. "
        "Every row shows ten paired candidate-versus-baseline effects, five AB and five "
        "BA, plus a "
        "paired-bootstrap confidence interval."
    )
    canvas = Canvas(width, height, title, description, render_png=render_png)
    canvas.text(40, 25, title, 38, bold=True)
    canvas.text(
        40,
        78,
        (
            f"Equal complete replies · baseline {BASELINE_SHORT} → candidate "
            f"{CANDIDATE_SHORT} · AWS Ubuntu / AMD EPYC 7R32"
        ),
        21,
        COLORS["sub"],
    )
    canvas.text(
        40,
        115,
        (
            "Every case: 10 paired runs = 10 baseline + 10 candidate measurements "
            "(20 fresh Redis processes); balanced 5 AB / 5 BA"
        ),
        20,
        COLORS["ink"],
        bold=True,
    )

    panels = (
        {
            "key": "throughput",
            "x": 390,
            "width": 620,
            "minimum": -12,
            "maximum": 4,
            "ticks": (-12, -9, -6, -3, 0, 3),
            "title": "Throughput effect",
            "subtitle": "candidate − baseline · positive = faster",
            "tick_format": lambda value: f"{value:+g}%",
            "equivalence": 3,
        },
        {
            "key": "latency_p95",
            "x": 1100,
            "width": 650,
            "minimum": -6,
            "maximum": 36,
            "ticks": (-6, 0, 6, 12, 18, 24, 30, 36),
            "title": "p95 latency effect",
            "subtitle": "candidate − baseline · positive = slower",
            "tick_format": lambda value: f"{value:+g}%",
            "equivalence": 3,
        },
    )
    row_top, row_step = 255, 70
    row_bottom = row_top + row_step * len(data["performance_cases"])
    for panel in panels:
        draw_panel_axes(canvas, panel, row_top, row_bottom)

    jitter = (-13.5, -10.5, -7.5, -4.5, -1.5, 1.5, 4.5, 7.5, 10.5, 13.5)
    for index, case in enumerate(data["performance_cases"]):
        y = row_top + row_step * index + row_step / 2
        canvas.text(40, y - 25, case["label"], 20, bold=True)
        canvas.text(40, y + 4, "10 pairs · 5 AB / 5 BA · 20 process runs", 15, COLORS["sub"])
        canvas.line(30, y + row_step / 2, 1760, y + row_step / 2, COLORS["row"])
        color, light = protocol_colors(case["protocol"])
        for panel in panels:
            metric = case["metrics"][panel["key"]]
            for point, order, offset in zip(
                metric["paired_percent"], metric["orders"], jitter
            ):
                clipped = min(max(point, panel["minimum"]), panel["maximum"])
                point_x = x_position(
                    clipped, panel["minimum"], panel["maximum"], panel["x"], panel["width"]
                )
                draw_marker(canvas, point_x, y + offset, order, light)
            ci_low, ci_high = metric["effect_ci95"]
            draw_ci(
                canvas,
                ci_low,
                ci_high,
                y,
                panel["minimum"],
                panel["maximum"],
                panel["x"],
                panel["width"],
                color,
            )
            estimate = metric["effect_percent"]
            estimate_x = x_position(
                min(max(estimate, panel["minimum"]), panel["maximum"]),
                panel["minimum"],
                panel["maximum"],
                panel["x"],
                panel["width"],
            )
            canvas.polygon(
                [
                    (estimate_x, y - 8),
                    (estimate_x + 8, y),
                    (estimate_x, y + 8),
                    (estimate_x - 8, y),
                ],
                color,
            )
            annotation = (
                f"{format_signed(estimate, suffix='%')}  "
                f"[{format_signed(ci_low, suffix='%')}, {format_signed(ci_high, suffix='%')}]"
            )
            canvas.text(
                panel["x"] + panel["width"] - 4,
                y - 29,
                annotation,
                16,
                color,
                bold=True,
                anchor="end",
            )

    legend_y = row_bottom + 56
    canvas.circle(48, legend_y + 8, 5, COLORS["muted"])
    canvas.text(63, legend_y, "AB run", 16, COLORS["sub"])
    canvas.rect(151, legend_y + 3, 10, 10, COLORS["muted"])
    canvas.text(174, legend_y, "BA run", 16, COLORS["sub"])
    canvas.polygon(
        [(272, legend_y), (280, legend_y + 8), (272, legend_y + 16), (264, legend_y + 8)],
        COLORS["resp2"],
    )
    canvas.text(292, legend_y, "RESP2 estimate", 16, COLORS["sub"])
    canvas.polygon(
        [(430, legend_y), (438, legend_y + 8), (430, legend_y + 16), (422, legend_y + 8)],
        COLORS["resp3"],
    )
    canvas.text(
        450,
        legend_y,
        "RESP3 estimate · whisker = 95% paired-bootstrap CI",
        16,
        COLORS["sub"],
    )
    canvas.text(
        40,
        legend_y + 38,
        (
            "Dots/squares are the 10 actual paired effects in every row. Blue band = ±3% "
            "equivalence. CI = 10,000 whole-pair resamples (seed 17473), not benchmark runs."
        ),
        16,
        COLORS["sub"],
    )
    campaign = data["performance_campaign"]
    canvas.text(
        40,
        legend_y + 72,
        (
            "Plotted equal-work subset: "
            f"{campaign['plotted_equal_work_cases']} cases × 10 pairs = "
            f"{campaign['plotted_pairs']} pairs / {campaign['plotted_observations']} observations. "
            f"Whole campaign: {campaign['cases']} cases × 10 = {campaign['pairs']} pairs / "
            f"{campaign['observations']} observations ({campaign['ab']} AB, {campaign['ba']} BA)."
        ),
        16,
        COLORS["ink"],
        bold=True,
    )
    canvas.text(
        40,
        legend_y + 104,
        (
            "Timeout cases are omitted because baseline and candidate returned different row "
            "distributions and therefore did unequal work."
        ),
        16,
        COLORS["worse"],
        bold=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    canvas.save(
        output_dir / "mod-17473-aws-full-output-performance.svg",
        output_dir / "mod-17473-aws-full-output-performance.png",
    )


def draw_memory_plot(data, output_dir, render_png):
    width, height = 1800, 1040
    title = "MOD-17473 — AWS full-output memory"
    description = (
        "Full-output memory comparison for four RESP and worker combinations. Every row "
        "shows ten paired candidate-minus-baseline deltas, five AB and five BA, with a "
        "paired-median bootstrap confidence interval."
    )
    canvas = Canvas(width, height, title, description, render_png=render_png)
    canvas.text(40, 25, title, 38, bold=True)
    canvas.text(
        40,
        78,
        (
            f"Complete wide 10k replies · baseline {BASELINE_SHORT} → candidate "
            f"{CANDIDATE_SHORT} · positive delta = candidate uses more memory"
        ),
        21,
        COLORS["sub"],
    )
    canvas.text(
        40,
        115,
        (
            "Every case: 10 paired runs = 10 baseline + 10 candidate measurements "
            "(20 fresh Redis processes); balanced 5 AB / 5 BA"
        ),
        20,
        COLORS["ink"],
        bold=True,
    )

    panels = (
        {
            "key": "allocator_peak",
            "x": 390,
            "width": 410,
            "minimum": -20,
            "maximum": 55,
            "ticks": (-20, 0, 20, 40),
            "title": "Allocator peak excursion",
            "subtitle": "load peak − pre-load · MiB",
            "tick_format": lambda value: f"{value:+g}",
            "digits": 1,
        },
        {
            "key": "allocator_live_10s",
            "x": 865,
            "width": 400,
            "minimum": -0.6,
            "maximum": 0.2,
            "ticks": (-0.6, -0.4, -0.2, 0, 0.2),
            "title": "Allocator live after 10s",
            "subtitle": "cooldown − pre-load · MiB",
            "tick_format": lambda value: f"{value:+.1f}",
            "digits": 4,
        },
        {
            "key": "rss_10s",
            "x": 1330,
            "width": 410,
            "minimum": -10,
            "maximum": 150,
            "ticks": (0, 50, 100, 150),
            "title": "Process RSS after 10s",
            "subtitle": "cooldown − pre-load · MiB",
            "tick_format": lambda value: f"{value:+g}",
            "digits": 1,
        },
    )
    row_top, row_step = 285, 135
    row_bottom = row_top + row_step * len(data["memory_cases"])
    for panel in panels:
        draw_panel_axes(canvas, panel, row_top, row_bottom)

    jitter = (-13.5, -10.5, -7.5, -4.5, -1.5, 1.5, 4.5, 7.5, 10.5, 13.5)
    for index, case in enumerate(data["memory_cases"]):
        y = row_top + row_step * index + row_step / 2
        canvas.text(40, y - 25, case["label"], 20, bold=True)
        canvas.text(40, y + 4, "10 pairs · 5 AB / 5 BA · 20 process runs", 15, COLORS["sub"])
        canvas.line(30, y + row_step / 2, 1760, y + row_step / 2, COLORS["row"])
        color, light = protocol_colors(case["protocol"])
        for panel in panels:
            metric = case["metrics"][panel["key"]]
            for point, order, offset in zip(
                metric["paired_absolute_deltas"], metric["orders"], jitter
            ):
                clipped = min(max(point, panel["minimum"]), panel["maximum"])
                point_x = x_position(
                    clipped, panel["minimum"], panel["maximum"], panel["x"], panel["width"]
                )
                draw_marker(canvas, point_x, y + offset, order, light)
            ci_low, ci_high = metric["median_delta_ci95"]
            draw_ci(
                canvas,
                ci_low,
                ci_high,
                y,
                panel["minimum"],
                panel["maximum"],
                panel["x"],
                panel["width"],
                color,
            )
            estimate = metric["median_delta"]
            digits = panel["digits"]
            estimate_x = x_position(
                min(max(estimate, panel["minimum"]), panel["maximum"]),
                panel["minimum"],
                panel["maximum"],
                panel["x"],
                panel["width"],
            )
            canvas.polygon(
                [
                    (estimate_x, y - 8),
                    (estimate_x + 8, y),
                    (estimate_x, y + 8),
                    (estimate_x - 8, y),
                ],
                color,
            )
            comparison = (
                f"{metric['baseline_median']:.{digits}f} → "
                f"{metric['candidate_median']:.{digits}f}; "
                f"median Δ {format_signed(estimate, digits=digits)} MiB"
            )
            canvas.text(
                panel["x"] + panel["width"] - 4,
                y - 30,
                comparison,
                16,
                color,
                bold=True,
                anchor="end",
            )
            canvas.text(
                panel["x"] + panel["width"] - 4,
                y + 23,
                (
                    f"95% CI [{format_signed(ci_low, digits=digits)}, "
                    f"{format_signed(ci_high, digits=digits)}] MiB"
                ),
                14,
                COLORS["sub"],
                anchor="end",
            )

    footer_y = row_bottom + 45
    canvas.circle(48, footer_y + 8, 5, COLORS["muted"])
    canvas.text(63, footer_y, "AB run", 16, COLORS["sub"])
    canvas.rect(151, footer_y + 3, 10, 10, COLORS["muted"])
    canvas.text(174, footer_y, "BA run", 16, COLORS["sub"])
    canvas.polygon(
        [(272, footer_y), (280, footer_y + 8), (272, footer_y + 16), (264, footer_y + 8)],
        COLORS["resp2"],
    )
    canvas.text(292, footer_y, "RESP2 median", 16, COLORS["sub"])
    canvas.polygon(
        [(420, footer_y), (428, footer_y + 8), (420, footer_y + 16), (412, footer_y + 8)],
        COLORS["resp3"],
    )
    canvas.text(440, footer_y, "RESP3 median · whisker = 95% bootstrap CI", 16, COLORS["sub"])
    canvas.text(
        40,
        footer_y + 36,
        (
            "Dots/squares are the 10 actual paired MiB deltas in every row. CI = 10,000 "
            "whole-pair resamples (seed 17473), not benchmark runs."
        ),
        16,
        COLORS["sub"],
    )
    campaign = data["memory_campaign"]
    canvas.text(
        40,
        footer_y + 70,
        (
            "Plotted equal-work subset: "
            f"{campaign['plotted_equal_work_cases']} cases × 10 pairs = "
            f"{campaign['plotted_pairs']} pairs / {campaign['plotted_observations']} observations. "
            f"Whole memory campaign: {campaign['cases']} cases × 10 = {campaign['pairs']} pairs / "
            f"{campaign['observations']} observations ({campaign['ab']} AB, {campaign['ba']} BA)."
        ),
        16,
        COLORS["ink"],
        bold=True,
    )
    canvas.text(
        40,
        footer_y + 102,
        (
            "Allocator-live found no retained live allocation after 10s; RSS residuals are "
            "consistent with allocator residency/high-water."
        ),
        16,
        COLORS["worse"],
        bold=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    canvas.save(
        output_dir / "mod-17473-aws-full-output-memory.svg",
        output_dir / "mod-17473-aws-full-output-memory.png",
    )


def validate_plot_data(data):
    if data.get("schema") != "mod17473-aws-plot-data-v2":
        raise ValueError(f"unexpected plot-data schema: {data.get('schema')!r}")
    for collection_name in ("performance_cases", "memory_cases"):
        for case in data[collection_name]:
            runs = case["runs"]
            expected = {
                "pairs": 10,
                "baseline_observations": 10,
                "candidate_observations": 10,
                "process_executions": 20,
                "ab": 5,
                "ba": 5,
            }
            if runs != expected:
                raise ValueError(f"{case['case']}: invalid run metadata: {runs}")


def render_plots(args):
    data = load_json(args.data)
    validate_plot_data(data)
    output_dir = Path(args.output_dir)
    render_png = not args.svg_only
    draw_performance_plot(data, output_dir, render_png=render_png)
    draw_memory_plot(data, output_dir, render_png=render_png)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare", help="extract compact plot data")
    prepare.add_argument("--performance-analysis", required=True)
    prepare.add_argument("--memory-analysis", required=True)
    prepare.add_argument("--performance-bundle", required=True)
    prepare.add_argument("--memory-bundle", required=True)
    prepare.add_argument("--output", required=True)
    prepare.set_defaults(func=prepare_data)

    render = commands.add_parser("render", help="render SVG and PNG plots")
    render.add_argument("--data", required=True)
    render.add_argument("--output-dir", required=True)
    render.add_argument(
        "--svg-only",
        action="store_true",
        help="skip PNG output so Pillow is not required",
    )
    render.set_defaults(func=render_plots)

    return parser.parse_args()


def main():
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
