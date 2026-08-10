#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Render deterministic SVG and PNG plots from cluster analysis JSON.

SVG output uses only the Python standard library. Matching PNG output requires
Pillow 10.2.0 and the fixed DejaVu Sans fonts named below.
"""

from __future__ import annotations

import argparse
import html
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Iterable

try:
    from PIL import Image, ImageColor, ImageDraw, ImageFont, __version__ as PILLOW_VERSION
except ModuleNotFoundError:
    Image = ImageColor = ImageDraw = ImageFont = None
    PILLOW_VERSION = None


SCHEMA = "mod17473-cluster-analysis-v2"
PAGE = "#f5f7fa"
PANEL = "#ffffff"
INK = "#17212b"
MUTED = "#5c6b7a"
GRID = "#d9e2ec"
ZERO = "#6f7d89"
AB = "#8a3f00"
BA = "#1677c8"
POINT = "#151b23"
CI = "#344454"
PINNED_PILLOW_VERSION = "10.2.0"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FONT_PATHS = {
    False: Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    True: Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
}


class PlotError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise PlotError(message)


class Canvas:
    def __init__(
        self,
        width: int,
        height: int,
        title: str,
        description: str,
        render_png: bool,
    ):
        self.width = width
        self.height = height
        self.items: list[str] = [
            f'<title id="svg-title">{html.escape(title)}</title>',
            f'<desc id="svg-desc">{html.escape(description)}</desc>',
            f'<rect width="{width}" height="{height}" fill="{PAGE}"/>',
        ]
        self.render_png = render_png
        self.fonts: dict[tuple[int, bool], Any] = {}
        if render_png:
            require(Image is not None, "PNG output requires Pillow 10.2.0; use --svg-only")
            require(
                PILLOW_VERSION == PINNED_PILLOW_VERSION,
                f"PNG output requires Pillow {PINNED_PILLOW_VERSION}, found {PILLOW_VERSION}",
            )
            for font_path in FONT_PATHS.values():
                require(font_path.is_file(), f"required deterministic font is missing: {font_path}")
            self.image = Image.new("RGBA", (width, height), PAGE)
            self.draw = ImageDraw.Draw(self.image)
        else:
            self.image = None
            self.draw = None

    def font(self, size: int, bold: bool) -> Any:
        key = (size, bold)
        if key not in self.fonts:
            try:
                self.fonts[key] = ImageFont.truetype(str(FONT_PATHS[bold]), size=size)
            except OSError as exc:
                raise PlotError(f"cannot load deterministic font {FONT_PATHS[bold]}: {exc}") from exc
        return self.fonts[key]

    def translucent_marker(
        self,
        box: tuple[float, float, float, float],
        fill: str,
        opacity: float,
        ellipse: bool,
        stroke: str | None = None,
    ) -> None:
        left = max(0, math.floor(box[0]) - 1)
        top = max(0, math.floor(box[1]) - 1)
        right = min(self.width, math.ceil(box[2]) + 1)
        bottom = min(self.height, math.ceil(box[3]) + 1)
        overlay = Image.new("RGBA", (right - left, bottom - top), (0, 0, 0, 0))
        overlay_draw = ImageDraw.Draw(overlay)
        local_box = (box[0] - left, box[1] - top, box[2] - left, box[3] - top)
        alpha = round(255 * opacity)
        kwargs = {
            "fill": (*ImageColor.getrgb(fill), alpha),
            "outline": (*ImageColor.getrgb(stroke), alpha) if stroke else None,
            "width": 1,
        }
        if ellipse:
            overlay_draw.ellipse(local_box, **kwargs)
        else:
            overlay_draw.rectangle(local_box, **kwargs)
        self.image.alpha_composite(overlay, dest=(left, top))
        self.draw = ImageDraw.Draw(self.image)

    def rect(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        fill: str,
        stroke: str | None = None,
        radius: float = 0,
    ) -> None:
        attrs = (
            f'x="{x:.2f}" y="{y:.2f}" width="{width:.2f}" height="{height:.2f}" '
            f'fill="{fill}"'
        )
        if stroke:
            attrs += f' stroke="{stroke}" stroke-width="1"'
        if radius:
            attrs += f' rx="{radius:.2f}"'
        self.items.append(f"<rect {attrs}/>")
        if self.draw is not None:
            box = (x, y, x + width, y + height)
            if radius:
                self.draw.rounded_rectangle(
                    box,
                    radius=round(radius),
                    fill=fill,
                    outline=stroke,
                    width=1,
                )
            else:
                self.draw.rectangle(box, fill=fill, outline=stroke, width=1)

    def line(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        color: str,
        width: float = 1,
        dash: str | None = None,
    ) -> None:
        attrs = (
            f'x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" '
            f'stroke="{color}" stroke-width="{width:.2f}"'
        )
        if dash:
            attrs += f' stroke-dasharray="{dash}"'
        self.items.append(f"<line {attrs}/>")
        if self.draw is not None:
            require(dash is None, "PNG renderer does not support dashed lines")
            self.draw.line(
                (x1, y1, x2, y2),
                fill=color,
                width=max(1, round(width)),
            )

    def circle(
        self,
        x: float,
        y: float,
        radius: float,
        fill: str,
        stroke: str | None = None,
        opacity: float = 1.0,
    ) -> None:
        attrs = (
            f'cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}" fill="{fill}" '
            f'opacity="{opacity:.2f}"'
        )
        if stroke:
            attrs += f' stroke="{stroke}" stroke-width="1"'
        self.items.append(f"<circle {attrs}/>")
        if self.draw is not None:
            box = (x - radius, y - radius, x + radius, y + radius)
            if opacity < 1.0:
                self.translucent_marker(box, fill, opacity, True, stroke)
            else:
                self.draw.ellipse(box, fill=fill, outline=stroke, width=1)

    def diamond(
        self,
        x: float,
        y: float,
        radius: float,
        fill: str,
    ) -> None:
        points = (
            f"{x:.2f},{y-radius:.2f} {x+radius:.2f},{y:.2f} "
            f"{x:.2f},{y+radius:.2f} {x-radius:.2f},{y:.2f}"
        )
        self.items.append(
            f'<polygon points="{points}" fill="{fill}" stroke="#ffffff" stroke-width="1"/>'
        )
        if self.draw is not None:
            polygon = [
                (x, y - radius),
                (x + radius, y),
                (x, y + radius),
                (x - radius, y),
            ]
            self.draw.polygon(polygon, fill=fill)
            self.draw.line(polygon + [polygon[0]], fill="#ffffff", width=1)

    def square(
        self,
        x: float,
        y: float,
        radius: float,
        fill: str,
        opacity: float = 1.0,
    ) -> None:
        self.items.append(
            f'<rect x="{x-radius:.2f}" y="{y-radius:.2f}" width="{2*radius:.2f}" '
            f'height="{2*radius:.2f}" fill="{fill}" opacity="{opacity:.2f}"/>'
        )
        if self.draw is not None:
            box = (x - radius, y - radius, x + radius, y + radius)
            if opacity < 1.0:
                self.translucent_marker(box, fill, opacity, False)
            else:
                self.draw.rectangle(box, fill=fill)

    def text(
        self,
        x: float,
        y: float,
        value: str,
        size: int,
        color: str = INK,
        anchor: str = "start",
        bold: bool = False,
    ) -> None:
        weight = "700" if bold else "400"
        self.items.append(
            f'<text x="{x:.2f}" y="{y:.2f}" fill="{color}" font-size="{size}" '
            f'font-family="DejaVu Sans,Arial,sans-serif" font-weight="{weight}" '
            f'text-anchor="{anchor}" dominant-baseline="middle">{html.escape(value)}</text>'
        )
        if self.draw is not None:
            pillow_anchor = {"start": "lm", "middle": "mm", "end": "rm"}[anchor]
            self.draw.text(
                (x, y),
                value,
                fill=color,
                font=self.font(size, bold),
                anchor=pillow_anchor,
            )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        document = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" '
            f'height="{self.height}" viewBox="0 0 {self.width} {self.height}" role="img" '
            'aria-labelledby="svg-title svg-desc">\n'
            + "\n".join(self.items)
            + "\n</svg>\n"
        )
        path.write_text(document, encoding="utf-8")
        if self.image is not None:
            self.image.convert("RGB").save(
                path.with_suffix(".png"),
                format="PNG",
                optimize=True,
                compress_level=9,
            )


def load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PlotError(f"cannot read analysis JSON {path}: {exc}") from exc
    require(isinstance(value, dict) and value.get("schema") == SCHEMA, "analysis schema mismatch")
    require(value.get("validation", {}).get("valid") is True, "analysis is not validated")
    return value


def validate_final(data: dict[str, Any], allow_nonfinal: bool) -> None:
    validation = data["validation"]
    if allow_nonfinal:
        return
    require(validation.get("profile") == "final", "refusing to plot a non-final analysis")
    require(validation.get("case_count") == 18, "final analysis must contain 18 cases")
    require(validation.get("full_case_count") == 12, "final analysis must contain 12 full cases")
    require(validation.get("timeout_case_count") == 6, "final analysis must contain 6 timeout cases")
    require(validation.get("repetitions_per_case") == 10, "final analysis must have ten pairs per case")
    accounting = data.get("run_accounting", {})
    require(
        (
            accounting.get("measured_pairs"),
            accounting.get("measured_topology_observations"),
            accounting.get("redis_process_executions"),
        )
        == (180, 360, 720),
        "final run accounting must be 180 pairs / 360 topology observations / 720 Redis processes",
    )
    require(accounting.get("subsets", {}).get("full", {}).get("pairs") == 120, "full subset accounting mismatch")
    require(accounting.get("subsets", {}).get("timeout", {}).get("pairs") == 60, "timeout subset accounting mismatch")
    source_bundle = data.get("campaign", {}).get("source_bundle_sha256")
    require(
        isinstance(source_bundle, str) and SHA256_RE.fullmatch(source_bundle) is not None,
        "final analysis must record the externally computed source-bundle SHA256",
    )
    for name, orders in validation.get("orders_by_case", {}).items():
        require(orders == {"AB": 5, "BA": 5}, f"{name}: expected 5 AB and 5 BA pairs")
    require(len(validation.get("orders_by_case", {})) == 18, "final order accounting must cover 18 cases")


def fmt_percent(value: float) -> str:
    normalized = 0.0 if abs(value) < 0.0005 else value
    return f"{normalized:+.1f}%".replace("-", "−")


def fmt_number(value: float, suffix: str = "") -> str:
    value = 0.0 if abs(value) < 0.0005 else value
    absolute = abs(value)
    if absolute >= 1000:
        text = f"{value:+,.0f}"
    elif absolute >= 10:
        text = f"{value:+.1f}"
    else:
        text = f"{value:+.2f}"
    return (text + suffix).replace("-", "−")


def nice_domain(values: Iterable[float], tick_count: int = 5) -> tuple[float, float, list[float]]:
    items = [float(value) for value in values if math.isfinite(float(value))]
    require(bool(items), "plot has no finite values")
    low = min(min(items), 0.0)
    high = max(max(items), 0.0)
    if math.isclose(low, high):
        low -= 1.0
        high += 1.0
    span = high - low
    raw = span / max(1, tick_count)
    magnitude = 10 ** math.floor(math.log10(raw))
    normalized = raw / magnitude
    if normalized <= 1:
        step = magnitude
    elif normalized <= 2:
        step = 2 * magnitude
    elif normalized <= 2.5:
        step = 2.5 * magnitude
    elif normalized <= 5:
        step = 5 * magnitude
    else:
        step = 10 * magnitude
    low = math.floor((low - span * 0.06) / step) * step
    high = math.ceil((high + span * 0.06) / step) * step
    ticks: list[float] = []
    value = low
    while value <= high + step / 2:
        ticks.append(round(value, 10))
        value += step
    return low, high, ticks


def xmap(value: float, low: float, high: float, left: float, right: float) -> float:
    return left + (value - low) / (high - low) * (right - left)


def title_block(
    canvas: Canvas,
    title: str,
    subtitle: str,
    accounting: dict[str, Any],
    subset: str,
) -> None:
    canvas.text(54, 44, title, 26, bold=True)
    canvas.text(54, 78, subtitle, 16, color=MUTED)
    per_case = accounting["per_case"]
    per_case_detail = (
        f"Per case: {per_case['pairs']} baseline/PR pairs = {per_case['topology_observations']} fresh "
        f"two-primary topology observations = {per_case['redis_process_executions']} Redis process executions."
    )
    subset_counts = accounting["subsets"][subset]
    campaign_detail = (
        f"This figure: {subset_counts['pairs']} pairs / {subset_counts['topology_observations']} topology "
        f"observations / {subset_counts['redis_process_executions']} Redis processes. Whole campaign: "
        f"{accounting['measured_pairs']} pairs / {accounting['measured_topology_observations']} topology "
        f"observations / {accounting['redis_process_executions']} Redis processes."
    )
    canvas.text(54, 104, per_case_detail, 15, color=MUTED)
    canvas.text(54, 126, campaign_detail, 15, color=MUTED)


def legend(canvas: Canvas, x: float, y: float) -> None:
    canvas.circle(x, y, 5, AB)
    canvas.text(x + 12, y, "AB = baseline first", 15, color=MUTED)
    canvas.square(x + 178, y, 5, BA)
    canvas.text(x + 190, y, "BA = PR first", 15, color=MUTED)
    canvas.line(x + 330, y, x + 370, y, CI, 2)
    canvas.diamond(x + 350, y, 6, POINT)
    canvas.text(x + 382, y, "paired effect + 95% order-stratified whole-pair bootstrap CI", 15, color=MUTED)


def row_label(entry: dict[str, Any]) -> str:
    worker_path = "shard-inline" if entry["workers"] == 0 else "shard-deferred"
    return f"RESP{entry['protocol']} · W{entry['workers']} {worker_path}"


def effect_values(entry: dict[str, Any], kind: str) -> tuple[float, list[float], list[dict[str, Any]], str]:
    if kind == "ratio":
        return (
            float(entry["effect_percent"]),
            [float(value) for value in entry["ci95_percent"]],
            entry["raw"],
            "effect_percent",
        )
    return (
        float(entry["effect"]),
        [float(value) for value in entry["ci95"]],
        entry["raw"],
        "effect",
    )


def draw_forest_column(
    canvas: Canvas,
    entries: list[dict[str, Any]],
    x: float,
    y: float,
    width: float,
    height: float,
    header: str,
    kind: str,
    low: float,
    high: float,
    ticks: list[float],
    tick_suffix: str,
) -> None:
    canvas.rect(x, y, width, height, PANEL, GRID, 7)
    canvas.text(x + width / 2, y + 20, header, 16, anchor="middle", bold=True)
    label_width = 210
    plot_left = x + label_width
    plot_right = x + width - 34
    plot_top = y + 40
    plot_bottom = y + height - 36
    row_height = (plot_bottom - plot_top) / max(1, len(entries))
    for tick in ticks:
        px = xmap(tick, low, high, plot_left, plot_right)
        canvas.line(px, plot_top, px, plot_bottom, ZERO if tick == 0 else GRID, 1.4 if tick == 0 else 1)
        normalized_tick = 0.0 if abs(tick) < 1e-10 else tick
        canvas.text(px, y + height - 18, f"{normalized_tick:g}{tick_suffix}", 13, color=MUTED, anchor="middle")
    for index, entry in enumerate(entries):
        center = plot_top + (index + 0.5) * row_height
        canvas.text(x + 10, center, row_label(entry), 14, color=INK)
        point, interval, raw, raw_key = effect_values(entry, kind)
        raw_radius = 4.2
        jitter_step = (
            max(0.0, row_height - 2 * raw_radius - 2) / (len(raw) - 1)
            if len(raw) > 1
            else 0.0
        )
        for raw_index, raw_point in enumerate(raw):
            offset = (raw_index - (len(raw) - 1) / 2) * jitter_step
            raw_x = xmap(float(raw_point[raw_key]), low, high, plot_left, plot_right)
            if raw_point["order"] == "AB":
                canvas.circle(raw_x, center + offset, raw_radius, AB)
            else:
                canvas.square(raw_x, center + offset, raw_radius, BA)
        canvas.line(
            xmap(interval[0], low, high, plot_left, plot_right),
            center,
            xmap(interval[1], low, high, plot_left, plot_right),
            center,
            CI,
            2.2,
        )
        point_x = xmap(point, low, high, plot_left, plot_right)
        canvas.diamond(point_x, center, 6.2, POINT)
        effect_label = fmt_percent(point) if kind == "ratio" else fmt_number(point)
        if point_x < (plot_left + plot_right) / 2:
            canvas.text(point_x + 9, center - 9, effect_label, 13, color=INK)
        else:
            canvas.text(point_x - 9, center - 9, effect_label, 13, color=INK, anchor="end")


def metric_domain(
    entries: list[dict[str, Any]], kind: str, tick_count: int = 5
) -> tuple[float, float, list[float]]:
    values: list[float] = [0.0]
    for entry in entries:
        point, interval, raw, raw_key = effect_values(entry, kind)
        values += [point, *interval]
        values += [float(item[raw_key]) for item in raw]
    return nice_domain(values, tick_count)


def sparse_ticks(low: float, high: float) -> list[float]:
    ticks = [low]
    if low < 0 < high:
        ticks.append(0.0)
    if not math.isclose(high, ticks[-1]):
        ticks.append(high)
    return ticks


def performance_plot(data: dict[str, Any], path: Path, render_png: bool) -> None:
    metrics = (
        ("throughput_rps", "Complete-reply throughput", "positive = faster"),
        ("latency_p50_ms", "Complete-reply p50 latency", "positive = slower"),
        ("latency_p95_ms", "Complete-reply p95 latency", "positive = slower"),
        ("latency_p99_ms", "Complete-reply p99 latency", "positive = slower"),
    )
    width = 1680
    section_height = 280
    height = 180 + len(metrics) * section_height + 110
    title = "MOD-17473 · Native OSS cluster · full-result performance"
    canvas = Canvas(
        width,
        height,
        title,
        "Paired throughput and p50/p95/p99 latency effects for equal-row-work cluster replies, faceted by per-primary SEARCH_THREADS.",
        render_png,
    )
    title_block(
        canvas,
        title,
        "Geomean of per-pair PR/baseline ratios, shown as (PR/baseline − 1) × 100%; equal 10k-row work and complete-reply hiredis timing.",
        data["run_accounting"],
        "full",
    )
    legend(canvas, 54, 152)
    all_rows = data["full_performance"]
    for metric_index, (metric, label, direction) in enumerate(metrics):
        top = 178 + metric_index * section_height
        canvas.text(54, top + 12, f"{label} · {direction}", 17, bold=True)
        selected = [entry for entry in all_rows if entry["metric"] == metric]
        low, high, ticks = metric_domain(selected, "ratio")
        for facet_index, threads in enumerate((1, 4)):
            entries = sorted(
                (entry for entry in selected if entry["search_threads"] == threads),
                key=lambda entry: (entry["protocol"], entry["workers"]),
            )
            if not entries:
                continue
            draw_forest_column(
                canvas,
                entries,
                54 + facet_index * 806,
                top + 30,
                772,
                232,
                f"SEARCH_THREADS={threads} per primary · n={entries[0]['n_pairs']} pairs/row "
                f"({entries[0]['ab_pairs']} AB, {entries[0]['ba_pairs']} BA)",
                "ratio",
                low,
                high,
                ticks,
                " %",
            )
    canvas.text(
        54,
        height - 50,
        "Public requests are coordinator-async/blocked in every case; W0 is shard-inline and W1/W4 are shard-deferred.",
        15,
        color=MUTED,
    )
    canvas.text(
        54,
        height - 27,
        f"CIs: {data['bootstrap']['iterations']:,} order-stratified whole-pair resamples, seed {data['bootstrap']['seed']}; resamples are not benchmark runs.",
        15,
        color=MUTED,
    )
    canvas.save(path)


def memory_plot(
    data: dict[str, Any],
    path: Path,
    scopes: tuple[str, ...],
    title: str,
    render_png: bool,
) -> None:
    metric_labels = {
        "redis_allocator_allocated_bytes": "Redis allocator allocated",
        "smaps_Pss_bytes": "Process proportional set size (PSS)",
    }
    sections = [(scope, metric) for scope in scopes for metric in metric_labels]
    width = 1680
    section_height = 468
    height = 180 + len(sections) * section_height + 134
    scope_note = (
        "Cluster totals are same-sample sums across both primaries."
        if scopes == ("cluster",)
        else "Diagnostic only: node0 is both ingress/coordinator and a shard; node1 is the peer shard."
    )
    canvas = Canvas(
        width,
        height,
        title,
        f"Paired absolute memory deltas for full equal-row-work cases. {scope_note}",
        render_png,
    )
    title_block(
        canvas,
        title,
        f"Full equal-row-work only. {scope_note} Peak and final cooldown are relative to pre-load; positive means more PR memory.",
        data["run_accounting"],
        "full",
    )
    legend(canvas, 54, 152)
    rows = data["full_memory"]
    scope_labels = {"cluster": "Cluster total", "node0": "Node0 ingress", "node1": "Node1 peer"}
    for section_index, (scope, metric) in enumerate(sections):
        top = 178 + section_index * section_height
        canvas.text(54, top + 10, f"{scope_labels[scope]} · {metric_labels[metric]}", 17, bold=True)
        selected = [entry for entry in rows if entry["scope"] == scope and entry["metric"] == metric]
        peak_entries = [entry for entry in selected if entry["view"] == "peak_excursion_delta"]
        cooldown_entries = [entry for entry in selected if entry["view"] == "cooldown_residual_delta"]
        peak_domain = metric_domain(peak_entries, "delta", 3)
        cooldown_domain = metric_domain(cooldown_entries, "delta", 3)
        peak_domain = (peak_domain[0], peak_domain[1], sparse_ticks(peak_domain[0], peak_domain[1]))
        cooldown_domain = (
            cooldown_domain[0],
            cooldown_domain[1],
            sparse_ticks(cooldown_domain[0], cooldown_domain[1]),
        )
        for view_index, (view, kind, domain, label) in enumerate(
            (
                ("peak_excursion_delta", "delta", peak_domain, "Observed peak"),
                ("cooldown_residual_delta", "delta", cooldown_domain, "Final cooldown"),
            )
        ):
            for facet_index, threads in enumerate((1, 4)):
                entries = sorted(
                    (
                        entry
                        for entry in selected
                        if entry["view"] == view and entry["search_threads"] == threads
                    ),
                    key=lambda entry: (entry["protocol"], entry["workers"]),
                )
                if not entries:
                    continue
                draw_forest_column(
                    canvas,
                    entries,
                    54 + facet_index * 806,
                    top + 28 + view_index * 216,
                    772,
                    202,
                    f"{label} · SEARCH_THREADS={threads}",
                    kind,
                    domain[0],
                    domain[1],
                    domain[2],
                    " MiB",
                )
        canvas.text(
            width - 54,
            top + 12,
            "10 pairs/row · 5 AB / 5 BA" if data["validation"]["repetitions_per_case"] == 10 else f"{data['validation']['repetitions_per_case']} pair(s)/row",
            13,
            color=MUTED,
            anchor="end",
        )
    canvas.text(
        54,
        height - 73,
        "Effects are medians of paired PR−baseline MiB deltas; cooldown may be negative and positive means more PR memory.",
        15,
        color=MUTED,
    )
    canvas.text(
        54,
        height - 50,
        "50 ms sampler ticks; allocator sampled about every 100 ms and PSS about every 200 ms. "
        "The sampler shares one load-generator physical core symmetrically.",
        15,
        color=MUTED,
    )
    canvas.text(
        54,
        height - 27,
        "RSS remains in JSON but is omitted from figures because process sums double-count shared pages. "
        f"CIs: {data['bootstrap']['iterations']:,} order-stratified whole-pair resamples.",
        15,
        color=MUTED,
    )
    canvas.save(path)


def timeout_plot(data: dict[str, Any], path: Path, render_png: bool) -> None:
    metrics = (
        ("rows_per_reply", "Rows returned per reply", " rows"),
        ("reply_mib_per_reply", "Wire reply size per reply", " MiB"),
        (
            "timeout_evidence_percentage_points",
            "Timeout evidence (RESP2 partial reply; RESP3 timeout-warning reply)",
            " pp",
        ),
    )
    width = 1600
    section_height = 252
    height = 182 + len(metrics) * section_height + 256
    title = "MOD-17473 · Native OSS cluster · calibrated-timeout diagnostics"
    canvas = Canvas(
        width,
        height,
        title,
        "Unequal-work timeout diagnostics showing paired row, wire-byte, and protocol-specific timeout-evidence deltas; no throughput or latency comparison.",
        render_png,
    )
    title_block(
        canvas,
        title,
        "Unequal returned work: descriptive PR − baseline paired medians only. No RPS or latency effect is shown or implied.",
        data["run_accounting"],
        "timeout",
    )
    legend(canvas, 54, 152)
    cases = sorted(data["timeout_diagnostics"], key=lambda entry: (entry["protocol"], entry["workers"]))
    for metric_index, (metric, label, suffix) in enumerate(metrics):
        top = 180 + metric_index * section_height
        canvas.text(54, top + 10, f"{label} · PR − baseline", 17, bold=True)
        entries: list[dict[str, Any]] = []
        for case in cases:
            row = case["metrics"][metric]
            entries.append(
                {
                    "protocol": case["protocol"],
                    "workers": case["workers"],
                    "search_threads": case["search_threads"],
                    "n_pairs": case["n_pairs"],
                    "effect": row["effect"],
                    "ci95": row["ci95"],
                    "raw": row["raw"],
                }
            )
        low, high, ticks = metric_domain(entries, "delta")
        draw_forest_column(
            canvas,
            entries,
            54,
            top + 28,
            width - 108,
            208,
            f"SEARCH_THREADS=4 per primary · n={entries[0]['n_pairs']} pairs/row",
            "delta",
            low,
            high,
            ticks,
            suffix,
        )
    calibration_top = 180 + len(metrics) * section_height
    canvas.text(54, calibration_top + 12, "Frozen baseline-only timeout calibration", 17, bold=True)
    canvas.rect(54, calibration_top + 34, width - 108, 160, PANEL, GRID, 7)
    timeout_values = [float(case["calibrated_timeout_ms"]) for case in cases]
    _unclamped_low, high, ticks = nice_domain([0.0, *timeout_values])
    low = 0.0
    ticks = [tick for tick in ticks if tick >= 0]
    if not ticks or ticks[0] != 0:
        ticks.insert(0, 0.0)
    plot_left = 180
    plot_right = width - 78
    for tick in ticks:
        px = xmap(tick, low, high, plot_left, plot_right)
        canvas.line(px, calibration_top + 58, px, calibration_top + 164, GRID if tick else ZERO)
        canvas.text(px, calibration_top + 180, f"{tick:g} ms", 13, color=MUTED, anchor="middle")
    for index, case in enumerate(cases):
        y = calibration_top + 67 + index * 16
        canvas.text(70, y, row_label(case), 14)
        x = xmap(float(case["calibrated_timeout_ms"]), low, high, plot_left, plot_right)
        canvas.diamond(x, y, 5, POINT)
        canvas.text(x + 9, y, f"{case['calibrated_timeout_ms']} ms", 13, color=MUTED)
    calibration = data["run_accounting"]["timeout_calibration"]
    canvas.text(
        54,
        height - 28,
        f"{calibration['baseline_topology_starts']} baseline-only calibration topology starts / "
        f"{calibration['redis_process_executions']} Redis processes / "
        f"{calibration['probe_client_invocations']} timeout-budget probe client invocations "
        f"are excluded from N. CIs use {data['bootstrap']['iterations']:,} order-stratified whole-pair resamples.",
        15,
        color=MUTED,
    )
    canvas.save(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("analysis", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prefix", default="mod-17473-aws-cluster")
    parser.add_argument("--allow-nonfinal", action="store_true")
    parser.add_argument(
        "--svg-only",
        action="store_true",
        help="skip PNG output so Pillow is not required",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        data = load(args.analysis)
        validate_final(data, args.allow_nonfinal)
        output = args.output_dir.expanduser().resolve()
        render_png = not args.svg_only
        performance_plot(
            data,
            output / f"{args.prefix}-full-output-performance.svg",
            render_png,
        )
        memory_plot(
            data,
            output / f"{args.prefix}-full-output-memory.svg",
            ("cluster",),
            "MOD-17473 · Native OSS cluster · cluster-total memory",
            render_png,
        )
        memory_plot(
            data,
            output / f"{args.prefix}-node-memory-diagnostic.svg",
            ("node0", "node1"),
            "MOD-17473 · Native OSS cluster · per-node memory diagnostic",
            render_png,
        )
        timeout_plot(data, output / f"{args.prefix}-timeout-diagnostic.svg", render_png)
    except PlotError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    formats = "SVG and PNG" if render_png else "SVG"
    print(f"wrote four {formats} plots under {output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
