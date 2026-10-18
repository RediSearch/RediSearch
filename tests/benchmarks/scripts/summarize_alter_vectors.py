#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Validate paired ALTER benchmark JSONL and write a descriptive Markdown report."""

import argparse
import json
import math
from pathlib import Path
import re
from statistics import median


COUNTERS = ("search_total_indexing_ops_vector_fields", "search_total_relabel_ops_vector_fields")
CONFIG = (
    "scenario", "documents", "dim", "preexisting_vector_fields", "seed", "workers",
    "optimize_partial_update", "poll_interval_seconds", "redis_version", "redis_git_sha1",
    "hostname",
)


def number(value, name, *, integer=False, positive=False):
    valid = type(value) is int if integer else type(value) in (int, float)
    if not valid or (type(value) is float and not math.isfinite(value)) or value < 0 or (positive and value == 0):
        raise ValueError(f"{name} must be a finite {'positive' if positive else 'non-negative'} "
                         f"{'integer' if integer else 'number'}")
    return value


def identity(record):
    for key in CONFIG:
        if key not in record:
            raise ValueError(f"missing configuration/runtime field {key}")
    for key in ("documents", "dim", "preexisting_vector_fields"):
        number(record[key], key, integer=True, positive=True)
    number(record["workers"], "workers", integer=True)
    if type(record["seed"]) is not int:
        raise ValueError("seed must be an integer")
    number(record["poll_interval_seconds"], "poll_interval_seconds", positive=True)
    if record["scenario"] not in ("add-vector", "add-tag"):
        raise ValueError("invalid scenario")
    if record["optimize_partial_update"] not in ("true", "false"):
        raise ValueError("invalid optimize_partial_update")
    for key in ("redis_version", "redis_git_sha1", "hostname"):
        if not isinstance(record[key], str) or not record[key]:
            raise ValueError(f"{key} must be a nonempty string")
    modules = record.get("modules")
    if not isinstance(modules, list) or not modules:
        raise ValueError("missing runtime modules")
    normalized = []
    for module in modules:
        if not isinstance(module, dict) or not {"name", "ver", "path", "args"} <= module.keys():
            raise ValueError("malformed runtime module")
        if (not isinstance(module["name"], str) or not module["name"]
                or type(module["ver"]) is not int or not isinstance(module["path"], str)
                or not isinstance(module["args"], list)
                or not all(isinstance(arg, str) for arg in module["args"])):
            raise ValueError("malformed runtime module")
        normalized.append({k: v for k, v in module.items()
                           if module["name"] != "search" or k not in ("path", "ver")})
    names = [module["name"] for module in modules]
    if names.count("search") != 1 or len(names) != len(set(names)):
        raise ValueError("runtime modules must include one search module and unique names")
    return tuple(record[key] for key in CONFIG), sorted(
        json.dumps(module, sort_keys=True) for module in normalized
    )


def search_version(record):
    return next(module["ver"] for module in record["modules"] if module["name"] == "search")


def read_trials(path, labels):
    trials = {}
    hashes = {}
    versions = {}
    runtime = None
    with Path(path).open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("record must be an object")
                label = record.get("label")
                if label not in labels:
                    raise ValueError(f"expected labels {labels}, got {label!r}")
                rate = number(record.get("writer_target_ops_per_second"), "writer target", integer=True)
                repetition = number(record.get("repetition"), "repetition", integer=True)
                key = label, rate, repetition
                if key in trials:
                    raise ValueError(f"duplicate label/rate/repetition {key}")
                current_runtime = identity(record)
                if runtime is not None and current_runtime != runtime:
                    raise ValueError("mixed benchmark configuration/runtime")
                runtime = current_runtime
                version = search_version(record)
                if label in versions and versions[label] != version:
                    raise ValueError(f"mixed Search module versions for {label}")
                versions[label] = version
                digest = record.get("module_sha256")
                if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    raise ValueError("module_sha256 must be a lowercase SHA-256 digest")
                if label in hashes and hashes[label] != digest:
                    raise ValueError(f"mixed module hashes for {label}")
                hashes[label] = digest
                number(record.get("scan_observed_seconds"), "scan_observed_seconds", positive=True)
                for field in ("writer_completed", "writer_missed_slots"):
                    number(record.get(field), field, integer=True)
                effective = number(record.get("writer_effective_ops_per_second"), "writer effective rate")
                expected = record["writer_completed"] / record["scan_observed_seconds"]
                if not math.isclose(effective, expected, rel_tol=1e-9, abs_tol=1e-9):
                    raise ValueError("writer effective rate disagrees with completed/elapsed")
                if not rate and (record["writer_completed"] or record["writer_missed_slots"]):
                    raise ValueError("zero writer target has writer activity")
                if rate:
                    first = number(record.get("writer_first_completion_seconds"), "first writer completion")
                    if not record["writer_completed"] or first >= record["scan_observed_seconds"]:
                        raise ValueError("no writer completion overlaps scan observation")
                deltas = record.get("counter_deltas")
                if not isinstance(deltas, dict):
                    raise ValueError("missing counter_deltas")
                for counter in COUNTERS:
                    number(deltas.get(counter), counter, integer=True)
                trials[key] = record
            except (ValueError, TypeError, KeyError) as error:
                raise ValueError(f"{path}:{line_number}: {error}") from error
    if not trials:
        raise ValueError(f"{path}: no trials")
    rates = sorted({key[1] for key in trials})
    repetitions = sorted({key[2] for key in trials})
    if repetitions != list(range(len(repetitions))):
        raise ValueError(f"{path}: incomplete repetitions; expected contiguous indices from zero")
    for label in labels:
        for rate in rates:
            for repetition in repetitions:
                if (label, rate, repetition) not in trials:
                    raise ValueError(f"{path}: incomplete pair: {(label, rate, repetition)}")
    return trials, hashes, runtime, rates, repetitions


def percent(before, after):
    return 100 * (after / before - 1)


def summarize(comparison, control, expected_repetitions=None, expected_writer_rates=None):
    main, hashes, runtime, rates, repetitions = read_trials(comparison, ("before", "after"))
    aa, aa_hashes, aa_runtime, aa_rates, aa_repetitions = read_trials(control, ("control_A", "control_B"))
    if runtime != aa_runtime:
        raise ValueError("main/control benchmark configuration/runtime differ")
    if 0 not in rates:
        raise ValueError("main comparison must include rate zero for the A/A baseline")
    if aa_rates != [0] or repetitions != aa_repetitions:
        raise ValueError("control must have rate zero and match main repetitions")
    if expected_repetitions is not None and len(repetitions) != expected_repetitions:
        raise ValueError("incomplete series: repetition count differs from expected")
    if expected_writer_rates is not None and rates != sorted(set(expected_writer_rates)):
        raise ValueError("incomplete series: writer rates differ from expected")
    if set(aa_hashes.values()) != {hashes["before"]}:
        raise ValueError("A/A controls must use the exact same module hash as before")
    baseline_version = search_version(main["before", 0, repetitions[0]])
    if any(search_version(record) != baseline_version for record in aa.values()):
        raise ValueError("A/A controls Search version must match before")
    configuration = next(iter(main.values()))
    lines = [
        "# ALTER vector benchmark", "",
        "Configuration/runtime: " + "; ".join(f"{key}={configuration[key]}" for key in CONFIG) + ".",
        "Elapsed is observed scan wall time (including concurrent work and polling).",
        "Derived change = 100 × (median after / median before − 1); negative means shorter.",
        "A/A uses the same formula for control_B / control_A. These are ratios of medians, "
        "not medians of paired changes.", "",
        "| Target writes/s | Pairs | Before median s | After median s | Derived change | "
        "A/A A median s | A/A B median s | A/A change |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for rate in rates:
        before, after = [median(main[label, rate, rep]["scan_observed_seconds"] for rep in repetitions)
                         for label in ("before", "after")]
        if rate == 0:
            a, b = [median(aa[label, 0, rep]["scan_observed_seconds"] for rep in repetitions)
                    for label in ("control_A", "control_B")]
            control_cells = f"{a:.6f} | {b:.6f} | {percent(a, b):+.2f}%"
        else:
            control_cells = "— | — | —"
        lines.append(f"| {rate} | {len(repetitions)} | {before:.6f} | {after:.6f} | "
                     f"{percent(before, after):+.2f}% | {control_cells} |")
    lines += ["", "Individual paired changes, derived as 100 × (second / first − 1):", "",
              "| Target writes/s | Repetition | Before → after | A/A A → B |",
              "| ---: | ---: | ---: | ---: |"]
    for rate in rates:
        for rep in repetitions:
            change = percent(main["before", rate, rep]["scan_observed_seconds"],
                             main["after", rate, rep]["scan_observed_seconds"])
            aa_cell = "—"
            if rate == 0:
                aa_change = percent(aa["control_A", 0, rep]["scan_observed_seconds"],
                                    aa["control_B", 0, rep]["scan_observed_seconds"])
                aa_cell = f"{aa_change:+.2f}%"
            lines.append(f"| {rate} | {rep} | {change:+.2f}% | {aa_cell} |")
    lines += ["", "Per-trial counts and achieved rates (relabels absent in a build are recorded "
              "by the driver as zero deltas):", "",
              "| Target writes/s | Rep | Label | Vector insert ops | Relabel ops | "
              "Writer completed | Achieved writes/s | Missed slots |",
              "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |"]
    for rate in rates:
        for rep in repetitions:
            for records, labels in ((main, ("before", "after")), (aa, ("control_A", "control_B"))):
                for label in labels:
                    if (label, rate, rep) not in records:
                        continue
                    record = records[label, rate, rep]
                    inserts, relabels = (record["counter_deltas"][key] for key in COUNTERS)
                    lines.append(f"| {rate} | {rep} | {label} | {inserts} | {relabels} | "
                                 f"{record['writer_completed']} | "
                                 f"{record['writer_effective_ops_per_second']:.2f} | "
                                 f"{record['writer_missed_slots']} |")
    lines += ["", "Module SHA-256:", "",
              f"- before = control_A = control_B: `{hashes['before']}`",
              f"- after: `{hashes['after']}`", "",
              "Small samples and observed control variation cannot establish a universal benefit "
              "or regression, a noise bound, or statistical significance.",
              "Validation checks complete observed pairs and contiguous repetitions. JSONL does "
              "not declare planned rates/repetitions, so uniformly missing trailing trials "
              "cannot be detected from these files alone; supply --expected-repetitions and "
              "--expected-writer-rates to check the planned main series. A/A controls "
              "cover only rate zero.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--control", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-repetitions", type=int)
    parser.add_argument("--expected-writer-rates", type=int, nargs="+")
    args = parser.parse_args()
    if args.expected_repetitions is not None and args.expected_repetitions <= 0:
        parser.error("expected repetitions must be positive")
    if args.expected_writer_rates is not None and min(args.expected_writer_rates) < 0:
        parser.error("expected writer rates must be non-negative")
    if args.output.resolve() in (args.comparison.resolve(), args.control.resolve()):
        parser.error("output must differ from input files")
    try:
        report = summarize(args.comparison, args.control, args.expected_repetitions,
                           args.expected_writer_rates)
        args.output.write_text(report, encoding="utf-8")
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
