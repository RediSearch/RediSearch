#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Validate and analyze the MOD-17473 two-primary cluster campaign.

The default profile is deliberately fail-closed: it accepts only the complete
18-case, ten-pair campaign.  ``--profile environment`` exists so a smoke bundle
can exercise the same schema and integrity checks without being mistaken for a
final result.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable


SCHEMA = "mod17473-cluster-analysis-v2"
CASE_RE = re.compile(
    r"^(full|timeout)-wide10000-r(2|3)-c32-w(0|1|4)-ct(1|4)$"
)
MIB = 1024.0 * 1024.0
PERFORMANCE_METRICS = {
    "throughput_rps": ("throughput_rps", "req/s", "higher"),
    "latency_p50_ms": ("latency_ms.p50", "ms", "lower"),
    "latency_p95_ms": ("latency_ms.p95", "ms", "lower"),
    "latency_p99_ms": ("latency_ms.p99", "ms", "lower"),
}
MEMORY_METRICS = (
    "redis_allocator_allocated_bytes",
    "smaps_Pss_bytes",
    "proc_VmRSS_bytes",
)
MEMORY_SCOPES = ("cluster", "node0", "node1")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SEARCH_SHARD_RE = re.compile(
    r"RespValue\(kind='simple', value=b'slots'\), "
    r"RespValue\(kind='array', value=\["
    r"RespValue\(kind='integer', value=(\d+)\), "
    r"RespValue\(kind='integer', value=(\d+)\)\]\)"
    r", RespValue\(kind='simple', value=b'id'\), "
    r"RespValue\(kind='simple', value=b'[0-9a-f]{40}'\), "
    r"RespValue\(kind='simple', value=b'host'\), "
    r"RespValue\(kind='simple', value=b'127\.0\.0\.1'\), "
    r"RespValue\(kind='simple', value=b'port'\), "
    r"RespValue\(kind='integer', value=(\d+)\)",
)

EXPECTED_PROVENANCE = {
    "baseline_revision": "20ba4ed7c2a90dd72b685275a0aca993412591af",
    "baseline_sha256": "eb9a0950d6f9fb70ae62d5314c4860cc8c77d75f44070745bfa314eac3110e94",
    "pr_revision": "a60f2a500805b3160b6be0213874236fca9a4ed9",
    "pr_sha256": "6a54643d25b0fb1da794b799ab1a1f471b56f16ca9475e736ad8236f244048da",
    "redis_server_source_revision": "3869512c920d4e865b54384bce5fcb6a4e06ae0d",
    "redis_server_sha256": "974a0e446b6e9d9d6f6bfb8dbdc5b106bbbdb64ead5cb4e95b03751cdd3c5e26",
    "cluster_seed_descriptor_sha256": "34c894c8a196c312b92eae28a6d970655cb9ff8f4d62b864a3d61ba6154d725f",
    "logical_dataset_sha256": "559d4156bcdedcaf588fb00920398211ea5fc5b667e82222f99062c0cfd116fa",
    "hiredis_load_client_sha256": "3348516d20fbc627b2a5100e89edc561fcd0627d8ab300be4297156defe2d669",
    "hiredis_load_client_source_sha256": "d6ee1da82663a0a8b0f3a9f9227aa7a918bbe6630784999d2d9b6f034ba7e068",
    "hiredis_static_archive_sha256": "9295cc7b2322d26ffefca03f283cef7520cc6be65ceaa80f02509cb694957339",
    "hiredis_source_tree": "a01f4ee70f3cd1fff17a38c1803dc9cf4ac5998e",
    "hiredis_version": "1.2.0",
}

EXPECTED_HARNESS_SCRIPTS = {
    "mod17473_benchmark_cluster.py": "6e177516b40305b3e058336fadc1d489da16de11b5ec86753cddf4997a10ee62",
    "mod17473_benchmark_aws.py": "35271770930bf5e207d9dd96414c72666adc1d97ce1bdb20c6b3876f441ea470",
    "mod17473_cluster_memory_sampler.py": "1cca6e4ba0aedfe01ed4f93c01e30c223ae73c95f8e3ea31d27403369ac08e3e",
    "mod17473_memory_sampler_v3.py": "361c3676c066ec07bf1251d97d1ec3e8d61ddff72e18935ae1147413ff53ed73",
    "mod17473_resp_client_aws.py": "924f45926b3e44c683c8a219aadf071608452fa1049662ec182b7026570d894c",
    "mod17473_resp_client_v3.py": "0a8f8d58e759be3f9eb053614eb5fdf1048c0f82c6df7815da5e134e2a5279de",
    "mod17473_hiredis_load.c": "d6ee1da82663a0a8b0f3a9f9227aa7a918bbe6630784999d2d9b6f034ba7e068",
}

MEMORY_SCOPE_NOTE = (
    "unprefixed redis_*, module_*, proc_*, and smaps_* metrics are sums across both "
    "Redis primaries; node0_* and node1_* retain per-process values; cgroup metrics omitted"
)
FULL_LOAD_LATENCY_BOUNDARY = "after redisGetReply parsed the complete reply"
CPU_ALLOCATION_NOTE = (
    "the memory sampler shares the final client physical core; both Redis primary CPU "
    "sets are disjoint from the load-generator/sampler set"
)
REPLACEMENT_REPETITION = 8
REPLACEMENT_CASE = "timeout-wide10000-r3-c32-w0-ct4"
REPLACEMENT_VARIANT_ORDER = ("pr", "baseline")
REPLACEMENT_MANIFEST_POSITIONS = (296, 297)
REPLACEMENT_PAIR_STEM = f"r{REPLACEMENT_REPETITION:02d}-{REPLACEMENT_CASE}"
COMPOSITION_SCRIPT_SHA256 = "7a2c32ce7afe58b3e05b1d29a89cd90fd5d36dae1222c7e97561ac530078ba2d"
RERUN_SCRIPT_SHA256 = "137784e02ef583732f66c58fca422879e3ce55e775dfaa4945ea47a61f81db1f"
SOURCE_BUNDLE_SHA256 = "50d5c397a469a3e2735964d3626770eb442ce4825c9e0c275f2c176f105bdb13"
SOURCE_CAMPAIGN_BASENAME = "aws-cluster-n10-complete-v3"
SOURCE_CAMPAIGN_FILE_COUNT = 6118
SOURCE_CAMPAIGN_TREE_SHA256 = "66b01858b81d4b324bb316dca4bc7fc59f7662941bb6b13bad71e5ffddd986df"
REPLACEMENT_RERUN_BASENAME = "aws-cluster-n10-complete-v3-r08-replacement"
REPLACEMENT_RERUN_FILE_COUNT = 85
REPLACEMENT_RERUN_TREE_SHA256 = "97cc0ebb55de6272b192a852cd2dd0274e192f95dade03db7a10cbfe398336a0"
CORRECTED_CAMPAIGN_FILE_COUNT = 6154
CORRECTED_CAMPAIGN_TREE_SHA256 = "ae884dc035a3f0b00a51e570a92dd8562f8d6a916243727e15b496830fa7dfed"
OBSERVATION_EVIDENCE_FILES = (
    "cluster-identity.json",
    "cluster-server-commands.json",
    "measured-client.json",
    "measured-client.stderr.log",
    "measured-command.json",
    "memory-sampler.stderr.log",
    "memory.jsonl",
    "node0/nodes.conf",
    "node0/server.log",
    "node1/nodes.conf",
    "node1/server.log",
    "summary.json",
    "warmup-client.json",
    "warmup-client.stderr.log",
    "warmup-command.json",
)
COMPOSITION_REASON = (
    "One read-only completion poll overlapped the original target pair. Both halves of the "
    "pair are conservatively excluded and replaced by a clean BA rerun."
)
RERUN_REASON = (
    "One read-only completion poll overlapped the original target pair. Both halves of the "
    "pair are conservatively excluded and replaced by this clean BA rerun."
)

FINAL_LAUNCH_CONTRACT = {
    "repetitions": 10,
    "seed": 17473,
    "node_ports": [6381, 6382],
    "server_cpus": "0-1,8-9;2-3,10-11",
    "client_cpus": "4-7,12-15",
    "monitor_cpus": "7,15",
    "full_timeout_ms": 600000,
    "warmup_requests": 8,
    "warmup_clients": 4,
    "settle_seconds": 2.0,
    "cooldown_offsets": [0.5, 2.0, 5.0, 10.0],
    "sample_interval": 0.05,
    "redis_every": 2,
    "smaps_every": 4,
    "semantic_every": 0,
    "min_timeout_fraction": 0.0,
    "calibration_candidates": "1,2,3,4,5,7,10,15,22,33,47,68,100",
    "calibration_requests": 64,
    "calibration_target_fraction": 0.5,
    "fixed_conn_per_shard": 5,
    "fixed_min_operation_workers": 4,
    "fixed_search_io_threads": 1,
    "fixed_cursor_reply_threshold": 1,
}

ENVIRONMENT_DYNAMIC_FIELDS = {
    "campaign_id",
    "created_epoch",
    "uname",
    "lscpu",
    "numa",
    "python_version",
    "perf_counter_clock",
}


class CampaignError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CampaignError(message)


def finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def positive(value: Any, label: str) -> float:
    require(finite(value) and float(value) > 0, f"{label} must be positive, got {value!r}")
    return float(value)


def positive_or_zero(value: Any) -> float:
    require(finite(value) and float(value) >= 0, f"expected a non-negative finite value, got {value!r}")
    return float(value)


def close(left: Any, right: Any) -> bool:
    return finite(left) and finite(right) and math.isclose(
        float(left), float(right), rel_tol=1e-9, abs_tol=1e-6
    )


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignError(f"cannot read valid JSON {path}: {exc}") from exc


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise CampaignError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json_bytes(value)).hexdigest()


def file_identity(path: Path) -> dict[str, int | str]:
    return {"size": path.stat().st_size, "sha256": sha256(path)}


def evidence_inventory(root: Path) -> dict[str, dict[str, int | str]]:
    require(root.is_dir(), f"evidence directory is missing: {root}")
    result: dict[str, dict[str, int | str]] = {}
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        require(not path.is_symlink(), f"symlink is not accepted in evidence: {path}")
        if path.is_dir():
            continue
        require(path.is_file(), f"non-regular evidence entry: {path}")
        relative = path.relative_to(root).as_posix()
        result[relative] = file_identity(path)
    return result


def inventory_sha256(value: dict[str, dict[str, int | str]]) -> str:
    return canonical_sha256(value)


def require_sha256(value: Any, label: str) -> str:
    require(isinstance(value, str) and SHA256_RE.fullmatch(value) is not None, f"{label} is not a SHA256")
    return value


def safe_relative(root: Path, relative: str, label: str) -> Path:
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise CampaignError(f"{label} escapes campaign root: {relative!r}") from exc
    return path


def manifest_lines(path: Path, label: str) -> tuple[list[bytes], list[dict[str, Any]]]:
    try:
        raw_lines = path.read_bytes().splitlines(keepends=True)
        require(bool(raw_lines) and raw_lines[-1].endswith(b"\n"), f"{label} has no final newline")
        require(all(line.strip() for line in raw_lines), f"{label} contains a blank line")
        entries = [json.loads(line) for line in raw_lines]
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignError(f"cannot read {label} {path}: {exc}") from exc
    require(all(isinstance(entry, dict) for entry in entries), f"{label} has a non-object entry")
    return raw_lines, entries


def expected_excluded_inventory() -> set[str]:
    files = {
        "original-manifest.jsonl",
        "original-pair-validation.json",
        "original-validation-summary.json",
        "replacement-rerun-provenance.json",
    }
    for variant in REPLACEMENT_VARIANT_ORDER:
        observation = f"{REPLACEMENT_PAIR_STEM}-{variant}"
        files.update(
            f"observations/{observation}/{relative}"
            for relative in OBSERVATION_EVIDENCE_FILES
        )
    return files


def validate_composition_deltas(
    root: Path,
    excluded_root: Path,
    rerun_root: Path,
) -> None:
    original_lines, original_manifest = manifest_lines(
        excluded_root / "original-manifest.jsonl", "original manifest"
    )
    active_lines, active_manifest = manifest_lines(root / "manifest.jsonl", "active manifest")
    _rerun_lines, rerun_manifest = manifest_lines(rerun_root / "manifest.jsonl", "rerun manifest")
    require(len(original_lines) == len(active_lines) == 360, "composition manifest length mismatch")
    changed_manifest = [
        index
        for index, (original, active) in enumerate(zip(original_lines, active_lines))
        if original != active
    ]
    require(
        changed_manifest == list(REPLACEMENT_MANIFEST_POSITIONS),
        f"composition changed unexpected manifest positions: {changed_manifest}",
    )
    expected_observations = [
        f"{REPLACEMENT_PAIR_STEM}-{variant}" for variant in REPLACEMENT_VARIANT_ORDER
    ]
    require(
        [active_manifest[index].get("observation") for index in REPLACEMENT_MANIFEST_POSITIONS]
        == expected_observations,
        "active replacement manifest order mismatch",
    )
    require(
        [original_manifest[index].get("observation") for index in REPLACEMENT_MANIFEST_POSITIONS]
        == expected_observations,
        "original replacement manifest order mismatch",
    )
    require(
        rerun_manifest
        == [active_manifest[index] for index in REPLACEMENT_MANIFEST_POSITIONS],
        "rerun manifest does not exactly match active replacement entries",
    )
    for index, observation in zip(REPLACEMENT_MANIFEST_POSITIONS, expected_observations):
        require(
            original_manifest[index].get("summary_sha256")
            == sha256(excluded_root / "observations" / observation / "summary.json"),
            f"original manifest summary hash mismatch for {observation}",
        )
        require(
            active_manifest[index].get("summary_sha256")
            == sha256(root / "observations" / observation / "summary.json"),
            f"active manifest summary hash mismatch for {observation}",
        )

    original_validation = load_json(excluded_root / "original-validation-summary.json")
    active_validation = load_json(root / "validation-summary.json")
    rerun_validation = load_json(rerun_root / "validation-summary.json")
    require(
        isinstance(original_validation, dict)
        and isinstance(active_validation, dict)
        and set(original_validation) == set(active_validation),
        "composition validation-summary schema mismatch",
    )
    require(
        {key: value for key, value in original_validation.items() if key != "pairs"}
        == {key: value for key, value in active_validation.items() if key != "pairs"},
        "composition changed validation-summary metadata",
    )
    original_pairs = original_validation.get("pairs")
    active_pairs = active_validation.get("pairs")
    require(
        isinstance(original_pairs, list)
        and isinstance(active_pairs, list)
        and len(original_pairs) == len(active_pairs) == 180,
        "composition validation-summary pair count mismatch",
    )
    changed_pairs = [
        index
        for index, (original, active) in enumerate(zip(original_pairs, active_pairs))
        if original != active
    ]
    require(changed_pairs == [148], f"composition changed unexpected validation pairs: {changed_pairs}")
    original_pair = load_json(excluded_root / "original-pair-validation.json")
    active_pair = load_json(root / "pair-validations" / f"{REPLACEMENT_PAIR_STEM}.json")
    rerun_pair = load_json(rerun_root / "pair-validations" / f"{REPLACEMENT_PAIR_STEM}.json")
    require(
        original_pairs[148] == original_pair,
        "retained original pair does not match original validation summary",
    )
    require(
        active_pairs[148] == active_pair == rerun_pair,
        "active replacement pair does not exactly match rerun evidence",
    )
    require(
        isinstance(rerun_validation, dict)
        and rerun_validation.get("pair_count") == 1
        and rerun_validation.get("all_pairs_valid") is True
        and rerun_validation.get("pairs") == [rerun_pair],
        "rerun validation summary is not exactly one valid replacement pair",
    )

    for variant in REPLACEMENT_VARIANT_ORDER:
        observation = f"{REPLACEMENT_PAIR_STEM}-{variant}"
        active_inventory = evidence_inventory(root / "observations" / observation)
        rerun_inventory = evidence_inventory(rerun_root / "observations" / observation)
        require(
            set(active_inventory) == set(OBSERVATION_EVIDENCE_FILES),
            f"active replacement artifact set mismatch for {observation}",
        )
        require(
            active_inventory == rerun_inventory,
            f"active replacement artifacts do not exactly match rerun evidence for {observation}",
        )


def reconstruct_source_inventory(
    corrected_inventory: dict[str, dict[str, int | str]],
    excluded_inventory: dict[str, dict[str, int | str]],
) -> dict[str, dict[str, int | str]]:
    source = {
        relative: identity
        for relative, identity in corrected_inventory.items()
        if relative != "composition-provenance.json" and not relative.startswith("_excluded/")
    }
    for variant in REPLACEMENT_VARIANT_ORDER:
        prefix = f"observations/{REPLACEMENT_PAIR_STEM}-{variant}/"
        for relative in [name for name in source if name.startswith(prefix)]:
            del source[relative]
        for relative, identity in excluded_inventory.items():
            if relative.startswith(prefix):
                source[relative] = identity
    source["manifest.jsonl"] = excluded_inventory["original-manifest.jsonl"]
    source["validation-summary.json"] = excluded_inventory["original-validation-summary.json"]
    source[f"pair-validations/{REPLACEMENT_PAIR_STEM}.json"] = excluded_inventory[
        "original-pair-validation.json"
    ]
    return dict(sorted(source.items()))


def validate_replacement_composition(
    root: Path, environment: dict[str, Any], profile: str
) -> dict[str, Any] | None:
    """Validate the single conservative pair substitution in the final campaign."""
    composition_path = root / "composition-provenance.json"
    if profile != "final":
        require(not composition_path.exists(), "non-final input unexpectedly has composition provenance")
        return None
    require(composition_path.is_file(), "final campaign is missing replacement composition provenance")
    composition = load_json(composition_path)
    require(
        isinstance(composition, dict) and composition.get("schema") == 1,
        "composition provenance schema mismatch",
    )
    require(
        set(composition)
        == {
            "schema",
            "campaign_id",
            "reason",
            "target",
            "source_campaign",
            "replacement_rerun",
            "excluded_originals",
            "composition_script_sha256",
            "replacement_scope",
        },
        "composition provenance field set mismatch",
    )
    require(
        composition.get("campaign_id") == environment["campaign_id"],
        "composition campaign ID mismatch",
    )
    require(composition.get("reason") == COMPOSITION_REASON, "composition reason mismatch")
    require(
        composition.get("composition_script_sha256") == COMPOSITION_SCRIPT_SHA256,
        "composition script hash mismatch",
    )
    require(
        composition.get("replacement_scope")
        == "exactly two observation directories, their two manifest entries, the "
        "pair-validation file, and the matching validation-summary pair",
        "composition replacement scope mismatch",
    )
    expected_target = {
        "case": REPLACEMENT_CASE,
        "manifest_positions_zero_based": list(REPLACEMENT_MANIFEST_POSITIONS),
        "pair_sequence_zero_based": 148,
        "repetition": REPLACEMENT_REPETITION,
        "variant_order": list(REPLACEMENT_VARIANT_ORDER),
    }
    require(composition.get("target") == expected_target, "composition target mismatch")
    source_meta = composition.get("source_campaign")
    require(
        source_meta
        == {
            "basename": SOURCE_CAMPAIGN_BASENAME,
            "file_count": SOURCE_CAMPAIGN_FILE_COUNT,
            "tree_sha256": SOURCE_CAMPAIGN_TREE_SHA256,
        },
        "composition source campaign identity mismatch",
    )
    rerun_meta = composition.get("replacement_rerun")
    require(
        rerun_meta
        == {
            "basename": REPLACEMENT_RERUN_BASENAME,
            "file_count": REPLACEMENT_RERUN_FILE_COUNT,
            "tree_sha256": REPLACEMENT_RERUN_TREE_SHA256,
        },
        "composition replacement rerun identity mismatch",
    )

    corrected_inventory = evidence_inventory(root)
    require(
        len(corrected_inventory) == CORRECTED_CAMPAIGN_FILE_COUNT
        and inventory_sha256(corrected_inventory) == CORRECTED_CAMPAIGN_TREE_SHA256,
        "corrected campaign tree does not match the audited archive",
    )
    rerun_root = safe_relative(root.parent, REPLACEMENT_RERUN_BASENAME, "replacement rerun")
    rerun_inventory = evidence_inventory(rerun_root)
    require(
        len(rerun_inventory) == REPLACEMENT_RERUN_FILE_COUNT
        and inventory_sha256(rerun_inventory) == REPLACEMENT_RERUN_TREE_SHA256,
        "replacement rerun tree does not match composition provenance",
    )

    excluded_meta = composition.get("excluded_originals")
    expected_relative = f"_excluded/{REPLACEMENT_PAIR_STEM}"
    require(
        isinstance(excluded_meta, dict)
        and set(excluded_meta) == {"relative_directory", "inventory_sha256"}
        and excluded_meta.get("relative_directory") == expected_relative,
        "excluded-original directory mismatch",
    )
    excluded_root = safe_relative(root, expected_relative, "excluded originals")
    require(excluded_root.is_dir(), "excluded-original directory is missing")
    inventory_path = excluded_root / "inventory.json"
    require(inventory_path.is_file(), "excluded-original inventory is missing")
    require(
        sha256(inventory_path) == require_sha256(excluded_meta.get("inventory_sha256"), "excluded inventory"),
        "excluded-original inventory hash mismatch",
    )
    inventory = load_json(inventory_path)
    require(isinstance(inventory, dict), "excluded inventory is malformed")
    files = inventory.get("files")
    require(
        set(inventory) == {"schema", "files", "tree_sha256"}
        and inventory.get("schema") == 1
        and isinstance(files, dict),
        "excluded inventory is malformed",
    )
    require(set(files) == expected_excluded_inventory(), "excluded inventory file set mismatch")
    require(
        inventory_sha256(files) == inventory.get("tree_sha256"),
        "excluded inventory tree hash mismatch",
    )
    for relative, identity in files.items():
        require(
            isinstance(relative, str) and isinstance(identity, dict),
            "excluded inventory entry is malformed",
        )
        path = safe_relative(excluded_root, relative, "excluded inventory entry")
        require(path.is_file(), f"excluded inventory file is missing: {relative}")
        require(path.stat().st_size == identity.get("size"), f"excluded inventory size mismatch: {relative}")
        require(sha256(path) == identity.get("sha256"), f"excluded inventory hash mismatch: {relative}")
    actual_excluded = evidence_inventory(excluded_root)
    require(
        set(actual_excluded) == set(files) | {"inventory.json"},
        "excluded-original directory contains untracked evidence",
    )
    source_inventory = reconstruct_source_inventory(corrected_inventory, files)
    require(
        len(source_inventory) == SOURCE_CAMPAIGN_FILE_COUNT
        and inventory_sha256(source_inventory) == SOURCE_CAMPAIGN_TREE_SHA256,
        "retained originals do not reconstruct the audited source campaign tree",
    )

    rerun_path = excluded_root / "replacement-rerun-provenance.json"
    rerun = load_json(rerun_path)
    require(
        isinstance(rerun, dict) and rerun.get("schema") == 1,
        "replacement rerun provenance is malformed",
    )
    require(
        rerun.get("campaign_id") == environment["campaign_id"],
        "replacement rerun campaign ID mismatch",
    )
    require(rerun.get("reason") == RERUN_REASON, "replacement rerun reason mismatch")
    require(
        rerun.get("rerun_script_sha256") == RERUN_SCRIPT_SHA256,
        "replacement rerun script hash mismatch",
    )
    require(
        rerun.get("source_campaign_basename") == SOURCE_CAMPAIGN_BASENAME,
        "replacement source basename mismatch",
    )
    require(
        rerun.get("source_environment_sha256") == sha256(root / "environment.json")
        and rerun.get("source_calibrated_cases_sha256") == sha256(root / "calibrated-cases.json"),
        "replacement rerun source metadata hashes mismatch",
    )
    expected_rerun_target = {
        "case": REPLACEMENT_CASE,
        "pair_sequence_zero_based": 148,
        "repetition": REPLACEMENT_REPETITION,
        "variant_order": list(REPLACEMENT_VARIANT_ORDER),
    }
    require(rerun.get("target") == expected_rerun_target, "replacement rerun target mismatch")
    for variant in REPLACEMENT_VARIANT_ORDER:
        observation = f"{REPLACEMENT_PAIR_STEM}-{variant}"
        active_summary = root / "observations" / observation / "summary.json"
        excluded_summary = excluded_root / "observations" / observation / "summary.json"
        replacement_entry = rerun.get("replacement", {}).get("observations", {}).get(variant, {})
        original_entry = rerun.get("original_excluded", {}).get("observations", {}).get(variant, {})
        require(
            replacement_entry.get("observation") == observation,
            f"replacement {variant} identity mismatch",
        )
        require(
            original_entry.get("observation") == observation,
            f"excluded {variant} identity mismatch",
        )
        require(
            sha256(active_summary) == replacement_entry.get("summary_sha256"),
            f"replacement {variant} summary hash mismatch",
        )
        require(
            sha256(excluded_summary) == original_entry.get("summary_sha256"),
            f"excluded {variant} summary hash mismatch",
        )
        require(
            replacement_entry.get("summary_sha256")
            != original_entry.get("summary_sha256"),
            f"replacement {variant} did not change",
        )
    active_pair = root / "pair-validations" / f"{REPLACEMENT_PAIR_STEM}.json"
    excluded_pair = excluded_root / "original-pair-validation.json"
    require(
        sha256(active_pair)
        == rerun.get("replacement", {}).get("pair_validation_sha256"),
        "replacement pair hash mismatch",
    )
    require(
        sha256(excluded_pair)
        == rerun.get("original_excluded", {}).get("pair_validation_sha256"),
        "excluded pair hash mismatch",
    )
    validate_composition_deltas(root, excluded_root, rerun_root)
    return {
        "excluded_original_pairs": 1,
        "clean_replacement_pairs": 1,
        "replacement_included_in_published_n": True,
        "case": REPLACEMENT_CASE,
        "repetition": REPLACEMENT_REPETITION,
        "order": "BA",
        "composition_provenance_sha256": sha256(composition_path),
        "composition_script_sha256": COMPOSITION_SCRIPT_SHA256,
        "rerun_script_sha256": RERUN_SCRIPT_SHA256,
        "corrected_campaign_file_count": len(corrected_inventory),
        "corrected_campaign_tree_sha256": inventory_sha256(corrected_inventory),
        "source_campaign_file_count": len(source_inventory),
        "source_campaign_tree_sha256": inventory_sha256(source_inventory),
        "replacement_rerun_file_count": len(rerun_inventory),
        "replacement_rerun_tree_sha256": inventory_sha256(rerun_inventory),
    }


def nested(obj: dict[str, Any], dotted: str) -> Any:
    current: Any = obj
    for part in dotted.split("."):
        require(isinstance(current, dict) and part in current, f"missing field {dotted}")
        current = current[part]
    return current


def quantile(values: list[float], probability: float) -> float:
    require(bool(values), "cannot take a quantile of an empty list")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] * (high - position) + ordered[high] * (position - low)


def geometric_mean(values: Iterable[float]) -> float:
    items = [float(value) for value in values]
    require(bool(items) and all(value > 0 for value in items), "geometric mean needs positive values")
    return math.exp(math.fsum(math.log(value) for value in items) / len(items))


def case_parts(name: str) -> dict[str, Any]:
    match = CASE_RE.fullmatch(name)
    require(match is not None, f"unexpected case name: {name!r}")
    mode, protocol, workers, threads = match.groups()
    return {
        "mode": mode,
        "protocol": int(protocol),
        "workers": int(workers),
        "search_threads": int(threads),
    }


def expected_query_command(spec: dict[str, Any], timeout_ms: int) -> list[str | int]:
    return [
        "FT.AGGREGATE",
        "idx",
        "*",
        "LOAD",
        len(spec["raw"]["fields"]),
        *spec["raw"]["fields"],
        "LIMIT",
        0,
        spec["raw"]["expected_rows"],
        "TIMEOUT",
        timeout_ms,
    ]


def final_case_names() -> set[str]:
    full = {
        f"full-wide10000-r{protocol}-c32-w{workers}-ct{threads}"
        for protocol in (2, 3)
        for threads in (1, 4)
        for workers in (0, 1, 4)
    }
    timeout = {
        f"timeout-wide10000-r{protocol}-c32-w{workers}-ct4"
        for protocol in (2, 3)
        for workers in (0, 1, 4)
    }
    return full | timeout


def validate_environment(environment: dict[str, Any], profile: str) -> dict[str, dict[str, Any]]:
    campaign_id = require_sha256(environment.get("campaign_id"), "campaign ID")
    immutable = {
        key: value for key, value in environment.items() if key not in ENVIRONMENT_DYNAMIC_FIELDS
    }
    require(canonical_sha256(immutable) == campaign_id, "environment does not reproduce its campaign ID")

    for key, expected in EXPECTED_PROVENANCE.items():
        actual = (
            environment.get("cluster_seed", {}).get("logical_dataset_sha256")
            if key == "logical_dataset_sha256"
            else environment.get(key)
        )
        require(actual == expected, f"provenance mismatch for {key}: {actual!r}")
    require(environment["baseline_sha256"] != environment["pr_sha256"], "baseline and PR module hashes are identical")
    for key in (
        "baseline_sha256",
        "pr_sha256",
        "redis_server_sha256",
        "cluster_seed_descriptor_sha256",
        "hiredis_load_client_sha256",
        "hiredis_load_client_source_sha256",
        "hiredis_static_archive_sha256",
    ):
        require_sha256(environment.get(key), key)

    scripts = environment.get("scripts")
    require(isinstance(scripts, dict) and scripts, "harness script provenance is missing")
    script_names: dict[str, str] = {}
    for path, digest in scripts.items():
        require(isinstance(path, str), "harness script path is malformed")
        name = Path(path).name
        require(name and name not in script_names, f"duplicate harness script basename: {name!r}")
        script_names[name] = require_sha256(digest, f"script {name}")
    require(
        set(script_names) == set(EXPECTED_HARNESS_SCRIPTS),
        f"unexpected harness script set: {sorted(script_names)}",
    )
    if profile == "final":
        require(
            script_names == EXPECTED_HARNESS_SCRIPTS,
            "final harness script SHA256 mapping does not match the audited launcher inputs",
        )

    require(environment.get("memory_scope_note") == MEMORY_SCOPE_NOTE, "memory-scope note mismatch")
    require(
        environment.get("full_load_latency_boundary") == FULL_LOAD_LATENCY_BOUNDARY,
        "full-load latency boundary mismatch",
    )

    descriptor = environment.get("cluster_seed")
    require(isinstance(descriptor, dict), "cluster seed descriptor is missing")
    require(canonical_sha256(descriptor) == environment["cluster_seed_descriptor_sha256"], "embedded cluster seed descriptor hash mismatch")
    require(descriptor.get("schema") == 1, "cluster seed schema mismatch")
    require(descriptor.get("topology") == environment.get("topology"), "cluster seed topology mismatch")
    require(descriptor.get("documents") == environment.get("expected_docs") == 10000, "cluster seed document count mismatch")
    nodes = descriptor.get("nodes")
    require(isinstance(nodes, list) and len(nodes) == 2, "cluster seed must contain two nodes")
    expected_ranges = ((0, 8191), (8192, 16383))
    for index, (node, slot_range) in enumerate(zip(nodes, expected_ranges)):
        require(isinstance(node, dict) and node.get("index") == index, f"cluster seed node {index} is malformed")
        require(node.get("documents") == 5000, f"cluster seed node {index} document count mismatch")
        require((node.get("slot_start"), node.get("slot_end")) == slot_range, f"cluster seed node {index} slot range mismatch")
        require_sha256(node.get("rdb_sha256"), f"cluster seed node {index} RDB")
        occupied = node.get("occupied_slots")
        require(isinstance(occupied, list) and occupied, f"cluster seed node {index} occupied slots missing")
        require(len(occupied) == len(set(occupied)), f"cluster seed node {index} occupied slots repeat")
        require(all(isinstance(slot, int) and slot_range[0] <= slot <= slot_range[1] for slot in occupied), f"cluster seed node {index} occupied slot out of range")

    raw_cases = environment.get("cases")
    require(isinstance(raw_cases, list), "environment cases are malformed")
    cases_by_name: dict[str, dict[str, Any]] = {}
    for case in raw_cases:
        require(isinstance(case, dict) and isinstance(case.get("name"), str), "environment case is malformed")
        require(case["name"] not in cases_by_name, f"duplicate environment case {case['name']}")
        require(case.get("timeout_ms") is None, f"environment case {case['name']} must precede calibration")
        cases_by_name[case["name"]] = case

    if profile == "final":
        for key, expected in FINAL_LAUNCH_CONTRACT.items():
            actual = environment.get(key)
            if isinstance(expected, float):
                require(close(actual, expected), f"final launch mismatch for {key}: {actual!r}")
            else:
                require(actual == expected, f"final launch mismatch for {key}: {actual!r}")
        require(environment.get("cpu_allocation_note") == CPU_ALLOCATION_NOTE, "final CPU-allocation note mismatch")
        lscpu = environment.get("lscpu")
        require(isinstance(lscpu, str), "final hardware inventory is missing")
        for pattern in (
            r"(?m)^CPU\(s\):\s+16$",
            r"(?m)^Model name:\s+AMD EPYC 7R32$",
            r"(?m)^Thread\(s\) per core:\s+2$",
            r"(?m)^Core\(s\) per socket:\s+8$",
            r"(?m)^Socket\(s\):\s+1$",
            r"(?m)^NUMA node\(s\):\s+1$",
        ):
            require(re.search(pattern, lscpu) is not None, f"final hardware mismatch for {pattern}")
        require(str(environment.get("python_version", "")).startswith("3.14.4 "), "final Python version mismatch")
        require(environment.get("perf_counter_clock") == "clock_gettime(CLOCK_MONOTONIC)", "final performance clock mismatch")
    return cases_by_name


def validate_calibrations(
    root: Path,
    environment: dict[str, Any],
    calibrated: dict[str, Any],
    specs: dict[str, dict[str, Any]],
    environment_cases: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    records = calibrated.get("calibration")
    require(isinstance(records, dict), "calibration index is missing")
    timeout_names = {name for name, spec in specs.items() if spec["mode"] == "timeout"}
    require(set(records) == timeout_names, "calibration index does not match timeout cases")
    candidates = sorted({int(value) for value in environment["calibration_candidates"].split(",") if value})
    probe_requests = max(32, int(environment["calibration_requests"]))
    warmup_requests = max(1, min(32, int(environment["warmup_requests"])))
    scripts = environment["scripts"]
    calibration_scripts = {
        path: digest
        for path, digest in scripts.items()
        if Path(path).name in {"mod17473_benchmark_aws.py", "mod17473_resp_client_aws.py"}
    }
    totals = {
        "baseline_topology_starts": len(timeout_names),
        "redis_process_executions": len(timeout_names) * 2,
        "warmup_client_invocations": 0,
        "probe_client_invocations": 0,
        "external_ft_aggregate_calls": 0,
        "reports": {},
    }
    for name in sorted(timeout_names):
        spec = specs[name]
        raw_case = environment_cases[name]
        calibrated_case = spec["raw"]
        require(
            {key: value for key, value in calibrated_case.items() if key != "timeout_ms"}
            == {key: value for key, value in raw_case.items() if key != "timeout_ms"},
            f"{name}: calibration changed a non-timeout case field",
        )
        directory = root / "calibration" / name
        report_path = directory / "calibration.json"
        report = load_json(report_path)
        require(isinstance(report, dict), f"{name}: calibration report is malformed")
        expected_input = {
            "case": raw_case,
            "baseline_module": environment["baseline_module"],
            "baseline_sha256": environment["baseline_sha256"],
            "dataset_sha256": environment["cluster_seed_descriptor_sha256"],
            "candidates_ms": candidates,
            "target_fraction": environment["calibration_target_fraction"],
            "requests": environment["calibration_requests"],
            "scripts": calibration_scripts,
        }
        require(report.get("input") == expected_input, f"{name}: calibration inputs do not match the campaign")
        fingerprint = canonical_sha256(expected_input)
        require(report.get("input_fingerprint") == fingerprint, f"{name}: calibration input fingerprint mismatch")
        record = records[name]
        require(isinstance(record, dict), f"{name}: calibration index entry is malformed")
        require(record.get("input_fingerprint") == fingerprint, f"{name}: indexed calibration fingerprint mismatch")
        require(record.get("timeout_ms") == spec["timeout_ms"] == report.get("selected_timeout_ms"), f"{name}: selected timeout mismatch")

        results = report.get("candidate_results")
        require(isinstance(results, list) and len(results) == len(candidates), f"{name}: calibration probe count mismatch")
        require([result.get("candidate_timeout_ms") for result in results] == candidates, f"{name}: calibration candidate order mismatch")
        for result, candidate_ms in zip(results, candidates):
            require(result.get("requests") == probe_requests, f"{name}: {candidate_ms} ms probe request count mismatch")
            rows = result.get("rows_returned")
            require(isinstance(rows, int) and 0 <= rows <= probe_requests * 10000, f"{name}: {candidate_ms} ms probe rows invalid")
            require(close(result.get("mean_rows_per_reply"), rows / probe_requests), f"{name}: {candidate_ms} ms probe mean rows mismatch")
            probe = load_json(directory / f"probe-{candidate_ms}ms-client.json")
            require(isinstance(probe, dict), f"{name}: {candidate_ms} ms probe artifact malformed")
            require(all(result.get(key) == value for key, value in probe.items()), f"{name}: {candidate_ms} ms probe artifact disagrees with report")
            command = load_json(directory / f"probe-{candidate_ms}ms-command.json")
            require(command == expected_query_command(spec, candidate_ms), f"{name}: {candidate_ms} ms probe command mismatch")
        viable = [
            result for result in results
            if positive_or_zero(result.get("timeout_evidence_fraction")) > 0
            and int(result.get("partial_response_count", 0)) > 0
            and int(result.get("rows_returned", 0)) > 0
        ]
        require(viable, f"{name}: no viable timeout calibration probe")
        target = float(environment["calibration_target_fraction"])
        chosen = min(
            viable,
            key=lambda result: (
                abs(float(result["timeout_evidence_fraction"]) - target)
                + 0.25 * abs(float(result["mean_rows_per_reply"]) / 10000.0 - 0.5),
                int(result["candidate_timeout_ms"]),
            ),
        )
        require(report.get("selected_result") == chosen, f"{name}: selected calibration result is not reproducible")
        warmup = load_json(directory / "warmup-full-client.json")
        require(isinstance(warmup, dict) and warmup.get("requests") == warmup_requests, f"{name}: calibration warmup count mismatch")
        warmup_command = load_json(directory / "warmup-full-command.json")
        require(warmup_command == expected_query_command(spec, int(environment["full_timeout_ms"])), f"{name}: calibration warmup command mismatch")
        totals["warmup_client_invocations"] += 1
        totals["probe_client_invocations"] += len(results)
        totals["external_ft_aggregate_calls"] += warmup_requests + probe_requests * len(results)
        totals["reports"][name] = {
            "sha256": sha256(report_path),
            "selected_timeout_ms": spec["timeout_ms"],
            "probe_count": len(results),
            "probe_requests_each": probe_requests,
            "warmup_requests": warmup_requests,
        }
    return totals


def order_stratified_bootstrap(
    rows: list[dict[str, Any]],
    statistic: Callable[[list[dict[str, Any]]], float],
    iterations: int,
    seed: int,
    identity: str,
) -> tuple[float, float]:
    require(rows and iterations > 0, "bootstrap requires observations and iterations")
    strata = {
        order: [row for row in rows if row.get("order") == order]
        for order in ("AB", "BA")
    }
    require(sum(len(items) for items in strata.values()) == len(rows), f"{identity}: invalid order label")
    material = f"{seed}:{identity}".encode("utf-8")
    rng = random.Random(int.from_bytes(hashlib.sha256(material).digest()[:8], "big"))
    estimates: list[float] = []
    for _ in range(iterations):
        sample: list[dict[str, Any]] = []
        for order in ("AB", "BA"):
            items = strata[order]
            sample.extend(items[rng.randrange(len(items))] for _ in range(len(items)))
        estimates.append(statistic(sample))
    return quantile(estimates, 0.025), quantile(estimates, 0.975)


def ratio_aggregate(
    rows: list[dict[str, Any]], iterations: int, seed: int, identity: str
) -> dict[str, Any]:
    ratios = [positive(row["pr"], f"{identity} PR") / positive(row["baseline"], f"{identity} baseline") for row in rows]
    point_ratio = geometric_mean(ratios)
    low, high = order_stratified_bootstrap(
        rows,
        lambda sample: geometric_mean(
            float(row["pr"]) / float(row["baseline"]) for row in sample
        ),
        iterations,
        seed,
        identity,
    )
    by_order: dict[str, float | None] = {}
    for order in ("AB", "BA"):
        selected = [ratio for ratio, row in zip(ratios, rows) if row["order"] == order]
        by_order[order] = (geometric_mean(selected) - 1.0) * 100.0 if selected else None
    return {
        "method": "paired-geometric-mean-pr-over-baseline",
        "baseline_geomean": geometric_mean(float(row["baseline"]) for row in rows),
        "pr_geomean": geometric_mean(float(row["pr"]) for row in rows),
        "ratio": point_ratio,
        "effect_percent": (point_ratio - 1.0) * 100.0,
        "ci95_percent": [(low - 1.0) * 100.0, (high - 1.0) * 100.0],
        "order_effect_percent": by_order,
        "raw": [
            row
            | {
                "ratio": ratio,
                "effect_percent": (ratio - 1.0) * 100.0,
            }
            for row, ratio in zip(rows, ratios)
        ],
    }


def delta_aggregate(
    rows: list[dict[str, Any]],
    iterations: int,
    seed: int,
    identity: str,
    scale: float,
) -> dict[str, Any]:
    raw = [
        row
        | {
            "baseline_scaled": float(row["baseline"]) / scale,
            "pr_scaled": float(row["pr"]) / scale,
            "effect": (float(row["pr"]) - float(row["baseline"])) / scale,
        }
        for row in rows
    ]
    effects = [row["effect"] for row in raw]
    low, high = order_stratified_bootstrap(
        raw,
        lambda sample: float(statistics.median(row["effect"] for row in sample)),
        iterations,
        seed,
        identity,
    )
    return {
        "method": "paired-median-pr-minus-baseline",
        "baseline_median": float(statistics.median(row["baseline_scaled"] for row in raw)),
        "pr_median": float(statistics.median(row["pr_scaled"] for row in raw)),
        "effect": float(statistics.median(effects)),
        "ci95": [low, high],
        "raw": raw,
    }


def validate_case_spec(case: dict[str, Any], search_threads: int) -> dict[str, Any]:
    require(isinstance(case, dict), "case spec must be an object")
    name = case.get("name")
    require(isinstance(name, str), "case name must be a string")
    parts = case_parts(name)
    require(case.get("protocol") == parts["protocol"], f"{name}: protocol mismatch")
    require(case.get("workers") == parts["workers"], f"{name}: workers mismatch")
    require(case.get("timeout_mode") == parts["mode"], f"{name}: mode mismatch")
    require(search_threads == parts["search_threads"], f"{name}: SEARCH_THREADS mismatch")
    expected_requests = 64 if parts["mode"] == "full" else 128
    require(case.get("requests") == expected_requests, f"{name}: requests mismatch")
    require(case.get("clients") == 32, f"{name}: expected 32 clients")
    require(case.get("expected_rows") == 10000, f"{name}: expected 10000 rows")
    require(case.get("limit") is True, f"{name}: LIMIT must be enabled")
    require(case.get("fields") == ["@n", "@tag", "@title", "@body"], f"{name}: fields mismatch")
    timeout = case.get("timeout_ms")
    if parts["mode"] == "full":
        require(timeout == 600000, f"{name}: full timeout must be 600000 ms")
    else:
        require(isinstance(timeout, int) and timeout > 0, f"{name}: calibrated timeout is invalid")
    return parts | {"name": name, "timeout_ms": timeout, "requests": expected_requests}


def validate_identity(
    summary: dict[str, Any], environment: dict[str, Any], observation: str
) -> None:
    identity = summary.get("server_identity")
    require(isinstance(identity, dict), f"{observation}: missing server identity")
    require(identity.get("topology") == "single-host-two-primary-oss-cluster", f"{observation}: topology mismatch")
    require(identity.get("primary_count") == 2 and identity.get("replica_count") == 0, f"{observation}: expected two primaries and no replicas")
    require(identity.get("public_num_docs") == 10000, f"{observation}: public document count mismatch")
    require(identity.get("ingress_node") == 0, f"{observation}: ingress must be node 0")
    seed = environment["cluster_seed"]
    require(identity.get("logical_dataset_sha256") == seed["logical_dataset_sha256"], f"{observation}: logical dataset hash mismatch")
    nodes = identity.get("nodes")
    require(isinstance(nodes, list) and len(nodes) == 2, f"{observation}: expected two node identities")
    expected_sha = environment[f"{summary['variant']}_sha256"]
    for index, node in enumerate(sorted(nodes, key=lambda value: value.get("index", -1))):
        prefix = f"{observation}: node {index}"
        require(isinstance(node, dict), f"{prefix}: identity is malformed")
        require(node.get("index") == index, f"{prefix}: index mismatch")
        require(node.get("port") == environment["node_ports"][index], f"{prefix}: port mismatch")
        require(node.get("bus_port") == environment["node_ports"][index] + 10000, f"{prefix}: bus port mismatch")
        require(node.get("cpus") == environment["server_cpus"].split(";")[index], f"{prefix}: CPU affinity mismatch")
        require(node.get("redis_mode") == "cluster", f"{prefix}: Redis is not in cluster mode")
        require(node.get("module_sha256") == expected_sha, f"{prefix}: module hash mismatch")
        require(node.get("seed_rdb_sha256") == seed["nodes"][index]["rdb_sha256"], f"{prefix}: seed RDB hash mismatch")
        require(node.get("shard_dbsize") == 5000, f"{prefix}: shard document count mismatch")
        cluster = node.get("cluster_info", {})
        require(cluster.get("cluster_state") == "ok", f"{prefix}: cluster state is not ok")
        require(cluster.get("cluster_known_nodes") == 2, f"{prefix}: known-node count mismatch")
        require(cluster.get("cluster_size") == 2, f"{prefix}: cluster size mismatch")
        require(cluster.get("cluster_slots_assigned") == 16384, f"{prefix}: slots are incomplete")
        require(cluster.get("cluster_slots_ok") == 16384, f"{prefix}: slots are unhealthy")
        require(cluster.get("cluster_slots_fail") == 0 and cluster.get("cluster_slots_pfail") == 0, f"{prefix}: failed slots present")
        search_cluster = node.get("search_cluster_info", {})
        require(search_cluster.get("cluster_type") == "redis_oss", f"{prefix}: RediSearch cluster type mismatch")
        require(str(search_cluster.get("num_partitions")) == "2", f"{prefix}: RediSearch partition count mismatch")
        shards = str(search_cluster.get("shards", ""))
        shard_tuples = [tuple(int(part) for part in match) for match in SEARCH_SHARD_RE.findall(shards)]
        expected_shards = [
            (0, 8191, environment["node_ports"][0]),
            (8192, 16383, environment["node_ports"][1]),
        ]
        require(
            len(shard_tuples) == 2 and set(shard_tuples) == set(expected_shards),
            f"{prefix}: RediSearch shard map is not the exact expected range-to-port mapping",
        )


def validate_config(
    summary: dict[str, Any], spec: dict[str, Any], observation: str
) -> None:
    before = summary.get("effective_config_before")
    after = summary.get("effective_config_after")
    require(isinstance(before, dict) and before == after, f"{observation}: configuration changed during measurement")
    fixed = {
        "search-conn-per-shard": "5",
        "search-min-operation-workers": "4",
        "search-io-threads": "1",
        "search-cursor-reply-threshold": "1",
        "search-on-timeout": "return",
        "search-timeout": "600000",
        "search-_max-foreground-timeout-limit": "0",
        "redis:save": "",
        "redis:appendonly": "no",
        "redis:io-threads": "1",
        "redis:maxmemory": "0",
        "redis:hz": "10",
        "redis:dynamic-hz": "yes",
        "redis:activedefrag": "no",
        "redis:jemalloc-bg-thread": "yes",
    }
    expected_flat: dict[str, str] = {}
    for node in (0, 1):
        prefix = f"node{node}:"
        expected_flat[prefix + "search-workers"] = str(spec["workers"])
        expected_flat[prefix + "search-threads"] = str(spec["search_threads"])
        for key, expected in fixed.items():
            expected_flat[prefix + key] = expected
    require(before == expected_flat, f"{observation}: effective configuration is not the exact v2 configuration")
    identity_nodes = sorted(summary["server_identity"]["nodes"], key=lambda node: node["index"])
    for index, node in enumerate(identity_nodes):
        node_config = {
            key.removeprefix(f"node{index}:"): value
            for key, value in expected_flat.items()
            if key.startswith(f"node{index}:")
        }
        require(node.get("effective_config") == node_config, f"{observation}: node {index} identity/config mismatch")


def validate_artifacts(root: Path, summary: dict[str, Any], summary_path: Path) -> None:
    artifacts = summary.get("artifact_sha256")
    require(isinstance(artifacts, dict) and artifacts, f"{summary_path}: artifact hashes missing")
    required = {"measured-client.json", "measured-command.json", "memory.jsonl"}
    require(required == set(artifacts), f"{summary_path}: artifact hash set mismatch")
    for relative, expected in artifacts.items():
        require(isinstance(relative, str) and isinstance(expected, str), f"{summary_path}: malformed artifact hash")
        path = safe_relative(root, str(summary_path.parent.relative_to(root) / relative), "artifact")
        require(path.is_file(), f"missing observation artifact: {path}")
        require(sha256(path) == expected, f"artifact hash mismatch: {path}")


def parse_commandstat(raw: Any, label: str) -> dict[str, int]:
    require(isinstance(raw, str), f"{label}: commandstats entry is missing")
    fields: dict[str, int] = {}
    for name in ("calls", "failed_calls", "rejected_calls"):
        match = re.search(rf"(?:^|,){name}=(\d+)(?:,|$)", raw)
        require(match is not None, f"{label}: commandstats has no integer {name}")
        fields[name] = int(match.group(1))
    return fields


def commandstat(commandstats: dict[str, Any], key: str, label: str) -> dict[str, int]:
    matches = [value for actual, value in commandstats.items() if actual.lower() == key.lower()]
    require(len(matches) == 1, f"{label}: expected exactly one {key} commandstats entry")
    return parse_commandstat(matches[0], label)


def validate_commandstats(summary: dict[str, Any], spec: dict[str, Any], observation: str) -> dict[str, dict[str, int]]:
    stats = summary.get("commandstats")
    require(isinstance(stats, dict), f"{observation}: commandstats are missing")
    public = commandstat(stats, "cmdstat_ft.aggregate", f"{observation}: public FT.AGGREGATE")
    require(public == {"calls": spec["requests"], "failed_calls": 0, "rejected_calls": 0}, f"{observation}: public FT.AGGREGATE counters mismatch")
    internal: dict[str, dict[str, int]] = {}
    for node in (0, 1):
        key = f"node{node}:cmdstat__ft.aggregate"
        parsed = commandstat(stats, key, f"{observation}: node {node} _FT.AGGREGATE")
        require(parsed["calls"] == spec["requests"] and parsed["rejected_calls"] == 0, f"{observation}: node {node} internal aggregate counters mismatch")
        internal[key] = parsed
        for suffix in ("read", "del"):
            cursor_key = f"node{node}:cmdstat__ft.cursor|{suffix}"
            matches = [value for actual, value in stats.items() if actual.lower() == cursor_key]
            if matches:
                cursor = parse_commandstat(matches[0], f"{observation}: {cursor_key}")
                require(cursor["rejected_calls"] == 0, f"{observation}: rejected internal cursor calls")
                internal[cursor_key] = cursor
    return internal


def validate_memory_artifact(summary: dict[str, Any], path: Path, observation: str) -> None:
    samples: list[dict[str, Any]] = []
    try:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            sample = json.loads(line)
            require(isinstance(sample, dict), f"{observation}: memory sample {line_number} is malformed")
            samples.append(sample)
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignError(f"{observation}: cannot parse memory artifact: {exc}") from exc
    memory = summary["memory"]
    require(len(samples) == memory["sample_count"], f"{observation}: memory sample count disagrees with artifact")
    require(sum(bool(sample.get("redis_sample_error")) for sample in samples) == memory["sampler_errors"], f"{observation}: sampler error count disagrees with artifact")
    load_start = summary.get("load_started_monotonic_ns")
    load_end = summary.get("load_finished_monotonic_ns")
    require(isinstance(load_start, int) and isinstance(load_end, int) and load_start < load_end, f"{observation}: load window is malformed")
    load_samples = [
        sample for sample in samples
        if load_start <= int(sample.get("monotonic_ns", 0)) <= load_end
    ]
    require(len(load_samples) == memory["load_window_sample_count"], f"{observation}: load-window sample count disagrees with artifact")
    before = summary.get("memory_before")
    after = summary.get("memory_after_immediate")
    cooldown = memory.get("cooldown_snapshots")
    require(isinstance(before, dict) and isinstance(after, dict) and isinstance(cooldown, list), f"{observation}: memory snapshots are malformed")
    final = cooldown[-1] if cooldown else after
    for scope in MEMORY_SCOPES:
        for metric in MEMORY_METRICS:
            key = metric if scope == "cluster" else f"{scope}_{metric}"
            report = memory_metric(summary, scope, metric)
            require(close(report["before"], before.get(key)), f"{observation}: {key}.before disagrees with snapshot")
            require(close(report["after_cooldown"], final.get(key)), f"{observation}: {key}.after_cooldown disagrees with snapshot")
            candidates = [
                float(sample[key])
                for sample in load_samples
                if finite(sample.get(key))
            ]
            require(candidates, f"{observation}: {key} was not sampled during load")
            candidates.extend((float(before[key]), float(after[key])))
            peak = max(candidates)
            require(close(report["peak_during_load"], peak), f"{observation}: {key}.peak disagrees with artifact")
            require(close(report["peak_minus_before"], peak - float(before[key])), f"{observation}: {key}.peak excursion disagrees with artifact")
            require(close(report["cooldown_minus_before"], float(final[key]) - float(before[key])), f"{observation}: {key}.cooldown residual disagrees with artifact")


def validate_artifact_contents(
    root: Path,
    summary: dict[str, Any],
    summary_path: Path,
    spec: dict[str, Any],
    environment: dict[str, Any],
    observation: str,
) -> None:
    directory = summary_path.parent
    command = load_json(directory / "measured-command.json")
    require(command == summary["query_command"], f"{observation}: measured command artifact mismatch")
    measured = load_json(directory / "measured-client.json")
    require(isinstance(measured, dict), f"{observation}: measured client artifact is malformed")
    performance = summary.get("performance")
    require(isinstance(performance, dict), f"{observation}: performance summary is malformed")
    replaced_validation_prefixes = ("semantic_", "row_semantic_", "unordered_row_")
    require(
        all(
            performance.get(key) == value
            for key, value in measured.items()
            if not key.startswith(replaced_validation_prefixes)
        ),
        f"{observation}: measured client artifact disagrees with summary",
    )
    validate_memory_artifact(summary, directory / "memory.jsonl", observation)

    warmup = load_json(directory / "warmup-client.json")
    expected_warmup = max(1, min(int(environment["warmup_requests"]), spec["requests"]))
    require(isinstance(warmup, dict) and warmup.get("requests") == expected_warmup, f"{observation}: observation warmup count mismatch")
    warmup_command = load_json(directory / "warmup-command.json")
    require(warmup_command == expected_query_command(spec, int(environment["full_timeout_ms"])), f"{observation}: observation warmup command mismatch")
    if spec["mode"] == "full":
        require(performance.get("excluded_validation_requests") == expected_warmup, f"{observation}: excluded semantic-validation count mismatch")
        for prefix in ("semantic_", "row_semantic_", "unordered_row_"):
            for key, value in warmup.items():
                if key.startswith(prefix):
                    require(performance.get(key) == value, f"{observation}: {key} does not match excluded validation")
        invocation = load_json(directory / "measured-client-invocation.json")
        require(isinstance(invocation, list) and invocation, f"{observation}: hiredis invocation is missing")
        executable_index = 3 if invocation[:2] == ["taskset", "-c"] else 0
        require(len(invocation) > executable_index and Path(str(invocation[executable_index])).name == "mod17473-hiredis-load", f"{observation}: unexpected full-load executable")
        require(invocation[:3] == ["taskset", "-c", environment["client_cpus"]], f"{observation}: hiredis CPU affinity mismatch")
        options = {
            "--host": "127.0.0.1",
            "--port": str(environment["node_ports"][0]),
            "--protocol": str(spec["protocol"]),
            "--clients": str(spec["raw"]["clients"]),
            "--requests": str(spec["requests"]),
            "--expected-rows": str(spec["raw"]["expected_rows"]),
        }
        for option, expected in options.items():
            positions = [index for index, value in enumerate(invocation) if value == option]
            require(len(positions) == 1 and positions[0] + 1 < len(invocation), f"{observation}: hiredis option {option} missing")
            require(invocation[positions[0] + 1] == expected, f"{observation}: hiredis option {option} mismatch")
        delimiter = invocation.index("--") if "--" in invocation else -1
        require(delimiter >= 0 and invocation[delimiter + 1:] == [str(value) for value in summary["query_command"]], f"{observation}: hiredis query invocation mismatch")


def memory_metric(summary: dict[str, Any], scope: str, metric: str) -> dict[str, Any]:
    key = metric if scope == "cluster" else f"{scope}_{metric}"
    value = summary.get("memory", {}).get("metrics", {}).get(key)
    require(isinstance(value, dict), f"{summary.get('observation')}: missing memory metric {key}")
    for field in (
        "before",
        "peak_during_load",
        "peak_minus_before",
        "after_cooldown",
        "cooldown_minus_before",
    ):
        require(finite(value.get(field)), f"{summary.get('observation')}: invalid {key}.{field}")
    return value


def validate_memory(summary: dict[str, Any], environment: dict[str, Any], observation: str) -> None:
    memory = summary.get("memory")
    require(isinstance(memory, dict), f"{observation}: memory summary missing")
    require(memory.get("sampler_errors") == 0, f"{observation}: sampler errors recorded")
    require(isinstance(memory.get("sample_count"), int) and memory["sample_count"] >= 3, f"{observation}: too few memory samples")
    require(isinstance(memory.get("load_window_sample_count"), int) and memory["load_window_sample_count"] >= 3, f"{observation}: too few load-window samples")
    cooldown = memory.get("cooldown_snapshots")
    offsets = environment.get("cooldown_offsets")
    require(isinstance(cooldown, list) and isinstance(offsets, list) and len(cooldown) == len(offsets), f"{observation}: cooldown snapshots mismatch")
    for sample, offset in zip(cooldown, offsets):
        require(close(sample.get("requested_offset_seconds"), offset), f"{observation}: cooldown offset mismatch")
    for metric in MEMORY_METRICS:
        total = memory_metric(summary, "cluster", metric)
        node0 = memory_metric(summary, "node0", metric)
        node1 = memory_metric(summary, "node1", metric)
        for field in ("before", "after_cooldown", "cooldown_minus_before"):
            require(close(total[field], node0[field] + node1[field]), f"{observation}: {metric}.{field} is not the two-node sum")


def reported_comparison(
    pair: dict[str, Any], category: str, metric: str, view: str | None = None
) -> dict[str, Any]:
    comparison = pair.get("comparison", {}).get(category, {})
    if category == "memory":
        comparison = comparison.get(metric, {})
        comparison = comparison.get(view, {})
    else:
        comparison = comparison.get(metric, {})
    require(isinstance(comparison, dict), f"{pair.get('case')}: missing reported {category} comparison")
    return comparison


def performance_row(summary: dict[str, Any], dotted: str, observation: str) -> float:
    value = nested(summary.get("performance", {}), dotted)
    return positive(value, f"{observation} performance.{dotted}")


def validate_full_pair(
    pair: dict[str, Any], baseline: dict[str, Any], candidate: dict[str, Any], spec: dict[str, Any]
) -> dict[str, dict[str, float]]:
    name = spec["name"]
    expected_rows = spec["requests"] * 10000
    expected_distribution = [{"value": 10000, "count": spec["requests"]}]
    validation = pair.get("validation", {})
    for flag in (
        "matching_effective_config",
        "matching_row_multiset_fingerprint",
        "matching_rows_returned",
        "matching_internal_fanout_call_counts",
        "pr_total_results_exact",
    ):
        require(validation.get(flag) is True, f"{name}: validation flag {flag} is not true")
    for summary in (baseline, candidate):
        perf = summary["performance"]
        observation = summary["observation"]
        require(perf.get("complete_reply_latency") is True, f"{observation}: latency does not certify complete replies")
        require(perf.get("load_engine") == "pinned-hiredis-complete-reply", f"{observation}: unexpected full-load engine")
        require(perf.get("semantic_validation_in_timed_window") is False, f"{observation}: semantic validation timing is ambiguous")
        require(perf.get("requests") == spec["requests"], f"{observation}: request count mismatch")
        require(perf.get("clients") == spec["raw"]["clients"], f"{observation}: client count mismatch")
        require(perf.get("protocol") == spec["protocol"], f"{observation}: protocol mismatch")
        require(perf.get("latency_ms", {}).get("count") == spec["requests"], f"{observation}: latency count mismatch")
        require(perf.get("rows_returned") == expected_rows, f"{observation}: full row total mismatch")
        require(perf.get("row_count_distribution") == expected_distribution, f"{observation}: full row distribution mismatch")
        require(perf.get("timeout_evidence_fraction") == 0, f"{observation}: timeout evidence in full case")
        require(perf.get("partial_response_count") == 0, f"{observation}: partial full response")
        require(perf.get("responses_with_warnings") == 0, f"{observation}: warning in full response")
        require(isinstance(perf.get("reply_bytes"), int) and perf["reply_bytes"] > 0, f"{observation}: invalid full reply bytes")
        latency = perf["latency_ms"]
        require(0 < perf.get("elapsed_seconds", 0), f"{observation}: invalid elapsed time")
        require(0 < latency["min"] <= latency["p50"], f"{observation}: invalid lower latency percentiles")
        require(latency["p50"] <= latency["p95"] <= latency["p99"] <= latency["max"] + 1e-6, f"{observation}: invalid latency percentiles")
        require(close(perf["throughput_rps"], spec["requests"] / perf["elapsed_seconds"]), f"{observation}: throughput calculation mismatch")
        require(close(perf["rows_per_second"], expected_rows / perf["elapsed_seconds"]), f"{observation}: row throughput calculation mismatch")
        require(close(perf["reply_bytes_per_second"], perf["reply_bytes"] / perf["elapsed_seconds"]), f"{observation}: byte throughput calculation mismatch")
    base_fingerprints = baseline["performance"].get("unordered_row_fingerprints", [])
    pr_fingerprints = candidate["performance"].get("unordered_row_fingerprints", [])
    require(len(base_fingerprints) == 1 and base_fingerprints == pr_fingerprints, f"{name}: row-multiset fingerprint mismatch")
    require(baseline["performance"].get("unordered_row_fingerprint_sample_count") == 1, f"{name}: baseline row fingerprint sample count mismatch")
    require(candidate["performance"].get("unordered_row_fingerprint_sample_count") == 1, f"{name}: PR row fingerprint sample count mismatch")
    baseline_perf = baseline["performance"]
    candidate_perf = candidate["performance"]
    if spec["protocol"] == 2:
        # The old streaming paths expose distinct placeholders: the inline shard
        # path reports one, while deferred shard execution reports its 1,000-row
        # cursor chunk. The PR must report the complete accumulated array length.
        baseline_total = 1 if spec["workers"] == 0 else 1000
    else:
        baseline_total = 10000
    baseline_mismatch = spec["requests"] if baseline_total != 10000 else 0
    baseline_distribution = [{"value": baseline_total, "count": spec["requests"]}]
    require(
        baseline_perf.get("total_results_distribution") == baseline_distribution,
        f"{name}: baseline total_results distribution does not match RESP{spec['protocol']} behavior",
    )
    require(baseline_perf.get("total_results_missing") == 0, f"{name}: baseline total_results is missing")
    require(baseline_perf.get("total_results_mismatch") == baseline_mismatch, f"{name}: baseline total_results mismatch counter is wrong")
    require(baseline_perf.get("total_results_below_rows") == baseline_mismatch, f"{name}: baseline below-row counter is wrong")
    require(baseline_perf.get("total_results_min") == baseline_total, f"{name}: baseline total_results minimum is wrong")
    require(baseline_perf.get("total_results_max") == baseline_total, f"{name}: baseline total_results maximum is wrong")
    require(candidate_perf.get("total_results_distribution") == expected_distribution, f"{name}: PR total_results is not exact")
    for field in ("total_results_missing", "total_results_mismatch", "total_results_below_rows"):
        require(candidate_perf.get(field) == 0, f"{name}: PR {field} is not zero")
    require(candidate_perf.get("total_results_min") == 10000, f"{name}: PR total_results minimum is wrong")
    require(candidate_perf.get("total_results_max") == 10000, f"{name}: PR total_results maximum is wrong")

    baseline_internal = validate_commandstats(baseline, spec, baseline["observation"])
    candidate_internal = validate_commandstats(candidate, spec, candidate["observation"])
    require(baseline_internal == validation.get("baseline_internal_commandstats"), f"{name}: baseline pair commandstats mismatch")
    require(candidate_internal == validation.get("pr_internal_commandstats"), f"{name}: PR pair commandstats mismatch")
    baseline_counts = {key: (value["calls"], value["failed_calls"]) for key, value in baseline_internal.items()}
    candidate_counts = {key: (value["calls"], value["failed_calls"]) for key, value in candidate_internal.items()}
    require(baseline_counts == candidate_counts, f"{name}: internal fan-out counters differ")

    rows: dict[str, dict[str, float]] = {}
    report_names = {
        "throughput_rps": "requests_per_second",
        "latency_p50_ms": "latency_p50_ms",
        "latency_p95_ms": "latency_p95_ms",
        "latency_p99_ms": "latency_p99_ms",
    }
    for metric, (path, _unit, _direction) in PERFORMANCE_METRICS.items():
        base_value = performance_row(baseline, path, baseline["observation"])
        pr_value = performance_row(candidate, path, candidate["observation"])
        reported = reported_comparison(pair, "performance", report_names[metric])
        require(close(reported.get("baseline"), base_value) and close(reported.get("pr"), pr_value), f"{name}: reported comparison disagrees for {metric}")
        rows[metric] = {"baseline": base_value, "pr": pr_value}
    return rows


def validate_timeout_pair(
    pair: dict[str, Any], baseline: dict[str, Any], candidate: dict[str, Any], spec: dict[str, Any]
) -> dict[str, dict[str, float]]:
    name = spec["name"]
    validation = pair.get("validation", {})
    require(validation.get("matching_effective_config") is True, f"{name}: configuration validation failed")
    for variant, summary in (("baseline", baseline), ("pr", candidate)):
        perf = summary["performance"]
        observation = summary["observation"]
        require(perf.get("requests") == spec["requests"], f"{observation}: request count mismatch")
        require(perf.get("clients") == spec["raw"]["clients"], f"{observation}: client count mismatch")
        require(perf.get("protocol") == spec["protocol"], f"{observation}: protocol mismatch")
        require(perf.get("latency_ms", {}).get("count") == spec["requests"], f"{observation}: response count mismatch")
        require(isinstance(perf.get("rows_returned"), int) and 0 <= perf["rows_returned"] <= spec["requests"] * 10000, f"{observation}: invalid rows returned")
        require(isinstance(perf.get("reply_bytes"), int) and perf["reply_bytes"] > 0, f"{observation}: invalid reply bytes")
        evidence = perf.get("timeout_evidence_fraction")
        require(finite(evidence) and 0 <= evidence <= 1, f"{observation}: invalid timeout evidence")
        require(isinstance(perf.get("timeout_evidence_count"), int) and close(perf["timeout_evidence_count"] / spec["requests"], evidence), f"{observation}: timeout evidence count mismatch")
        require(isinstance(perf.get("total_results_missing"), int) and perf["total_results_missing"] == 0, f"{observation}: total_results is missing")
        require(isinstance(perf.get("row_count_distribution"), list), f"{observation}: row distribution missing")
        require(sum(int(item["count"]) for item in perf["row_count_distribution"]) == spec["requests"], f"{observation}: row distribution response count mismatch")
        require(sum(int(item["value"]) * int(item["count"]) for item in perf["row_count_distribution"]) == perf["rows_returned"], f"{observation}: row distribution total mismatch")
        if variant == "pr":
            require(perf.get("total_results_mismatch") == 0, f"{observation}: PR total_results mismatch")
            require(perf.get("total_results_below_rows") == 0, f"{observation}: PR total_results is below returned rows")
            require(isinstance(perf.get("total_results_max"), int) and perf["total_results_max"] <= 10000, f"{observation}: PR total_results exceeds dataset")
            if spec["protocol"] == 3:
                require(perf.get("partial_without_timeout_warning") == 0, f"{observation}: RESP3 partial reply lacks timeout warning")
        parsed_internal = validate_commandstats(summary, spec, observation)
        reported_key = f"{variant}_internal_commandstats" if variant == "baseline" else "pr_internal_commandstats"
        require(parsed_internal == validation.get(reported_key), f"{name}: {variant} pair commandstats mismatch")
    require(baseline["performance"]["timeout_evidence_fraction"] > 0, f"{name}: baseline calibration did not exercise timeout")
    for field in (
        "baseline_timeout_evidence_fraction",
        "pr_timeout_evidence_fraction",
        "baseline_total_results_mismatch",
        "pr_total_results_mismatch",
        "baseline_total_results_missing",
        "pr_total_results_missing",
    ):
        variant = "baseline" if field.startswith("baseline_") else "pr"
        perf_key = field.removeprefix(f"{variant}_")
        source = baseline if variant == "baseline" else candidate
        require(validation.get(field) == source["performance"].get(perf_key), f"{name}: pair validation field {field} mismatch")
    requests = float(spec["requests"])
    return {
        "rows_per_reply": {
            "baseline": baseline["performance"]["rows_returned"] / requests,
            "pr": candidate["performance"]["rows_returned"] / requests,
        },
        "reply_mib_per_reply": {
            "baseline": baseline["performance"]["reply_bytes"] / requests / MIB,
            "pr": candidate["performance"]["reply_bytes"] / requests / MIB,
        },
        "timeout_evidence_percentage_points": {
            "baseline": baseline["performance"]["timeout_evidence_fraction"] * 100.0,
            "pr": candidate["performance"]["timeout_evidence_fraction"] * 100.0,
        },
    }


def validate_summary(
    root: Path,
    summary: dict[str, Any],
    summary_path: Path,
    observation: str,
    environment: dict[str, Any],
    specs: dict[str, dict[str, Any]],
) -> None:
    require(summary.get("observation") == observation, f"{observation}: summary name mismatch")
    require(summary.get("campaign_id") == environment["campaign_id"], f"{observation}: campaign ID mismatch")
    variant = summary.get("variant")
    require(variant in ("baseline", "pr"), f"{observation}: invalid variant")
    require(summary.get("module_sha256") == environment[f"{variant}_sha256"], f"{observation}: module hash mismatch")
    require(summary.get("revision") == environment[f"{variant}_revision"], f"{observation}: revision mismatch")
    case = summary.get("case")
    require(isinstance(case, dict) and case.get("name") in specs, f"{observation}: unknown case")
    spec = specs[case["name"]]
    for key in ("name", "protocol", "workers", "timeout_mode", "timeout_ms", "requests", "clients", "expected_rows", "fields", "limit"):
        require(case.get(key) == spec["raw"].get(key), f"{observation}: case field {key} mismatch")
    require(summary.get("validated_measured_aggregate_calls") == spec["requests"], f"{observation}: public aggregate-call count mismatch")
    command = summary.get("query_command")
    require(command == expected_query_command(spec, spec["timeout_ms"]), f"{observation}: query command mismatch")
    validate_artifacts(root, summary, summary_path)
    validate_identity(summary, environment, observation)
    validate_config(summary, spec, observation)
    validate_memory(summary, environment, observation)
    validate_commandstats(summary, spec, observation)
    validate_artifact_contents(root, summary, summary_path, spec, environment, observation)


def load_campaign(
    root: Path, profile: str
) -> tuple[
    dict[str, Any],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, Path],
    dict[str, dict[str, Any]],
    dict[str, Any],
]:
    environment = load_json(root / "environment.json")
    calibrated = load_json(root / "calibrated-cases.json")
    validation = load_json(root / "validation-summary.json")
    require(isinstance(environment, dict), "environment must be an object")
    environment_cases = validate_environment(environment, profile)
    require(environment.get("topology") == "single-host-two-primary-oss-cluster", "campaign is not the expected cluster topology")
    require(environment.get("observation_unit") == "one fresh two-primary topology", "unexpected observation unit")
    require(environment.get("servers_per_observation") == 2, "unexpected servers per observation")
    require(environment.get("expected_docs") == 10000, "unexpected dataset size")
    require(environment.get("seed") == 17473, "campaign seed mismatch")
    require(environment.get("campaign_id") == calibrated.get("campaign_id") == validation.get("campaign_id"), "top-level campaign IDs differ")
    require(validation.get("all_pairs_valid") is True, "harness did not validate all pairs")
    require(environment.get("memory_scope_note", "").startswith("unprefixed redis_*"), "cluster-total memory scope is undocumented")
    case_threads = environment.get("case_search_threads")
    require(isinstance(case_threads, dict), "case SEARCH_THREADS map is missing")
    raw_cases = calibrated.get("cases")
    require(isinstance(raw_cases, list), "calibrated cases are missing")
    specs: dict[str, dict[str, Any]] = {}
    for raw in raw_cases:
        name = raw.get("name") if isinstance(raw, dict) else None
        require(isinstance(name, str) and name not in specs, f"duplicate or malformed case: {name!r}")
        parts = validate_case_spec(raw, case_threads.get(name))
        specs[name] = parts | {"raw": raw}
    require(set(environment_cases) == set(specs), "environment/calibrated case sets differ")
    for name, spec in specs.items():
        environment_case = environment_cases[name]
        require(
            {key: value for key, value in spec["raw"].items() if key != "timeout_ms"}
            == {key: value for key, value in environment_case.items() if key != "timeout_ms"},
            f"{name}: calibrated case differs from environment case",
        )
    repetitions = environment.get("repetitions")
    require(isinstance(repetitions, int) and repetitions > 0, "invalid repetition count")
    if profile == "final":
        require(set(specs) == final_case_names(), "final campaign is not the exact 12-full/6-timeout matrix")
    calibration_accounting = validate_calibrations(
        root, environment, calibrated, specs, environment_cases
    )
    manifest_path = root / "manifest.jsonl"
    try:
        manifest = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignError(f"cannot read manifest {manifest_path}: {exc}") from exc
    expected_pairs = len(specs) * repetitions
    require(len(manifest) == expected_pairs * 2, f"manifest has {len(manifest)} observations, expected {expected_pairs * 2}")
    require(validation.get("pair_count") == expected_pairs, "validation pair count mismatch")
    pair_reports = validation.get("pairs")
    require(isinstance(pair_reports, list) and len(pair_reports) == expected_pairs, "validation pair list mismatch")
    pair_index: dict[tuple[str, int], dict[str, Any]] = {}
    for pair in pair_reports:
        require(isinstance(pair, dict), "pair report is malformed")
        require(pair.get("campaign_id") == environment["campaign_id"], "pair report campaign ID mismatch")
        pair_case = pair.get("case")
        pair_repetition = pair.get("repetition")
        require(isinstance(pair_case, str) and isinstance(pair_repetition, int), "pair report key is malformed")
        key = (pair_case, pair_repetition)
        require(key not in pair_index, f"duplicate pair report {key}")
        pair_path = root / "pair-validations" / f"r{pair_repetition:02d}-{pair_case}.json"
        require(pair_path.is_file(), f"missing pair-validation artifact: {pair_path}")
        require(load_json(pair_path) == pair, f"pair-validation artifact disagrees with summary: {pair_path}")
        pair_index[key] = pair

    summaries: dict[str, dict[str, Any]] = {}
    summary_paths: dict[str, Path] = {}
    seen: set[str] = set()
    for index, entry in enumerate(manifest):
        require(isinstance(entry, dict), f"manifest entry {index} is malformed")
        require(set(entry) == {"campaign_id", "observation", "relative_summary", "summary_sha256"}, f"manifest entry {index} has an unexpected schema")
        observation = entry.get("observation")
        require(isinstance(observation, str) and observation not in seen, f"manifest observation is duplicate/malformed: {observation!r}")
        seen.add(observation)
        require(entry.get("campaign_id") == environment["campaign_id"], f"{observation}: manifest campaign ID mismatch")
        relative = entry.get("relative_summary")
        require(isinstance(relative, str), f"{observation}: relative summary missing")
        path = safe_relative(root, relative, "summary")
        require(path.is_file(), f"{observation}: summary file missing")
        require(sha256(path) == entry.get("summary_sha256"), f"{observation}: summary hash mismatch")
        summary = load_json(path)
        require(isinstance(summary, dict), f"{observation}: summary must be an object")
        validate_summary(root, summary, path, observation, environment, specs)
        summaries[observation] = summary
        summary_paths[observation] = path
    return environment, specs, manifest, summaries, summary_paths, pair_index, calibration_accounting


def analyze(
    root: Path,
    profile: str,
    iterations: int,
    seed: int,
    source_bundle_sha256: str | None = None,
) -> dict[str, Any]:
    require(
        profile != "final" or source_bundle_sha256 is not None,
        "final analysis requires --source-bundle-sha256",
    )
    require(
        profile != "final" or source_bundle_sha256 == SOURCE_BUNDLE_SHA256,
        "final analysis source bundle SHA256 does not match the audited archive",
    )
    environment, specs, manifest, summaries, _summary_paths, pair_index, calibration_accounting = load_campaign(root, profile)
    replacement = validate_replacement_composition(root, environment, profile)
    if replacement is not None:
        expected_observations = [
            f"{REPLACEMENT_PAIR_STEM}-{variant}" for variant in REPLACEMENT_VARIANT_ORDER
        ]
        actual_observations = [
            manifest[position].get("observation") for position in REPLACEMENT_MANIFEST_POSITIONS
        ]
        require(
            actual_observations == expected_observations,
            "replacement observations are not at the audited manifest positions",
        )
    repetitions = int(environment["repetitions"])
    manifest_positions = {entry["observation"]: index for index, entry in enumerate(manifest)}
    pair_records: list[dict[str, Any]] = []
    orders: dict[str, Counter[str]] = defaultdict(Counter)
    seen_repetitions: dict[str, set[int]] = defaultdict(set)

    for key in sorted(pair_index, key=lambda item: (item[0], item[1])):
        name, repetition = key
        pair = pair_index[key]
        require(name in specs and isinstance(repetition, int), f"invalid pair key: {key}")
        require(0 <= repetition < repetitions, f"{name}: repetition out of range")
        require(repetition not in seen_repetitions[name], f"{name}: duplicate repetition {repetition}")
        seen_repetitions[name].add(repetition)
        base_id = pair.get("baseline_observation")
        pr_id = pair.get("pr_observation")
        require(base_id == f"r{repetition:02d}-{name}-baseline", f"{name}: baseline observation name mismatch")
        require(pr_id == f"r{repetition:02d}-{name}-pr", f"{name}: PR observation name mismatch")
        require(base_id in summaries and pr_id in summaries, f"{name}: pair summaries are missing")
        baseline = summaries[base_id]
        candidate = summaries[pr_id]
        require(baseline.get("variant") == "baseline" and candidate.get("variant") == "pr", f"{name}: pair variants mismatch")
        require(baseline.get("repetition") == repetition == candidate.get("repetition"), f"{name}: repetition mismatch")
        require(baseline["case"]["name"] == name == candidate["case"]["name"], f"{name}: summary case mismatch")
        require(abs(manifest_positions[base_id] - manifest_positions[pr_id]) == 1, f"{name} r{repetition}: paired observations are not adjacent")
        order = "AB" if manifest_positions[base_id] < manifest_positions[pr_id] else "BA"
        orders[name][order] += 1
        require(baseline["effective_config_before"] == candidate["effective_config_before"], f"{name}: baseline/PR configurations differ")
        spec = specs[name]
        record: dict[str, Any] = {
            "case": name,
            "repetition": repetition,
            "order": order,
            "sequence": min(manifest_positions[base_id], manifest_positions[pr_id]) // 2,
            "baseline_observation": base_id,
            "pr_observation": pr_id,
            "mode": spec["mode"],
            "protocol": spec["protocol"],
            "workers": spec["workers"],
            "search_threads": spec["search_threads"],
        }
        if spec["mode"] == "full":
            record["performance"] = validate_full_pair(pair, baseline, candidate, spec)
        else:
            record["timeout"] = validate_timeout_pair(pair, baseline, candidate, spec)
        memory: dict[str, Any] = {}
        for scope in MEMORY_SCOPES:
            memory[scope] = {}
            for metric in MEMORY_METRICS:
                base_metric = memory_metric(baseline, scope, metric)
                pr_metric = memory_metric(candidate, scope, metric)
                memory[scope][metric] = {
                    "peak_excursion": {
                        "baseline": float(base_metric["peak_minus_before"]),
                        "pr": float(pr_metric["peak_minus_before"]),
                    },
                    "cooldown_residual": {
                        "baseline": float(base_metric["cooldown_minus_before"]),
                        "pr": float(pr_metric["cooldown_minus_before"]),
                    },
                    "peak_absolute": {
                        "baseline": float(base_metric["peak_during_load"]),
                        "pr": float(pr_metric["peak_during_load"]),
                    },
                    "cooldown_absolute": {
                        "baseline": float(base_metric["after_cooldown"]),
                        "pr": float(pr_metric["after_cooldown"]),
                    },
                }
                if scope == "cluster":
                    reported = reported_comparison(pair, "memory", metric, "peak_excursion_from_before")
                    require(close(reported.get("baseline"), base_metric["peak_minus_before"]) and close(reported.get("pr"), pr_metric["peak_minus_before"]), f"{name}: reported memory peak mismatch for {metric}")
                    reported = reported_comparison(pair, "memory", metric, "cooldown_residual")
                    require(close(reported.get("baseline"), base_metric["cooldown_minus_before"]) and close(reported.get("pr"), pr_metric["cooldown_minus_before"]), f"{name}: reported memory cooldown mismatch for {metric}")
        record["memory"] = memory
        pair_records.append(record)

    expected_repetitions = set(range(repetitions))
    for name in specs:
        require(seen_repetitions[name] == expected_repetitions, f"{name}: incomplete repetition set")
        if profile == "final":
            require(orders[name] == Counter({"AB": 5, "BA": 5}), f"{name}: expected 5 AB and 5 BA pairs, got {dict(orders[name])}")

    full_aggregates: list[dict[str, Any]] = []
    full_names = sorted(name for name, spec in specs.items() if spec["mode"] == "full")
    for name in full_names:
        selected = [record for record in pair_records if record["case"] == name]
        for metric, (_path, unit, direction) in PERFORMANCE_METRICS.items():
            rows = [
                {
                    "repetition": record["repetition"],
                    "order": record["order"],
                    "baseline": record["performance"][metric]["baseline"],
                    "pr": record["performance"][metric]["pr"],
                }
                for record in selected
            ]
            aggregate = ratio_aggregate(rows, iterations, seed, f"performance:{name}:{metric}")
            full_aggregates.append(
                {
                    "case": name,
                    "protocol": specs[name]["protocol"],
                    "workers": specs[name]["workers"],
                    "search_threads": specs[name]["search_threads"],
                    "metric": metric,
                    "unit": unit,
                    "direction": direction,
                    "n_pairs": len(rows),
                    "ab_pairs": orders[name]["AB"],
                    "ba_pairs": orders[name]["BA"],
                }
                | aggregate
            )

    memory_aggregates: list[dict[str, Any]] = []
    for name in full_names:
        selected = [record for record in pair_records if record["case"] == name]
        for scope in MEMORY_SCOPES:
            for metric in MEMORY_METRICS:
                peak_rows = [
                    {
                        "repetition": record["repetition"],
                        "order": record["order"],
                        **record["memory"][scope][metric]["peak_excursion"],
                    }
                    for record in selected
                ]
                peak_delta = delta_aggregate(
                    peak_rows,
                    iterations,
                    seed,
                    f"memory:{name}:{scope}:{metric}:peak-delta",
                    MIB,
                )
                memory_aggregates.append(
                    {
                        "case": name,
                        "protocol": specs[name]["protocol"],
                        "workers": specs[name]["workers"],
                        "search_threads": specs[name]["search_threads"],
                        "scope": scope,
                        "metric": metric,
                        "view": "peak_excursion_delta",
                        "unit": "MiB",
                        "n_pairs": len(peak_rows),
                        "ab_pairs": orders[name]["AB"],
                        "ba_pairs": orders[name]["BA"],
                    }
                    | peak_delta
                )
                if all(row["baseline"] > 0 and row["pr"] > 0 for row in peak_rows):
                    peak_ratio = ratio_aggregate(
                        peak_rows,
                        iterations,
                        seed,
                        f"memory:{name}:{scope}:{metric}:peak-ratio",
                    )
                    memory_aggregates.append(
                        {
                            "case": name,
                            "protocol": specs[name]["protocol"],
                            "workers": specs[name]["workers"],
                            "search_threads": specs[name]["search_threads"],
                            "scope": scope,
                            "metric": metric,
                            "view": "peak_excursion_ratio",
                            "unit": "percent",
                            "n_pairs": len(peak_rows),
                            "ab_pairs": orders[name]["AB"],
                            "ba_pairs": orders[name]["BA"],
                        }
                        | peak_ratio
                    )
                cooldown_rows = [
                    {
                        "repetition": record["repetition"],
                        "order": record["order"],
                        **record["memory"][scope][metric]["cooldown_residual"],
                    }
                    for record in selected
                ]
                cooldown = delta_aggregate(
                    cooldown_rows,
                    iterations,
                    seed,
                    f"memory:{name}:{scope}:{metric}:cooldown",
                    MIB,
                )
                memory_aggregates.append(
                    {
                        "case": name,
                        "protocol": specs[name]["protocol"],
                        "workers": specs[name]["workers"],
                        "search_threads": specs[name]["search_threads"],
                        "scope": scope,
                        "metric": metric,
                        "view": "cooldown_residual_delta",
                        "unit": "MiB",
                        "n_pairs": len(cooldown_rows),
                        "ab_pairs": orders[name]["AB"],
                        "ba_pairs": orders[name]["BA"],
                    }
                    | cooldown
                )

    timeout_diagnostics: list[dict[str, Any]] = []
    for name in sorted(name for name, spec in specs.items() if spec["mode"] == "timeout"):
        selected = [record for record in pair_records if record["case"] == name]
        metrics: dict[str, Any] = {}
        for metric in (
            "rows_per_reply",
            "reply_mib_per_reply",
            "timeout_evidence_percentage_points",
        ):
            rows = [
                {
                    "repetition": record["repetition"],
                    "order": record["order"],
                    **record["timeout"][metric],
                }
                for record in selected
            ]
            metrics[metric] = delta_aggregate(
                rows,
                iterations,
                seed,
                f"timeout:{name}:{metric}",
                1.0,
            )
        timeout_diagnostics.append(
            {
                "case": name,
                "protocol": specs[name]["protocol"],
                "workers": specs[name]["workers"],
                "search_threads": specs[name]["search_threads"],
                "calibrated_timeout_ms": specs[name]["timeout_ms"],
                "n_pairs": len(selected),
                "ab_pairs": orders[name]["AB"],
                "ba_pairs": orders[name]["BA"],
                "interpretation": "unequal-work timeout diagnostic; no causal throughput or latency comparison",
                "metrics": metrics,
            }
        )

    full_cases = len(full_names)
    timeout_cases = len(specs) - full_cases
    pairs = len(pair_records)
    observations = pairs * 2
    full_calls = sum(specs[name]["requests"] * repetitions * 2 for name in full_names)
    timeout_calls = sum(
        specs[name]["requests"] * repetitions * 2
        for name in specs
        if specs[name]["mode"] == "timeout"
    )
    full_pairs = full_cases * repetitions
    timeout_pairs = timeout_cases * repetitions
    observation_warmup_calls = observations * int(environment["warmup_requests"])
    excluded_original_pair_calls = (
        specs[REPLACEMENT_CASE]["requests"] * 2 if replacement is not None else 0
    )
    excluded_original_warmup_calls = (
        int(environment["warmup_requests"]) * 2 if replacement is not None else 0
    )
    published_calls_including_excluded = (
        full_calls
        + timeout_calls
        + observation_warmup_calls
        + calibration_accounting["external_ft_aggregate_calls"]
    )
    sanitized_scripts = {
        Path(path).name: digest for path, digest in environment["scripts"].items()
    }
    return {
        "schema": SCHEMA,
        "campaign": {
            "campaign_id": environment["campaign_id"],
            "baseline_revision": environment["baseline_revision"],
            "baseline_sha256": environment["baseline_sha256"],
            "pr_revision": environment["pr_revision"],
            "pr_sha256": environment["pr_sha256"],
            "redis_server_source_revision": environment.get("redis_server_source_revision"),
            "redis_server_sha256": environment.get("redis_server_sha256"),
            "topology": environment["topology"],
            "dataset_documents": environment["expected_docs"],
            "dataset_sha256": environment.get("cluster_seed", {}).get("logical_dataset_sha256"),
            "cooldown_offsets_seconds": environment["cooldown_offsets"],
            "memory_scope": MEMORY_SCOPE_NOTE,
            "full_latency_boundary": FULL_LOAD_LATENCY_BOUNDARY,
            "hiredis_load_client_sha256": environment["hiredis_load_client_sha256"],
            "hiredis_load_client_source_sha256": environment["hiredis_load_client_source_sha256"],
            "harness_scripts": dict(sorted(sanitized_scripts.items())),
            "cpu_affinity": {
                "redis_primaries": environment["server_cpus"],
                "load_generator": environment["client_cpus"],
                "memory_sampler": environment["monitor_cpus"],
                "note": CPU_ALLOCATION_NOTE,
            },
            "hardware": {
                "cpu_model": "AMD EPYC 7R32",
                "logical_cpus": 16,
                "physical_cores": 8,
                "numa_nodes": 1,
            },
            "seed": environment["seed"],
            "source_bundle_sha256": source_bundle_sha256,
            "execution_semantics": (
                "Public multi-shard FT.AGGREGATE is coordinator-async/blocked in every case. "
                "W0 executes internal shard work inline; W1/W4 defer internal shard work. "
                "SEARCH_THREADS changes per-primary coordinator capacity, so this is an end-to-end "
                "sensitivity matrix rather than causal isolation of the serialization caller."
            ),
            "memory_peak_definition": (
                "maximum across the pre-load snapshot, samples whose monotonic timestamps fall in "
                "the client load window, and the immediate post-load snapshot, minus pre-load"
            ),
            "memory_sampling": {
                "sampler_tick_seconds": environment["sample_interval"],
                "allocator_effective_cadence_seconds": (
                    environment["sample_interval"] * environment["redis_every"]
                ),
                "pss_effective_cadence_seconds": (
                    environment["sample_interval"] * environment["smaps_every"]
                ),
            },
        },
        "validation": {
            "profile": profile,
            "valid": True,
            "case_count": len(specs),
            "full_case_count": full_cases,
            "timeout_case_count": timeout_cases,
            "repetitions_per_case": repetitions,
            "pair_count": pairs,
            "manifest_observations": len(manifest),
            "orders_by_case": {name: dict(orders[name]) for name in sorted(orders)},
            "artifact_hashes_checked": True,
            "summary_hashes_checked": True,
            "campaign_id_recomputed": True,
            "calibrations_recomputed": True,
            "raw_artifacts_reconciled": True,
            "replacement_composition": replacement,
            "full_reply_metadata": {
                "baseline_resp2_workers_0_total_results": 1,
                "baseline_resp2_workers_1_or_4_total_results": 1000,
                "baseline_resp3_total_results": 10000,
                "pr_all_paths_total_results": 10000,
                "returned_rows_per_reply": 10000,
            },
            "top_level_sha256": {
                "environment.json": sha256(root / "environment.json"),
                "calibrated-cases.json": sha256(root / "calibrated-cases.json"),
                "manifest.jsonl": sha256(root / "manifest.jsonl"),
                "validation-summary.json": sha256(root / "validation-summary.json"),
            },
        },
        "run_accounting": {
            "measured_pairs": pairs,
            "excluded_original_pair_executions": 1 if replacement is not None else 0,
            "clean_replacement_pairs_in_published_n": 1 if replacement is not None else 0,
            "executed_pair_comparisons_including_replaced_original": (
                pairs + (1 if replacement is not None else 0)
            ),
            "preliminary_or_smoke_campaigns_included": False,
            "measured_topology_observations": observations,
            "executed_topology_observations_including_replaced_original": (
                observations + (2 if replacement is not None else 0)
            ),
            "all_successful_topology_starts_including_calibration_and_replaced_original": (
                observations
                + (2 if replacement is not None else 0)
                + calibration_accounting["baseline_topology_starts"]
            ),
            "redis_process_executions": observations * 2,
            "executed_redis_processes_including_replaced_original": (
                observations * 2 + (4 if replacement is not None else 0)
            ),
            "all_redis_process_executions_including_calibration_and_replaced_original": (
                observations * 2
                + (4 if replacement is not None else 0)
                + calibration_accounting["redis_process_executions"]
            ),
            "full_external_ft_aggregate_calls": full_calls,
            "timeout_external_ft_aggregate_calls": timeout_calls,
            "total_external_ft_aggregate_calls": full_calls + timeout_calls,
            "excluded_observation_warmup_client_invocations": observations,
            "excluded_observation_warmup_ft_aggregate_calls": observation_warmup_calls,
            "excluded_replaced_original_measured_ft_aggregate_calls": excluded_original_pair_calls,
            "excluded_replaced_original_warmup_ft_aggregate_calls": excluded_original_warmup_calls,
            "published_active_external_ft_aggregate_calls_including_warmups_and_calibration": (
                published_calls_including_excluded
            ),
            "all_external_ft_aggregate_calls_including_replaced_original": (
                published_calls_including_excluded
                + excluded_original_pair_calls
                + excluded_original_warmup_calls
            ),
            "subsets": {
                "full": {
                    "cases": full_cases,
                    "pairs": full_pairs,
                    "topology_observations": full_pairs * 2,
                    "redis_process_executions": full_pairs * 4,
                },
                "timeout": {
                    "cases": timeout_cases,
                    "pairs": timeout_pairs,
                    "topology_observations": timeout_pairs * 2,
                    "redis_process_executions": timeout_pairs * 4,
                },
            },
            "per_case": {
                "pairs": repetitions,
                "topology_observations": repetitions * 2,
                "redis_process_executions": repetitions * 4,
            },
            "timeout_calibration": calibration_accounting | {"included_in_measured_n": False},
            "bootstrap_resamples_are_runs": False,
        },
        "bootstrap": {
            "method": "within-case, AB/BA-order-stratified whole-pair nonparametric percentile bootstrap",
            "iterations": iterations,
            "seed": seed,
            "confidence": 0.95,
        },
        "cases": {
            name: {
                key: value
                for key, value in spec.items()
                if key in ("mode", "protocol", "workers", "search_threads", "timeout_ms", "requests")
            }
            for name, spec in sorted(specs.items())
        },
        "full_performance": full_aggregates,
        "full_memory": memory_aggregates,
        "timeout_diagnostics": timeout_diagnostics,
    }


def format_percent(value: float) -> str:
    return f"{value:+.2f}%"


def render_markdown(analysis: dict[str, Any]) -> str:
    runs = analysis["run_accounting"]
    validation = analysis["validation"]
    if validation["profile"] == "final":
        pairing_sentence = (
            "Each final-case estimate uses 10 whole baseline/PR pairs (5 AB, 5 BA)."
        )
        execution_accounting = [
            "One original BA pair was conservatively excluded and replaced; the clean replacement "
            "occupies that pair's published N. Including the discarded original and the six "
            "baseline-only calibration topologies, the final evidence lifecycle executed "
            f"{runs['executed_pair_comparisons_including_replaced_original']} paired comparisons, "
            f"{runs['all_successful_topology_starts_including_calibration_and_replaced_original']} "
            "topology starts, "
            f"{runs['all_redis_process_executions_including_calibration_and_replaced_original']} "
            "Redis process executions, and "
            f"{runs['all_external_ft_aggregate_calls_including_replaced_original']:,} external "
            "FT.AGGREGATE calls.",
            "",
        ]
    else:
        pairing_sentence = (
            f"This environment-profile validation uses {validation['repetitions_per_case']} "
            "baseline/PR pair(s) per included case; it is not a final campaign estimate."
        )
        execution_accounting = []
    lines = [
        "# MOD-17473 cluster benchmark analysis",
        "",
        f"Validated {validation['case_count']} cases × {validation['repetitions_per_case']} paired runs: "
        f"{runs['measured_pairs']} baseline/PR pairs, {runs['measured_topology_observations']} fresh two-primary "
        f"topology observations, and {runs['redis_process_executions']} Redis process executions.",
        "",
        pairing_sentence + " Confidence intervals are "
        f"95% percentile intervals from {analysis['bootstrap']['iterations']:,} deterministic, AB/BA-order-"
        f"stratified whole-pair bootstrap resamples (seed {analysis['bootstrap']['seed']}). Bootstrap "
        "resamples are not benchmark runs.",
        "",
        *execution_accounting,
        "## Full, equal-row-work performance",
        "",
        "Positive throughput deltas are faster; positive latency deltas are slower.",
        "The estimator is the geometric mean of the per-pair PR/baseline ratios, displayed as "
        "(PR/baseline − 1) × 100%. Public cluster requests are coordinator-async/blocked in every "
        "case; the WORKERS and SEARCH_THREADS matrix is an end-to-end sensitivity test.",
        "",
        "| SEARCH_THREADS | RESP | Workers | Metric | PR vs baseline | 95% CI | N |",
        "|---:|---:|---:|---|---:|---:|---:|",
    ]
    for row in sorted(
        analysis["full_performance"],
        key=lambda value: (value["search_threads"], value["protocol"], value["workers"], value["metric"]),
    ):
        lines.append(
            f"| {row['search_threads']} | {row['protocol']} | {row['workers']} | {row['metric']} | "
            f"{format_percent(row['effect_percent'])} | "
            f"[{format_percent(row['ci95_percent'][0])}, {format_percent(row['ci95_percent'][1])}] | "
            f"{row['n_pairs']} pairs |"
        )
    lines += [
        "",
        "## Full-case cluster-total memory",
        "",
        "Observed peak is the maximum of the pre-load snapshot, in-window samples, and immediate "
        "post-load snapshot, minus pre-load. Cooldown is the final cooldown snapshot minus pre-load "
        "and may be negative. Both effects are paired median PR−baseline MiB deltas; positive means "
        "the PR used more memory. Relative peak ratios remain available in the analysis JSON.",
        "",
        "| SEARCH_THREADS | RESP | Workers | Metric | View | Effect | 95% CI | N |",
        "|---:|---:|---:|---|---|---:|---:|---:|",
    ]
    for row in sorted(
        (
            value
            for value in analysis["full_memory"]
            if value["scope"] == "cluster"
            and value["view"] in {"peak_excursion_delta", "cooldown_residual_delta"}
        ),
        key=lambda value: (value["search_threads"], value["protocol"], value["workers"], value["metric"], value["view"]),
    ):
        effect = f"{row['effect']:+.2f} MiB"
        interval = f"[{row['ci95'][0]:+.2f}, {row['ci95'][1]:+.2f}] MiB"
        view = (
            "observed peak PR−baseline"
            if row["view"] == "peak_excursion_delta"
            else "final cooldown PR−baseline"
        )
        lines.append(
            f"| {row['search_threads']} | {row['protocol']} | {row['workers']} | {row['metric']} | "
            f"{view} | {effect} | {interval} | {row['n_pairs']} pairs |"
        )
    lines += [
        "",
        "Node0 (ingress/coordinator) and node1 breakdowns are retained in the analysis JSON under "
        "`full_memory`; unprefixed cluster totals are sums sampled across both Redis primaries.",
        "The memory sampler shares one physical core with the load-generator CPU set; this is held "
        "constant for both variants and every case.",
        "",
        "## Calibrated-timeout diagnostics",
        "",
        "Timeout cases return unequal work and therefore have no causal RPS or latency comparison. Values "
        "below are descriptive paired median PR−baseline deltas; calibration runs are excluded from N. "
        "Timeout evidence means a partial reply for RESP2 and a timeout-warning reply for RESP3.",
        "",
        "| RESP | Workers | Timeout | Metric | Baseline median | PR median | Paired delta (95% CI) | N |",
        "|---:|---:|---:|---|---:|---:|---:|---:|",
    ]
    for case in sorted(analysis["timeout_diagnostics"], key=lambda value: (value["protocol"], value["workers"])):
        for metric, row in case["metrics"].items():
            lines.append(
                f"| {case['protocol']} | {case['workers']} | {case['calibrated_timeout_ms']} ms | {metric} | "
                f"{row['baseline_median']:.3f} | {row['pr_median']:.3f} | "
                f"{row['effect']:+.3f} [{row['ci95'][0]:+.3f}, {row['ci95'][1]:+.3f}] | "
                f"{case['n_pairs']} pairs |"
            )
    lines += [
        "",
        f"Measured external FT.AGGREGATE calls: {runs['full_external_ft_aggregate_calls']:,} full + "
        f"{runs['timeout_external_ft_aggregate_calls']:,} timeout = {runs['total_external_ft_aggregate_calls']:,}. "
        f"The {runs['timeout_calibration']['baseline_topology_starts']} baseline-only timeout calibrations "
        f"used {runs['timeout_calibration']['probe_client_invocations']} timeout-budget probe client invocations and "
        f"{runs['timeout_calibration']['external_ft_aggregate_calls']:,} FT.AGGREGATE calls; they are "
        "excluded from measured N. Observation warmups add "
        f"{runs['excluded_observation_warmup_ft_aggregate_calls']:,} more excluded calls.",
        "",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path)
    parser.add_argument("--profile", choices=("final", "environment"), default="final")
    parser.add_argument("--bootstrap-iterations", type=int, default=10000)
    parser.add_argument("--bootstrap-seed", type=int, default=17473)
    parser.add_argument(
        "--source-bundle-sha256",
        help="SHA256 of the externally archived raw campaign evidence",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        root = args.campaign.expanduser().resolve()
        require(root.is_dir(), f"campaign directory does not exist: {root}")
        require(args.bootstrap_iterations > 0, "bootstrap iterations must be positive")
        if args.source_bundle_sha256 is not None:
            require_sha256(args.source_bundle_sha256, "source bundle")
        analysis = analyze(
            root,
            args.profile,
            args.bootstrap_iterations,
            args.bootstrap_seed,
            args.source_bundle_sha256,
        )
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(analysis, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        if args.output_markdown:
            args.output_markdown.parent.mkdir(parents=True, exist_ok=True)
            args.output_markdown.write_text(render_markdown(analysis), encoding="utf-8")
    except CampaignError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(
        f"validated {analysis['validation']['pair_count']} pairs; "
        f"wrote {args.output_json}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
