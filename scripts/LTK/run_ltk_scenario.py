#!/usr/bin/env python3
"""Run the LTK monitor-replay and field-consistency scenario on Redis OSS."""

# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import argparse
import concurrent.futures
import ipaddress
import logging
import os
import random
import re
import sys
import tempfile
import threading
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

INDEX_NAME = "idx:acs"
S3_BUCKET = "rl-automation"
S3_KEY = "rlauto/search-customer-scenarios/ltk/monitor.txt"
S3_EXPECTED_BUCKET_OWNER = "620187402834"
SEARCH_TIMEOUT_MS = 2_000
FT_TIMEOUT_RATE_BUDGET = 0.01
MIN_REPLAY_COMPLETION_RATIO = 0.99
REPLAY_DURATION_SEC = 600.0
READ_PIPELINE_BATCH = 50
REPLAY_SOCKET_TIMEOUT_SEC = 60.0
PROBE_SOCKET_TIMEOUT_SEC = 5.0
VALIDATOR_CYCLE_SEC = 5.0
TTL_PROBE_PATTERN = "A#PR#*"
TTL_PROBE_KEY_PREFIX = b"A#PR#"
TTL_PROBE_ID_FIELD = "product_id"
TTL_PROBE_SAMPLE_SIZE = 1_000
TTL_PROBE_EXPIRE_SEC = 360
TTL_SURVIVOR_SAMPLE_SIZE = 5
TTL_SURVIVOR_EXPIRE_SEC = 7 * 24 * 3600
TRACKED_KEYS_BY_PREFIX = {"A#PR#*": 24_000, "A#PO#*": 3_000, "A#CR#*": 3_000}
FT_WALK_TIMEOUT_MS = 1_000_000
INDEX_LOOKUP_BATCH = 200
LTK_SEARCH_CONFIG = {
    "search-workers": "12",
    "search-cursor-max-idle": "60000",
    "search-workers-priority-bias-threshold": "6",
    "search-enable-unstable-features": "yes",
    "search-bm25std-tanh-factor": "200",
    "search-default-dialect": "2",
    "search-index-cursor-limit": "256",
}

LOGGER = logging.getLogger("ltk-scenario")

_LINE_RE = re.compile(r"^\[ ([^\]]+) \]\s+([\d.]+)\s+\[(\d+)\s+([^\]]+)\]\s+(.*)$")
_QUOTED_RE = re.compile(r'"((?:\\.|[^"\\])*)"')
_OK_LINE_RE = re.compile(r"^\[ [^\]]+ \]\s+OK\s*$")
_WRITES = {"HSET", "HDEL", "EXPIRE", "UNLINK"}
_FT_COMMANDS = {"FT.SEARCH", "FT.AGGREGATE"}
_SKIP_COMMANDS = {
    "HELLO",
    "AUTH",
    "SELECT",
    "RESET",
    "QUIT",
    "CLIENT",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    "MONITOR",
    "DEBUG",
    "CLUSTER",
    "CONFIG",
    "FAILOVER",
    "SHUTDOWN",
    "REPLICAOF",
    "SLAVEOF",
}
_SIMPLE_UNESCAPES = {
    "\\": "\\",
    '"': '"',
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "a": "\a",
    "b": "\b",
}
_COORDINATOR_ERROR_RE = re.compile("coordinator", re.IGNORECASE)
_TIMEOUT_ERROR_RE = re.compile(
    r"(?:SEARCH_TIMEOUT )?Timeout limit was reached\.?",
    re.IGNORECASE,
)

Command = tuple[str | bytes, ...]


@dataclass
class ReplayBuckets:
    writes: list[Command] = field(default_factory=list)
    ft: list[Command] = field(default_factory=list)
    reads: list[Command] = field(default_factory=list)


@dataclass
class TraceTotals:
    insert_keys: set[bytes] = field(default_factory=set)
    delete_keys: set[bytes] = field(default_factory=set)
    expire_keys: set[bytes] = field(default_factory=set)
    hdel_keys: set[bytes] = field(default_factory=set)
    expire_ttl_values: list[int] = field(default_factory=list)

    @property
    def inserts(self) -> int:
        return len(self.insert_keys)

    @property
    def deletes(self) -> int:
        return len(self.delete_keys)

    @property
    def expires(self) -> int:
        return len(self.expire_keys)

    def expire_ttl_summary(self) -> str:
        if not self.expire_ttl_values:
            return "<no EXPIRE commands>"
        values = sorted(self.expire_ttl_values)
        within_window = sum(1 for value in values if 0 <= value <= 1_800)
        return (
            f"count={len(values)} min={values[0]}s p50={values[len(values) // 2]}s "
            f"p95={values[int(0.95 * len(values))]}s max={values[-1]}s "
            f"test_window_pct={100.0 * within_window / len(values):.2f}%"
        )


@dataclass(frozen=True)
class FtInfoSample:
    num_docs: int
    indexing: int
    cleaning: int
    hash_indexing_failures: int


@dataclass(frozen=True)
class IndexSchema:
    fields: frozenset[str]
    types: dict[str, str]
    paths: dict[str, str]
    sortable_paths: frozenset[str]


@dataclass
class ValidatorReport:
    info_samples: list[FtInfoSample] = field(default_factory=list)
    probe_samples: int = 0
    failures: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TtlKeySet:
    keys: list[bytes]
    ids: list[str]
    expire_at_unix: float


GOLDEN_PROBES: tuple[tuple[str, Command], ...] = (
    ("all_sample", ("FT.SEARCH", INDEX_NAME, "*", "LIMIT", "0", "50")),
    ("profile_sample", ("FT.SEARCH", INDEX_NAME, "@type:{Profile}", "LIMIT", "0", "20")),
    ("post_sample", ("FT.SEARCH", INDEX_NAME, "@type:{Post}", "LIMIT", "0", "20")),
    ("text_query", ("FT.SEARCH", INDEX_NAME, "@full_name:redis", "LIMIT", "0", "5")),
    ("numeric_range", ("FT.SEARCH", INDEX_NAME, "@iscore:[0 +inf]", "LIMIT", "0", "5")),
    (
        "count_aggregate",
        (
            "FT.AGGREGATE",
            INDEX_NAME,
            "*",
            "GROUPBY",
            "1",
            "@type",
            "REDUCE",
            "COUNT",
            "0",
        ),
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard_ip", type=ipaddress.ip_address)
    parser.add_argument("--port", type=int, default=7000)
    parser.add_argument("--username", default=os.getenv("REDIS_USERNAME"))
    parser.add_argument(
        "--password",
        default=os.getenv("REDIS_PASSWORD") or os.getenv("REDISCLI_AUTH"),
        help="Redis password; prefer REDIS_PASSWORD instead of the command line",
    )
    parser.add_argument("--tls", action="store_true")
    parser.add_argument(
        "--monitor-trace",
        type=Path,
        help="Use a local monitor trace instead of downloading the default trace from S3",
    )
    parser.add_argument("--s3-bucket", default=S3_BUCKET)
    parser.add_argument("--s3-key", default=S3_KEY)
    parser.add_argument("--s3-expected-bucket-owner", default=S3_EXPECTED_BUCKET_OWNER)
    parser.add_argument("--replay-duration", type=float, default=REPLAY_DURATION_SEC)
    parser.add_argument("--search-timeout-ms", type=int, default=SEARCH_TIMEOUT_MS)
    parser.add_argument("--ft-timeout-rate-budget", type=float, default=FT_TIMEOUT_RATE_BUDGET)
    parser.add_argument(
        "--min-replay-completion-ratio",
        type=float,
        default=MIN_REPLAY_COMPLETION_RATIO,
    )
    parser.add_argument("--ttl-probe-count", type=int, default=TTL_PROBE_SAMPLE_SIZE)
    parser.add_argument("--ttl-probe-seconds", type=int, default=TTL_PROBE_EXPIRE_SEC)
    parser.add_argument("--ttl-survivor-count", type=int, default=TTL_SURVIVOR_SAMPLE_SIZE)
    parser.add_argument("--rng-seed", type=int)
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING"), default="INFO")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.replay_duration <= 0:
        parser.error("--replay-duration must be positive")
    if args.search_timeout_ms <= 0:
        parser.error("--search-timeout-ms must be positive")
    if not re.fullmatch(r"\d{12}", args.s3_expected_bucket_owner):
        parser.error("--s3-expected-bucket-owner must be a 12-digit AWS account ID")
    if not 0 <= args.ft_timeout_rate_budget <= 1:
        parser.error("--ft-timeout-rate-budget must be between 0 and 1")
    if not 0 < args.min_replay_completion_ratio <= 1:
        parser.error("--min-replay-completion-ratio must be greater than 0 and at most 1")
    if args.ttl_probe_count < 0 or args.ttl_survivor_count < 0:
        parser.error("TTL sample counts cannot be negative")
    if args.ttl_probe_seconds <= 0:
        parser.error("--ttl-probe-seconds must be positive")
    return args


def connection_kwargs(args: argparse.Namespace, socket_timeout: float) -> dict[str, Any]:
    return {
        "username": args.username,
        "password": args.password,
        "ssl": args.tls,
        "socket_connect_timeout": socket_timeout,
        "socket_timeout": socket_timeout,
        "decode_responses": False,
    }


def make_cluster_client(redis_module: Any, args: argparse.Namespace, timeout: float) -> Any:
    return redis_module.RedisCluster(
        host=str(args.shard_ip),
        port=args.port,
        **connection_kwargs(args, timeout),
    )


def make_search_client(redis_module: Any, args: argparse.Namespace, timeout: float) -> Any:
    return redis_module.Redis(
        host=str(args.shard_ip),
        port=args.port,
        **connection_kwargs(args, timeout),
    )


def decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("latin-1", errors="replace")
    return str(value)


def flat_pairs(values: Iterable[Any]) -> dict[str, Any]:
    if isinstance(values, dict):
        return {decode(key): value for key, value in values.items()}
    items = list(values)
    return {decode(items[i]): items[i + 1] for i in range(0, len(items) - 1, 2)}


def expected_search_config(args: argparse.Namespace) -> dict[str, str]:
    return {
        **LTK_SEARCH_CONFIG,
        "search-timeout": str(args.search_timeout_ms),
        "search-on-timeout": "fail",
    }


def search_config_matches(name: str, actual: str | None, expected: str) -> bool:
    if actual is None:
        return False
    if name == "search-enable-unstable-features":
        return actual.lower() in {"1", "true", "yes"}
    return actual.lower() == expected.lower()


def verify_search_config_on_primaries(redis_module: Any, args: argparse.Namespace) -> None:
    cluster = make_cluster_client(redis_module, args, PROBE_SOCKET_TIMEOUT_SEC)
    failures: list[str] = []
    expected = expected_search_config(args)
    try:
        primaries = list(cluster.get_primaries())
        if not primaries:
            raise RuntimeError("Redis Cluster returned no primary nodes")
        for node in primaries:
            client = redis_module.Redis(
                host=node.host,
                port=node.port,
                **connection_kwargs(args, PROBE_SOCKET_TIMEOUT_SEC),
            )
            try:
                config = client.execute_command("CONFIG", "GET", *expected)
                actual = {name: decode(value) for name, value in flat_pairs(config).items()}
                for name, value in expected.items():
                    if not search_config_matches(name, actual.get(name), value):
                        failures.append(
                            f"{node.host}:{node.port} {name}={actual.get(name)!r}, "
                            f"expected {value!r}"
                        )
            finally:
                client.close()
    finally:
        cluster.close()
    if failures:
        raise RuntimeError("LTK Search configuration mismatch:\n  " + "\n  ".join(failures))
    LOGGER.info("LTK Search configuration verified on all primaries")


@contextmanager
def obtain_monitor_trace(args: argparse.Namespace) -> Iterator[Path]:
    if args.monitor_trace is not None:
        if not args.monitor_trace.is_file():
            raise RuntimeError(f"monitor trace does not exist: {args.monitor_trace}")
        yield args.monitor_trace
        return
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError(
            "boto3 is required to download the monitor trace; "
            "install boto3 or use --monitor-trace"
        ) from error
    with tempfile.TemporaryDirectory(prefix="ltk-monitor-") as temp_dir_name:
        destination = Path(temp_dir_name) / "monitor.txt"
        LOGGER.info("Downloading s3://%s/%s to %s", args.s3_bucket, args.s3_key, destination)
        boto3.client("s3").download_file(
            args.s3_bucket,
            args.s3_key,
            str(destination),
            ExtraArgs={"ExpectedBucketOwner": args.s3_expected_bucket_owner},
        )
        yield destination


def unescape_monitor_token(value: str) -> str:
    if "\\" not in value:
        return value
    output: list[str] = []
    index = 0
    while index < len(value):
        current = value[index]
        if current != "\\" or index + 1 == len(value):
            output.append(current)
            index += 1
            continue
        following = value[index + 1]
        if following in _SIMPLE_UNESCAPES:
            output.append(_SIMPLE_UNESCAPES[following])
            index += 2
        elif following == "x" and index + 4 <= len(value):
            try:
                output.append(chr(int(value[index + 2 : index + 4], 16)))
                index += 4
            except ValueError:
                output.append(current)
                index += 1
        else:
            output.append(current)
            index += 1
    return "".join(output)


def parse_monitor_line(line: str) -> Command | None:
    line = line.rstrip("\r\n")
    if _OK_LINE_RE.match(line):
        return None
    match = _LINE_RE.match(line)
    if match is None:
        return None
    parts = [unescape_monitor_token(token) for token in _QUOTED_RE.findall(match.group(5))]
    if not parts:
        return None
    command = parts[0].upper()
    if command in _SKIP_COMMANDS:
        return None
    return (command, *(part.encode("latin-1") for part in parts[1:]))


def partition_monitor_trace(path: Path) -> tuple[ReplayBuckets, TraceTotals]:
    buckets = ReplayBuckets()
    totals = TraceTotals()
    bucket_lists = (buckets.writes, buckets.ft, buckets.reads)
    with path.open("r", encoding="latin-1") as trace:
        for line in trace:
            parsed = parse_monitor_line(line)
            if parsed is None:
                continue
            command = str(parsed[0])
            bucket_index = 0 if command in _WRITES else 1 if command in _FT_COMMANDS else 2
            if command == "UNLINK":
                # A monitor trace can contain keys from several slots in one command, while
                # Redis Cluster only accepts a multi-key UNLINK when every key shares a slot.
                buckets.writes.extend((command, arg) for arg in parsed[1:])
            else:
                bucket_lists[bucket_index].append(parsed)
            if len(parsed) < 2:
                continue
            first_key = parsed[1]
            assert isinstance(first_key, bytes)
            if command == "HSET":
                totals.insert_keys.add(first_key)
            elif command == "UNLINK":
                totals.delete_keys.update(arg for arg in parsed[1:] if isinstance(arg, bytes))
            elif command == "EXPIRE":
                totals.expire_keys.add(first_key)
                if len(parsed) >= 3:
                    try:
                        totals.expire_ttl_values.append(int(parsed[2]))
                    except (TypeError, ValueError):
                        pass
            elif command == "HDEL":
                totals.hdel_keys.add(first_key)
    return buckets, totals


def validate_trace_buckets(buckets: ReplayBuckets) -> None:
    empty = [
        name
        for name, commands in (
            ("writes", buckets.writes),
            ("FT queries", buckets.ft),
            ("reads", buckets.reads),
        )
        if not commands
    ]
    if empty:
        raise RuntimeError(
            "monitor trace did not produce required command buckets: " + ", ".join(empty)
        )


def derive_product_id(key: bytes) -> str:
    return key[len(TTL_PROBE_KEY_PREFIX) :].decode("latin-1", errors="replace")


def seed_ttl_keys(
    cluster: Any,
    *,
    label: str,
    exclude_keys: set[bytes],
    sample_size: int,
    expire_seconds: int,
) -> TtlKeySet:
    if sample_size == 0:
        return TtlKeySet(keys=[], ids=[], expire_at_unix=time.time() + expire_seconds)
    candidates: list[bytes] = []
    for key in cluster.scan_iter(match=TTL_PROBE_PATTERN, count=1_000):
        if key in exclude_keys:
            continue
        candidates.append(key)
        if len(candidates) >= sample_size:
            break
    if len(candidates) < sample_size:
        raise RuntimeError(
            f"only {len(candidates)} eligible {label} keys matched {TTL_PROBE_PATTERN}; "
            f"need {sample_size}"
        )
    sanity_keys = candidates[:3]
    if sanity_keys:
        pipeline = cluster.pipeline(transaction=False)
        for key in sanity_keys:
            pipeline.hget(key, TTL_PROBE_ID_FIELD)
        for key, raw_id in zip(sanity_keys, pipeline.execute()):
            if raw_id is not None and derive_product_id(key) != decode(raw_id):
                raise RuntimeError(
                    f"{label} key/id convention failed for {decode(key)}: "
                    f"derived {derive_product_id(key)!r}, hash has {decode(raw_id)!r}"
                )
    deadline = time.time() + expire_seconds
    deadline_ms = int(deadline * 1_000)
    failures = 0
    for start in range(0, len(candidates), 200):
        pipeline = cluster.pipeline(transaction=False)
        for key in candidates[start : start + 200]:
            pipeline.pexpireat(key, deadline_ms)
        failures += sum(result == 0 for result in pipeline.execute())
    if failures:
        raise RuntimeError(f"PEXPIREAT failed for {failures}/{len(candidates)} {label} keys")
    ids = [derive_product_id(key) for key in candidates]
    LOGGER.info(
        "%s seeded: keys=%d ttl=%ds deadline=%.0f samples=%s",
        label,
        len(candidates),
        expire_seconds,
        deadline,
        [decode(key) for key in candidates[:3]],
    )
    return TtlKeySet(keys=candidates, ids=ids, expire_at_unix=deadline)


def reservoir_sample_tracked_keys(
    cluster: Any,
    excluded_keys: set[bytes],
    rng: random.Random,
) -> list[bytes]:
    combined: list[bytes] = []
    for pattern, target in TRACKED_KEYS_BY_PREFIX.items():
        reservoir: list[bytes] = []
        seen = 0
        for key in cluster.scan_iter(match=pattern, count=5_000):
            if key in excluded_keys:
                continue
            seen += 1
            if len(reservoir) < target:
                reservoir.append(key)
            else:
                replacement = rng.randrange(seen)
                if replacement < target:
                    reservoir[replacement] = key
        LOGGER.info("Tracked sample %s: selected=%d eligible=%d", pattern, len(reservoir), seen)
        if seen < target:
            LOGGER.warning("Tracked sample %s is smaller than target=%d", pattern, target)
        combined.extend(reservoir)
    return combined


def capture_ft_info(search: Any) -> FtInfoSample:
    info = flat_pairs(search.execute_command("FT.INFO", INDEX_NAME))
    return FtInfoSample(
        num_docs=int(info["num_docs"]),
        indexing=int(info["indexing"]),
        cleaning=int(info["cleaning"]),
        hash_indexing_failures=int(info["hash_indexing_failures"]),
    )


def capture_index_schema(search: Any) -> IndexSchema:
    info = flat_pairs(search.execute_command("FT.INFO", INDEX_NAME))
    types: dict[str, str] = {}
    paths: dict[str, str] = {}
    sortable_paths: set[str] = set()
    attributes = info.get("attributes", [])
    for raw_attribute in attributes:
        attribute = flat_pairs(raw_attribute)
        name = decode(attribute.get("attribute") or attribute.get("identifier"))
        if not name or name == "None":
            continue
        types[name] = decode(attribute.get("type", "")).upper()
        paths[name] = decode(attribute.get("identifier", name))
        raw_flags = attribute.get("flags", [])
        if isinstance(raw_flags, (str, bytes)):
            raw_flags = [raw_flags]
        flags = {decode(flag).upper() for flag in raw_flags}
        if not isinstance(raw_attribute, dict):
            flags.update(
                token
                for value in raw_attribute
                if (token := decode(value).upper()) in {"SORTABLE", "UNF", "NOINDEX"}
            )
        if "SORTABLE" in flags and "UNF" in flags and "NOINDEX" not in flags:
            sortable_paths.add(paths[name])
    return IndexSchema(
        fields=frozenset(types),
        types=types,
        paths=paths,
        sortable_paths=frozenset(sortable_paths),
    )


def clamp_inline_timeout(command: Command, floor_ms: int) -> tuple[Command, bool]:
    for index in range(len(command) - 1):
        if decode(command[index]).upper() != "TIMEOUT":
            continue
        try:
            current = int(command[index + 1])
        except (TypeError, ValueError):
            return command, False
        if current >= floor_ms:
            return command, False
        replacement: str | bytes = str(floor_ms)
        if isinstance(command[index + 1], bytes):
            replacement = replacement.encode()
        return command[: index + 1] + (replacement,) + command[index + 2 :], True
    return command, False


def replay_bucket(
    redis_module: Any,
    args: argparse.Namespace,
    name: str,
    commands: list[Command],
    stop_event: threading.Event,
    *,
    is_ft: bool = False,
    pipeline_batch: int = 1,
) -> dict[str, int]:
    client = (
        make_search_client(redis_module, args, REPLAY_SOCKET_TIMEOUT_SEC)
        if is_ft
        else make_cluster_client(redis_module, args, REPLAY_SOCKET_TIMEOUT_SEC)
    )
    started = time.perf_counter()
    ok = errors = timeouts = processed = clamped = 0
    error_counts: Counter[tuple[str, str]] = Counter()
    try:
        for offset in range(0, len(commands), pipeline_batch):
            if stop_event.is_set() or time.perf_counter() - started >= args.replay_duration:
                break
            batch = commands[offset : offset + pipeline_batch]
            if is_ft:
                adjusted: list[Command] = []
                for command in batch:
                    command, changed = clamp_inline_timeout(command, args.search_timeout_ms)
                    clamped += int(changed)
                    adjusted.append(command)
                batch = adjusted
            deadline = (
                started
                + args.replay_duration * (offset + len(batch)) / max(1, len(commands))
            )
            stop_event.wait(max(0.0, deadline - time.perf_counter()))
            if stop_event.is_set():
                break
            if pipeline_batch == 1:
                results: list[Any]
                try:
                    results = [client.execute_command(*batch[0])]
                except Exception as error:  # noqa: BLE001 - classified below
                    results = [error]
            else:
                pipeline = client.pipeline(transaction=False)
                for command in batch:
                    pipeline.execute_command(*command)
                try:
                    results = pipeline.execute(raise_on_error=False)
                except Exception as error:  # noqa: BLE001 - apply to the whole batch
                    results = [error] * len(batch)
            for command, result in zip(batch, results):
                processed += 1
                if not isinstance(result, BaseException):
                    ok += 1
                    continue
                errors += 1
                message = str(result)
                error_counts[(type(result).__name__, message[:160])] += 1
                if (
                    is_ft
                    and isinstance(result, redis_module.exceptions.ResponseError)
                    and _TIMEOUT_ERROR_RE.fullmatch(message.strip())
                    and not _COORDINATOR_ERROR_RE.search(message)
                ):
                    timeouts += 1
                    continue
                if is_ft and (
                    isinstance(result, redis_module.exceptions.TimeoutError)
                    or _COORDINATOR_ERROR_RE.search(message)
                ):
                    stop_event.set()
                    raise RuntimeError(
                        f"fatal FT replay error for {command!r}: {result}"
                    ) from result
            if processed and processed % 5_000 < pipeline_batch:
                LOGGER.info(
                    "[%s] processed=%d/%d ok=%d errors=%d",
                    name,
                    processed,
                    len(commands),
                    ok,
                    errors,
                )
    finally:
        client.close()
        LOGGER.info(
            "[%s] processed=%d/%d ok=%d errors=%d timeouts=%d clamped=%d wall=%.1fs",
            name,
            processed,
            len(commands),
            ok,
            errors,
            timeouts,
            clamped,
            time.perf_counter() - started,
        )
        for (error_name, message), count in error_counts.most_common(5):
            LOGGER.info("[%s] error: %d x %s: %s", name, count, error_name, message)
    return {"ok": ok, "err": errors, "timeout": timeouts, "processed": processed}


def run_validator(
    redis_module: Any,
    args: argparse.Namespace,
    baseline: FtInfoSample,
    totals: TraceTotals,
    stop_event: threading.Event,
) -> ValidatorReport:
    report = ValidatorReport()
    search = make_search_client(redis_module, args, PROBE_SOCKET_TIMEOUT_SEC)
    lower_bound = baseline.num_docs - totals.deletes - totals.expires - args.ttl_probe_count
    upper_bound = baseline.num_docs + totals.inserts
    try:
        while not stop_event.is_set():
            cycle_started = time.perf_counter()
            try:
                sample = capture_ft_info(search)
                report.info_samples.append(sample)
                if not lower_bound <= sample.num_docs <= upper_bound:
                    report.failures.append(
                        f"num_docs={sample.num_docs} outside [{lower_bound}, {upper_bound}]"
                    )
                if sample.hash_indexing_failures > baseline.hash_indexing_failures:
                    report.failures.append(
                        "hash_indexing_failures increased from "
                        f"{baseline.hash_indexing_failures} to {sample.hash_indexing_failures}"
                    )
            except Exception as error:  # noqa: BLE001 - validator records and continues
                report.failures.append(
                    f"FT.INFO probe raised {type(error).__name__}: {error}"
                )

            for probe_name, command in GOLDEN_PROBES:
                report.probe_samples += 1
                try:
                    search.execute_command(*command)
                except redis_module.exceptions.ResponseError as error:
                    message = str(error)
                    if _TIMEOUT_ERROR_RE.fullmatch(
                        message.strip()
                    ) and not _COORDINATOR_ERROR_RE.search(message):
                        continue
                    report.failures.append(f"probe {probe_name} response error: {message}")
                except Exception as error:  # noqa: BLE001 - validator records and continues
                    report.failures.append(
                        f"probe {probe_name} raised {type(error).__name__}: {error}"
                    )
            LOGGER.info(
                "validator: samples=%d probes=%d failures=%d",
                len(report.info_samples),
                report.probe_samples,
                len(report.failures),
            )
            stop_event.wait(max(0.0, VALIDATOR_CYCLE_SEC - (time.perf_counter() - cycle_started)))
    finally:
        search.close()
    return report


def run_replay_and_validator(
    redis_module: Any,
    args: argparse.Namespace,
    buckets: ReplayBuckets,
    totals: TraceTotals,
    baseline: FtInfoSample,
) -> tuple[
    ValidatorReport,
    BaseException | None,
    dict[str, int],
    dict[str, int],
    dict[str, int],
]:
    stop_event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        ft_future = executor.submit(
            replay_bucket,
            redis_module,
            args,
            "ft",
            buckets.ft,
            stop_event,
            is_ft=True,
        )
        writes_future = executor.submit(
            replay_bucket,
            redis_module,
            args,
            "writes",
            buckets.writes,
            stop_event,
        )
        reads_future = executor.submit(
            replay_bucket,
            redis_module,
            args,
            "reads",
            buckets.reads,
            stop_event,
            pipeline_batch=READ_PIPELINE_BATCH,
        )
        validator_future = executor.submit(
            run_validator,
            redis_module,
            args,
            baseline,
            totals,
            stop_event,
        )
        concurrent.futures.wait((ft_future, writes_future, reads_future))
        stop_event.set()
        ft_exception: BaseException | None = None
        try:
            ft_stats = ft_future.result()
        except BaseException as error:  # noqa: BLE001 - aggregated below
            ft_exception = error
            ft_stats = {"ok": 0, "err": 0, "timeout": 0, "processed": 0}
        write_stats = {"ok": 0, "err": 0, "timeout": 0, "processed": 0}
        read_stats = {"ok": 0, "err": 0, "timeout": 0, "processed": 0}
        for future_name, future in (("writes", writes_future), ("reads", reads_future)):
            try:
                stats = future.result()
                if future_name == "writes":
                    write_stats = stats
                else:
                    read_stats = stats
            except BaseException as error:  # noqa: BLE001 - non-FT replay is evidence
                LOGGER.warning("%s replay failed: %s: %s", future_name, type(error).__name__, error)
                if future_name == "writes":
                    write_stats["err"] = 1
                else:
                    read_stats["err"] = 1
        report = validator_future.result()
    return report, ft_exception, ft_stats, write_stats, read_stats


def assert_replay_and_validator(
    args: argparse.Namespace,
    buckets: ReplayBuckets,
    report: ValidatorReport,
    ft_exception: BaseException | None,
    ft_stats: dict[str, int],
    write_stats: dict[str, int],
    read_stats: dict[str, int],
) -> None:
    processed = ft_stats["processed"]
    timeout_rate = ft_stats["timeout"] / processed if processed else 0.0
    LOGGER.info(
        "Validator report: info_samples=%d probe_samples=%d failures=%d",
        len(report.info_samples),
        report.probe_samples,
        len(report.failures),
    )
    failures: list[str] = []
    if ft_exception is not None:
        failures.append(f"FT replay failed: {type(ft_exception).__name__}: {ft_exception}")
    replay_stats = (
        ("FT", len(buckets.ft), ft_stats),
        ("write", len(buckets.writes), write_stats),
        ("read", len(buckets.reads), read_stats),
    )
    for name, expected, stats in replay_stats:
        completion_ratio = stats["processed"] / expected if expected else 0.0
        if completion_ratio < args.min_replay_completion_ratio:
            failures.append(
                f"{name} replay completed {stats['processed']}/{expected} commands "
                f"({completion_ratio:.2%}), below {args.min_replay_completion_ratio:.2%}"
            )
    non_timeout_errors = ft_stats["err"] - ft_stats["timeout"]
    if non_timeout_errors:
        failures.append(
            f"FT replay had {non_timeout_errors} non-timeout errors in {processed} commands"
        )
    if write_stats["err"]:
        failures.append(
            f"write replay had {write_stats['err']} errors in "
            f"{write_stats['processed']} processed commands"
        )
    if read_stats["err"]:
        failures.append(
            f"read replay had {read_stats['err']} errors in "
            f"{read_stats['processed']} processed commands"
        )
    if timeout_rate > args.ft_timeout_rate_budget:
        failures.append(
            f"FT timeout rate {timeout_rate:.2%} ({ft_stats['timeout']}/{processed}) exceeds "
            f"{args.ft_timeout_rate_budget:.2%}"
        )
    if not report.info_samples:
        failures.append("validator captured no FT.INFO samples")
    if report.probe_samples < len(GOLDEN_PROBES):
        failures.append(
            f"validator ran only {report.probe_samples}/{len(GOLDEN_PROBES)} golden probes"
        )
    failures.extend(report.failures[:50])
    if failures:
        raise RuntimeError("LTK replay/validator failed:\n  " + "\n  ".join(failures))


def absorb_search_results(
    response: Any,
    identifiers: list[str],
    output: dict[bytes, set[str]],
) -> None:
    if isinstance(response, dict):
        for result in response.get("results") or response.get(b"results") or []:
            if not isinstance(result, dict):
                continue
            raw_key = result.get("id") or result.get(b"id")
            raw_fields = result.get("extra_attributes") or result.get(b"extra_attributes") or {}
            if raw_key is None:
                continue
            key = raw_key if isinstance(raw_key, bytes) else str(raw_key).encode("latin-1")
            fields = flat_pairs(raw_fields)
            output[key] = {
                name
                for name in identifiers
                if name in fields and fields[name] not in (None, b"", "")
            }
        return

    values = list(response)
    for index in range(1, len(values) - 1, 2):
        raw_key = values[index]
        raw_fields = values[index + 1]
        if not isinstance(raw_fields, (dict, list, tuple)):
            continue
        key = raw_key if isinstance(raw_key, bytes) else str(raw_key).encode("latin-1")
        fields = flat_pairs(raw_fields)
        output[key] = {
            name
            for name in identifiers
            if name in fields and fields[name] not in (None, b"", "")
        }


def walk_index_field_presence(
    search: Any,
    schema: IndexSchema,
    tracked: set[bytes],
) -> dict[bytes, set[str]]:
    # SORTABLE UNF values are served from Search's sorting vector. Other RETURN
    # fields may be reloaded from the live hash and cannot validate indexed state.
    identifiers = sorted(schema.sortable_paths)
    output: dict[bytes, set[str]] = {}
    tracked_keys = list(tracked)
    for start in range(0, len(tracked_keys), INDEX_LOOKUP_BATCH):
        keys = tracked_keys[start : start + INDEX_LOOKUP_BATCH]
        response = search.execute_command(
            "FT.SEARCH",
            INDEX_NAME,
            "*",
            "INKEYS",
            str(len(keys)),
            *keys,
            "RETURN",
            str(len(identifiers)),
            *identifiers,
            "LIMIT",
            "0",
            str(len(keys)),
            "TIMEOUT",
            str(FT_WALK_TIMEOUT_MS),
            "DIALECT",
            "2",
        )
        absorb_search_results(response, identifiers, output)
        LOGGER.info("Index lookup: matched=%d/%d", len(output), len(tracked))
    return output


def capture_redis_field_presence(
    cluster: Any,
    tracked_keys: list[bytes],
    identifiers: frozenset[str],
) -> tuple[dict[bytes, set[str]], set[bytes]]:
    output: dict[bytes, set[str]] = {}
    existing: set[bytes] = set()
    for start in range(0, len(tracked_keys), 200):
        keys = tracked_keys[start : start + 200]
        pipeline = cluster.pipeline(transaction=False)
        for key in keys:
            pipeline.hgetall(key)
        for key, raw_hash in zip(keys, pipeline.execute()):
            if raw_hash:
                existing.add(key)
            output[key] = {
                decode(raw_field)
                for raw_field, raw_value in (raw_hash or {}).items()
                if raw_value not in (None, b"", "") and decode(raw_field) in identifiers
            }
    return output, existing


def cross_check_tracked_keys(
    cluster: Any,
    search: Any,
    schema: IndexSchema,
    keys: list[bytes],
) -> None:
    identifiers = schema.sortable_paths
    if not identifiers:
        raise RuntimeError("index schema has no SORTABLE UNF fields to cross-check")
    actual = walk_index_field_presence(search, schema, set(keys))
    expected, existing = capture_redis_field_presence(cluster, keys, identifiers)
    failures: list[str] = []
    missing_fields: Counter[str] = Counter()
    phantom_fields: Counter[str] = Counter()
    for key, expected_fields in expected.items():
        actual_fields = actual.get(key)
        if key not in existing and actual_fields is not None:
            failures.append(f"{decode(key)} is absent from Redis but remains in the index")
            continue
        if actual_fields is None:
            if key in existing:
                failures.append(f"{decode(key)} missing from index walk")
            continue
        missing = expected_fields - actual_fields
        phantom = actual_fields - expected_fields
        missing_fields.update(missing)
        phantom_fields.update(phantom)
        if missing or phantom:
            failures.append(f"{decode(key)} missing={sorted(missing)} phantom={sorted(phantom)}")
    if missing_fields:
        LOGGER.error("Missing-field histogram: %s", missing_fields.most_common(15))
    if phantom_fields:
        LOGGER.error("Phantom-field histogram: %s", phantom_fields.most_common(15))
    if failures:
        raise RuntimeError(
            f"{len(failures)} tracked documents disagree between Redis and the index:\n  "
            + "\n  ".join(failures[:20])
        )
    LOGGER.info("Tracked-key cross-check passed for %d documents", len(keys))


def escape_tag_value(value: str) -> str:
    return re.sub(r"([^A-Za-z0-9_])", r"\\\1", value)


def search_result_keys(response: Any) -> set[bytes]:
    if isinstance(response, dict):
        results = response.get("results") or response.get(b"results") or []
        keys = []
        for result in results:
            if not isinstance(result, dict):
                continue
            raw_key = result.get("id") or result.get(b"id")
            if raw_key is not None:
                keys.append(raw_key)
    else:
        keys = list(response)[1:]
    return {
        key if isinstance(key, bytes) else str(key).encode("latin-1")
        for key in keys
    }


def find_indexed_ttl_keys(search: Any, key_set: TtlKeySet) -> set[bytes]:
    indexed: set[bytes] = set()
    for start in range(0, len(key_set.keys), INDEX_LOOKUP_BATCH):
        keys = key_set.keys[start : start + INDEX_LOOKUP_BATCH]
        ids = key_set.ids[start : start + INDEX_LOOKUP_BATCH]
        if not keys:
            continue
        query = "@product_id:{" + "|".join(escape_tag_value(value) for value in ids) + "}"
        response = search.execute_command(
            "FT.SEARCH",
            INDEX_NAME,
            query,
            "INKEYS",
            str(len(keys)),
            *keys,
            "NOCONTENT",
            "LIMIT",
            "0",
            str(len(keys)),
            "TIMEOUT",
            str(FT_WALK_TIMEOUT_MS),
            "DIALECT",
            "2",
        )
        indexed.update(search_result_keys(response))
    return indexed


def existing_keys(cluster: Any, keys: list[bytes]) -> set[bytes]:
    existing: set[bytes] = set()
    for start in range(0, len(keys), INDEX_LOOKUP_BATCH):
        batch = keys[start : start + INDEX_LOOKUP_BATCH]
        pipeline = cluster.pipeline(transaction=False)
        for key in batch:
            pipeline.exists(key)
        existing.update(key for key, result in zip(batch, pipeline.execute()) if result)
    return existing


def assert_ttl_set_present(
    cluster: Any,
    search: Any,
    key_set: TtlKeySet,
    label: str,
) -> None:
    present_in_redis = existing_keys(cluster, key_set.keys)
    present_in_index = find_indexed_ttl_keys(search, key_set)
    missing_redis = set(key_set.keys) - present_in_redis
    missing_index = set(key_set.keys) - present_in_index
    if missing_redis or missing_index:
        raise RuntimeError(
            f"{label} was not present before replay: "
            f"redis={len(missing_redis)} index={len(missing_index)}"
        )
    LOGGER.info("%s precondition passed for %d documents", label, len(key_set.keys))


def validate_ttl_sets(
    cluster: Any,
    search: Any,
    probes: TtlKeySet,
    survivors: TtlKeySet,
) -> None:
    deadline = max(time.time(), probes.expire_at_unix) + 60.0
    remaining_keys: set[bytes] = set(probes.keys)
    remaining_indexed: set[bytes] = set(probes.keys)
    while time.time() <= deadline:
        remaining_keys = existing_keys(cluster, probes.keys)
        remaining_indexed = find_indexed_ttl_keys(search, probes)
        if not remaining_keys and not remaining_indexed:
            break
        time.sleep(2.0)
    if remaining_keys or remaining_indexed:
        raise RuntimeError(
            "TTL probes did not expire cleanly: "
            f"redis={len(remaining_keys)} index={len(remaining_indexed)}"
        )

    survivor_keys = existing_keys(cluster, survivors.keys)
    survivor_indexed = find_indexed_ttl_keys(search, survivors)
    missing_redis = set(survivors.keys) - survivor_keys
    missing_index = set(survivors.keys) - survivor_indexed
    if missing_redis or missing_index:
        raise RuntimeError(
            "TTL survivors disappeared: "
            f"redis={len(missing_redis)} index={len(missing_index)}"
        )
    LOGGER.info(
        "TTL validation passed: expired=%d survivors=%d",
        len(probes.keys),
        len(survivors.keys),
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )
    try:
        import redis
    except ImportError:
        print(
            "error: redis-py is required; install it with 'python3 -m pip install redis'",
            file=sys.stderr,
        )
        return 2

    cluster = make_cluster_client(redis, args, REPLAY_SOCKET_TIMEOUT_SEC)
    search = make_search_client(redis, args, REPLAY_SOCKET_TIMEOUT_SEC)
    try:
        LOGGER.info("Step: Verify LTK Search configuration before warmup")
        verify_search_config_on_primaries(redis, args)

        LOGGER.info("Step: Load monitor trace")
        with obtain_monitor_trace(args) as trace_path:
            LOGGER.info("Step: Parse monitor trace")
            buckets, totals = partition_monitor_trace(trace_path)
        validate_trace_buckets(buckets)
        LOGGER.info(
            "Trace buckets: writes=%d ft=%d reads=%d inserts=%d deletes=%d expires=%d hdel=%d",
            len(buckets.writes),
            len(buckets.ft),
            len(buckets.reads),
            totals.inserts,
            totals.deletes,
            totals.expires,
            len(totals.hdel_keys),
        )
        LOGGER.info("Trace EXPIRE distribution: %s", totals.expire_ttl_summary())

        touched_keys = (
            totals.insert_keys | totals.delete_keys | totals.expire_keys | totals.hdel_keys
        )
        LOGGER.info("Step: Sample tracked keys for post-replay validation")
        stable_keys = reservoir_sample_tracked_keys(
            cluster,
            touched_keys,
            random.Random(args.rng_seed),
        )

        LOGGER.info("Step: Seed TTL probes")
        ttl_probes = seed_ttl_keys(
            cluster,
            label="TTL probe set",
            exclude_keys=touched_keys | set(stable_keys),
            sample_size=args.ttl_probe_count,
            expire_seconds=args.ttl_probe_seconds,
        )

        LOGGER.info("Step: Seed TTL survivor set")
        ttl_survivors = seed_ttl_keys(
            cluster,
            label="TTL survivor set",
            exclude_keys=touched_keys | set(stable_keys) | set(ttl_probes.keys),
            sample_size=args.ttl_survivor_count,
            expire_seconds=TTL_SURVIVOR_EXPIRE_SEC,
        )

        LOGGER.info("Step: Verify TTL sets before replay")
        assert_ttl_set_present(cluster, search, ttl_probes, "TTL probe set")
        assert_ttl_set_present(cluster, search, ttl_survivors, "TTL survivor set")

        baseline = capture_ft_info(search)
        baseline_schema = capture_index_schema(search)

        tracked_keys = list(dict.fromkeys([*stable_keys, *sorted(touched_keys)]))
        LOGGER.info(
            "Tracked keys selected: total=%d stable=%d replay_touched=%d",
            len(tracked_keys),
            len(stable_keys),
            len(touched_keys),
        )

        LOGGER.info("Running monitor replay and validator for %.0fs", args.replay_duration)
        report, ft_exception, ft_stats, write_stats, read_stats = run_replay_and_validator(
            redis,
            args,
            buckets,
            totals,
            baseline,
        )

        LOGGER.info("Step: Assert FT replay bucket and validator report")
        assert_replay_and_validator(
            args,
            buckets,
            report,
            ft_exception,
            ft_stats,
            write_stats,
            read_stats,
        )

        LOGGER.info("Step: Validate TTL probes and survivors")
        validate_ttl_sets(cluster, search, ttl_probes, ttl_survivors)

        LOGGER.info("Step: §4.6 tracked-key FT cross-check")
        cross_check_tracked_keys(cluster, search, baseline_schema, tracked_keys)
    except Exception as error:  # noqa: BLE001 - top-level diagnostic
        LOGGER.error("LTK scenario failed: %s: %s", type(error).__name__, error)
        return 1
    finally:
        search.close()
        cluster.close()

    LOGGER.info("LTK scenario completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
