# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import importlib.util
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

SCRIPT = Path(__file__).with_name("run_ltk_scenario.py")
SPEC = importlib.util.spec_from_file_location("run_ltk_scenario", SCRIPT)
assert SPEC and SPEC.loader
scenario = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = scenario
SPEC.loader.exec_module(scenario)


class FakePipeline:
    def __init__(self, hashes: dict[bytes, dict[bytes, bytes]], existing: set[bytes]):
        self.hashes = hashes
        self.existing = existing
        self.operations: list[tuple[str, bytes]] = []

    def hgetall(self, key: bytes) -> None:
        self.operations.append(("hgetall", key))

    def exists(self, key: bytes) -> None:
        self.operations.append(("exists", key))

    def execute(self) -> list[object]:
        return [
            self.hashes.get(key, {}) if command == "hgetall" else int(key in self.existing)
            for command, key in self.operations
        ]


class FakeCluster:
    def __init__(
        self,
        *,
        hashes: dict[bytes, dict[bytes, bytes]] | None = None,
        existing: set[bytes] | None = None,
    ):
        self.hashes = hashes or {}
        self.existing = existing or set()

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        assert not transaction
        return FakePipeline(self.hashes, self.existing)


class FakeSearch:
    def __init__(self, indexed: dict[bytes, dict[bytes, object]]):
        self.indexed = indexed

    def execute_command(self, *command: object) -> list[object]:
        inkeys = command.index("INKEYS")
        count = int(command[inkeys + 1])
        keys = command[inkeys + 2 : inkeys + 2 + count]
        if "NOCONTENT" in command:
            matches = [key for key in keys if key in self.indexed]
            return [len(matches), *matches]
        identifiers = command[command.index("RETURN") + 2 : command.index("LIMIT")]
        response: list[object] = [0]
        for key in keys:
            if key not in self.indexed:
                continue
            fields = self.indexed[key]
            returned: list[object] = []
            for identifier in identifiers:
                encoded = str(identifier).encode()
                if encoded in fields:
                    returned.extend((encoded, fields[encoded]))
            response.extend((key, returned))
            response[0] += 1
        return response


class ScenarioTests(unittest.TestCase):
    def test_expected_search_config_matches_customer_profile(self) -> None:
        config = scenario.expected_search_config(SimpleNamespace(search_timeout_ms=5000))
        self.assertEqual(
            config,
            {
                "search-workers": "12",
                "search-timeout": "5000",
                "search-on-timeout": "fail",
                "search-cursor-max-idle": "60000",
                "search-workers-priority-bias-threshold": "6",
                "search-enable-unstable-features": "yes",
                "search-bm25std-tanh-factor": "200",
                "search-default-dialect": "2",
                "search-index-cursor-limit": "256",
            },
        )

    def test_s3_trace_uses_secure_temporary_path_and_expected_owner(self) -> None:
        calls: list[tuple[object, ...]] = []

        class S3Client:
            def download_file(self, *args: object, **kwargs: object) -> None:
                calls.append((*args, kwargs))
                Path(str(args[2])).write_text("trace")

        s3 = S3Client()
        boto3 = SimpleNamespace(client=lambda _service: s3)
        args = SimpleNamespace(
            monitor_trace=None,
            s3_bucket="bucket",
            s3_key="monitor.txt",
            s3_expected_bucket_owner="123456789012",
        )
        with (
            mock.patch.dict(sys.modules, {"boto3": boto3}),
            scenario.obtain_monitor_trace(args) as trace_path,
        ):
            self.assertTrue(trace_path.is_file())
            self.assertNotEqual(trace_path, Path("/tmp/ltk-monitor.txt"))
            temporary_directory = trace_path.parent

        self.assertFalse(temporary_directory.exists())
        self.assertEqual(
            calls,
            [
                (
                    "bucket",
                    "monitor.txt",
                    str(temporary_directory / "monitor.txt"),
                    {"ExtraArgs": {"ExpectedBucketOwner": "123456789012"}},
                )
            ],
        )

    def test_partition_splits_cross_slot_unlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trace = Path(directory) / "monitor.txt"
            trace.write_text('[ proxy ] 1.0 [0 client] "UNLINK" "key-a" "key-b"\n')
            buckets, totals = scenario.partition_monitor_trace(trace)
        self.assertEqual(buckets.writes, [("UNLINK", b"key-a"), ("UNLINK", b"key-b")])
        self.assertFalse(buckets.reads)
        self.assertEqual(totals.delete_keys, {b"key-a", b"key-b"})

    def test_trace_requires_all_workload_buckets(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "FT queries, reads"):
            scenario.validate_trace_buckets(
                scenario.ReplayBuckets(writes=[("HSET", b"key", b"field", b"value")])
            )

    def test_trace_rejects_incomplete_artifact(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "monitor trace is incomplete"):
            scenario.validate_trace_buckets(
                scenario.ReplayBuckets(
                    writes=[("HSET", b"key", b"field", b"value")],
                    ft=[("FT.SEARCH", "idx", "*")],
                    reads=[("HGETALL", b"key")],
                )
            )

    def test_timeout_clamp_skips_params_payload(self) -> None:
        command = (
            "FT.SEARCH",
            "idx",
            "@score:[$TIMEOUT +inf]",
            "PARAMS",
            "2",
            "TIMEOUT",
            "1",
            "TIMEOUT",
            "10",
            "DIALECT",
            "2",
        )
        adjusted, changed = scenario.clamp_inline_timeout(command, 2_000)
        self.assertTrue(changed)
        self.assertEqual(adjusted[5:7], ("TIMEOUT", "1"))
        self.assertEqual(adjusted[7:9], ("TIMEOUT", "2000"))

    def test_params_key_named_timeout_is_not_an_inline_timeout(self) -> None:
        command = (
            "FT.SEARCH",
            "idx",
            "@score:[$TIMEOUT +inf]",
            "PARAMS",
            "2",
            "TIMEOUT",
            "1",
            "DIALECT",
            "2",
        )
        self.assertEqual(scenario.clamp_inline_timeout(command, 2_000), (command, False))

    def test_non_timeout_ft_error_fails(self) -> None:
        args = SimpleNamespace(ft_timeout_rate_budget=1.0, min_replay_completion_ratio=1.0)
        buckets = scenario.ReplayBuckets(
            writes=[("SET", b"key", b"value")],
            ft=[("FT.SEARCH", "idx", "*")],
            reads=[("GET", b"key")],
        )
        report = scenario.ValidatorReport(
            info_samples=[scenario.FtInfoSample(1, 0, 0, 0)],
            probe_samples=len(scenario.GOLDEN_PROBES),
        )
        with self.assertRaisesRegex(RuntimeError, "non-timeout errors"):
            scenario.assert_replay_and_validator(
                args,
                buckets,
                report,
                None,
                {"ok": 0, "err": 1, "timeout": 0, "processed": 1},
                {"ok": 1, "err": 0, "timeout": 0, "processed": 1},
                {"ok": 1, "err": 0, "timeout": 0, "processed": 1},
            )

    def test_read_error_fails(self) -> None:
        args = SimpleNamespace(ft_timeout_rate_budget=1.0, min_replay_completion_ratio=1.0)
        buckets = scenario.ReplayBuckets(
            writes=[("SET", b"key", b"value")],
            ft=[("FT.SEARCH", "idx", "*")],
            reads=[("GET", b"key")],
        )
        report = scenario.ValidatorReport(
            info_samples=[scenario.FtInfoSample(1, 0, 0, 0)],
            probe_samples=len(scenario.GOLDEN_PROBES),
        )
        with self.assertRaisesRegex(RuntimeError, "read replay had 1 errors"):
            scenario.assert_replay_and_validator(
                args,
                buckets,
                report,
                None,
                {"ok": 1, "err": 0, "timeout": 0, "processed": 1},
                {"ok": 1, "err": 0, "timeout": 0, "processed": 1},
                {"ok": 0, "err": 1, "timeout": 0, "processed": 1},
            )

    def test_incomplete_replay_fails(self) -> None:
        args = SimpleNamespace(ft_timeout_rate_budget=1.0, min_replay_completion_ratio=0.99)
        buckets = scenario.ReplayBuckets(
            writes=[("SET", b"key", b"value")],
            ft=[("FT.SEARCH", "idx", "*")] * 100,
            reads=[("GET", b"key")],
        )
        report = scenario.ValidatorReport(
            info_samples=[scenario.FtInfoSample(1, 0, 0, 0)],
            probe_samples=len(scenario.GOLDEN_PROBES),
        )
        with self.assertRaisesRegex(RuntimeError, "FT replay completed 98/100"):
            scenario.assert_replay_and_validator(
                args,
                buckets,
                report,
                None,
                {"ok": 98, "err": 0, "timeout": 0, "processed": 98},
                {"ok": 1, "err": 0, "timeout": 0, "processed": 1},
                {"ok": 1, "err": 0, "timeout": 0, "processed": 1},
            )

    def test_only_canonical_execution_timeout_is_allowed(self) -> None:
        self.assertIsNotNone(scenario._TIMEOUT_ERROR_RE.fullmatch("Timeout limit was reached"))
        self.assertIsNotNone(
            scenario._TIMEOUT_ERROR_RE.fullmatch(
                "SEARCH_TIMEOUT Timeout limit was reached"
            )
        )
        self.assertIsNone(
            scenario._TIMEOUT_ERROR_RE.fullmatch("Syntax error near TIMEOUT argument")
        )

    def test_nonpositive_ttl_probe_duration_is_rejected(self) -> None:
        argv = [str(SCRIPT), "--ttl-probe-seconds", "0", "127.0.0.1"]
        with (
            mock.patch.object(sys, "argv", argv),
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as error,
        ):
            scenario.parse_args()
        self.assertEqual(error.exception.code, 2)

    def test_final_ft_info_sample_detects_late_indexing_failure(self) -> None:
        baseline = scenario.FtInfoSample(100, 0, 0, 2)
        report = scenario.ValidatorReport()
        scenario.validate_ft_info_sample(
            report,
            scenario.FtInfoSample(100, 0, 0, 3),
            baseline,
            scenario.TraceTotals(),
            0,
        )
        self.assertEqual(len(report.info_samples), 1)
        self.assertIn("hash_indexing_failures increased from 2 to 3", report.failures)

    def test_replay_takes_final_ft_info_after_all_workers_finish(self) -> None:
        completed: set[str] = set()

        def fake_replay(
            _redis_module: object,
            _args: object,
            name: str,
            _commands: object,
            _stop_event: object,
            **_kwargs: object,
        ) -> dict[str, int]:
            completed.add(name)
            return {"ok": 1, "err": 0, "timeout": 0, "processed": 1}

        def fake_capture(_search: object) -> scenario.FtInfoSample:
            self.assertEqual(completed, {"ft", "writes", "reads"})
            return scenario.FtInfoSample(100, 0, 0, 0)

        search = SimpleNamespace(close=lambda: None)
        with (
            mock.patch.object(scenario, "replay_bucket", side_effect=fake_replay),
            mock.patch.object(
                scenario,
                "run_validator",
                return_value=scenario.ValidatorReport(),
            ),
            mock.patch.object(scenario, "make_search_client", return_value=search),
            mock.patch.object(scenario, "capture_ft_info", side_effect=fake_capture),
        ):
            report, ft_error, *_stats = scenario.run_replay_and_validator(
                object(),
                SimpleNamespace(ttl_probe_count=0),
                scenario.ReplayBuckets(
                    writes=[("HSET", b"key", b"field", b"value")],
                    ft=[("FT.SEARCH", "idx", "*")],
                    reads=[("HGETALL", b"key")],
                ),
                scenario.TraceTotals(),
                scenario.FtInfoSample(100, 0, 0, 0),
            )

        self.assertIsNone(ft_error)
        self.assertEqual(report.info_samples, [scenario.FtInfoSample(100, 0, 0, 0)])

    def test_zero_sized_ttl_set_does_not_scan_or_mutate(self) -> None:
        key_set = scenario.seed_ttl_keys(
            object(),
            label="empty set",
            exclude_keys=set(),
            sample_size=0,
            expire_seconds=60,
        )
        self.assertEqual(key_set.keys, [])
        self.assertEqual(key_set.ids, [])

    def test_schema_keeps_only_sortable_unf_indexed_paths(self) -> None:
        class InfoClient:
            def execute_command(self, *command: object) -> dict[str, object]:
                return {
                    "attributes": [
                        {
                            "identifier": "score",
                            "attribute": "score",
                            "type": "NUMERIC",
                            "flags": ["SORTABLE", "UNF"],
                        },
                        {
                            "identifier": "raw",
                            "attribute": "raw",
                            "type": "TEXT",
                            "flags": [],
                        },
                        {
                            "identifier": "hidden",
                            "attribute": "hidden",
                            "type": "TAG",
                            "flags": ["SORTABLE", "UNF", "NOINDEX"],
                        },
                    ]
                }

        schema = scenario.capture_index_schema(InfoClient())
        self.assertEqual(schema.sortable_paths, frozenset({"score"}))

    def test_schema_reads_resp2_trailing_flags(self) -> None:
        class InfoClient:
            def execute_command(self, *command: object) -> list[object]:
                return [
                    "attributes",
                    [
                        [
                            "identifier",
                            "score",
                            "attribute",
                            "score",
                            "type",
                            "NUMERIC",
                            "SORTABLE",
                            "UNF",
                        ],
                        [
                            "identifier",
                            "hidden",
                            "attribute",
                            "hidden",
                            "type",
                            "TAG",
                            "SORTABLE",
                            "UNF",
                            "NOINDEX",
                        ],
                    ],
                ]

        schema = scenario.capture_index_schema(InfoClient())
        self.assertEqual(schema.sortable_paths, frozenset({"score"}))

    def test_sortable_values_accept_equivalent_numeric_representations(self) -> None:
        key = b"A#PR#1"
        schema = scenario.IndexSchema(
            fields=frozenset({"score"}),
            types={"score": "NUMERIC"},
            paths={"score": "score"},
            sortable_paths=frozenset({"score"}),
        )
        cluster = FakeCluster(hashes={key: {b"score": b"10.0"}}, existing={key})
        search = FakeSearch({key: {b"score": 10}})
        scenario.cross_check_tracked_keys(cluster, search, schema, [key])

    def test_stale_sortable_value_fails_cross_check(self) -> None:
        key = b"A#PR#1"
        schema = scenario.IndexSchema(
            fields=frozenset({"score"}),
            types={"score": "NUMERIC"},
            paths={"score": "score"},
            sortable_paths=frozenset({"score"}),
        )
        with self.assertRaisesRegex(RuntimeError, "value_mismatches=\\['score'\\]"):
            scenario.cross_check_tracked_keys(
                FakeCluster(hashes={key: {b"score": b"20"}}, existing={key}),
                FakeSearch({key: {b"score": 10}}),
                schema,
                [key],
            )

    def test_indexed_document_absent_from_redis_fails_cross_check(self) -> None:
        key = b"A#PR#1"
        schema = scenario.IndexSchema(
            fields=frozenset({"score"}),
            types={"score": "NUMERIC"},
            paths={"score": "score"},
            sortable_paths=frozenset({"score"}),
        )
        with self.assertRaisesRegex(RuntimeError, "absent from Redis"):
            scenario.cross_check_tracked_keys(
                FakeCluster(),
                FakeSearch({key: {b"score": 1}}),
                schema,
                [key],
            )

    def test_hash_with_only_non_sortable_fields_is_not_treated_as_absent(self) -> None:
        key = b"A#PR#1"
        schema = scenario.IndexSchema(
            fields=frozenset({"score", "full_name"}),
            types={"score": "NUMERIC", "full_name": "TEXT"},
            paths={"score": "score", "full_name": "full_name"},
            sortable_paths=frozenset({"score"}),
        )
        scenario.cross_check_tracked_keys(
            FakeCluster(hashes={key: {b"full_name": b"Redis"}}, existing={key}),
            FakeSearch({key: {}}),
            schema,
            [key],
        )

    def test_ttl_set_must_be_indexed_before_replay(self) -> None:
        key = b"A#PR#probe"
        probes = scenario.TtlKeySet([key], ["probe"], 1_000_000)
        with self.assertRaisesRegex(RuntimeError, "was not present before replay"):
            scenario.assert_ttl_set_present(
                FakeCluster(existing={key}),
                FakeSearch({}),
                probes,
                "TTL probe set",
            )
        scenario.assert_ttl_set_present(
            FakeCluster(existing={key}),
            FakeSearch({key: {}}),
            probes,
            "TTL probe set",
        )

    def test_ttl_survivors_must_exist_in_redis_and_index(self) -> None:
        survivor = b"A#PR#survivor"
        probes = scenario.TtlKeySet([], [], 0)
        survivors = scenario.TtlKeySet([survivor], ["survivor"], 1_000_000)
        scenario.validate_ttl_sets(
            FakeCluster(existing={survivor}),
            FakeSearch({survivor: {}}),
            probes,
            survivors,
        )
        with self.assertRaisesRegex(RuntimeError, "TTL survivors disappeared"):
            scenario.validate_ttl_sets(
                FakeCluster(),
                FakeSearch({survivor: {}}),
                probes,
                survivors,
            )


if __name__ == "__main__":
    unittest.main()
