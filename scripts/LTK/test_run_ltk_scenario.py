# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

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

    def test_non_timeout_ft_error_fails(self) -> None:
        args = SimpleNamespace(ft_timeout_rate_budget=1.0)
        report = scenario.ValidatorReport(
            info_samples=[scenario.FtInfoSample(1, 0, 0, 0)],
            probe_samples=len(scenario.GOLDEN_PROBES),
        )
        with self.assertRaisesRegex(RuntimeError, "non-timeout errors"):
            scenario.assert_replay_and_validator(
                args,
                report,
                None,
                {"ok": 0, "err": 1, "timeout": 0, "processed": 1},
                {"ok": 1, "err": 0, "timeout": 0, "processed": 1},
            )

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

    def test_sortable_presence_uses_index_result_and_accepts_numeric_zero(self) -> None:
        key = b"A#PR#1"
        schema = scenario.IndexSchema(
            fields=frozenset({"score"}),
            types={"score": "NUMERIC"},
            paths={"score": "score"},
            sortable_paths=frozenset({"score"}),
        )
        cluster = FakeCluster(hashes={key: {b"score": b"0"}}, existing={key})
        search = FakeSearch({key: {b"score": 0}})
        scenario.cross_check_tracked_keys(cluster, search, schema, [key])

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
