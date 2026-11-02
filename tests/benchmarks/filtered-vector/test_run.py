# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np
import redis

import run


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.args = run.parser().parse_args([
            "--bundle", "/fixture", "--output", "/results", "--run-label", "test",
            "--source-revision", "test-only",
        ])
        self.bundle = SimpleNamespace(
            manifest={"dimensions": 2, "metric": "l2"},
            filters={"all": [0, 1], "none": []},
            queries=np.array([[1, 2]], dtype=np.float32),
        )
        self.case = {"case_id": "q0", "query_id": 0, "filter_id": "all", "k": 10}

    def test_auto_omits_policy_but_keeps_real_filter(self):
        command = run.query_command(self.args, self.bundle, self.case, "AUTO")
        self.assertIn("(@filter:{f0})", command[2])
        self.assertNotIn("HYBRID_POLICY", command[2])
        self.assertIn("EF_RUNTIME 100", command[2])
        self.assertEqual(command[6], self.bundle.queries[0].tobytes())

    def test_adhoc_does_not_receive_incompatible_ef_runtime(self):
        command = run.query_command(self.args, self.bundle, self.case, "ADHOC_BF")
        self.assertIn("HYBRID_POLICY ADHOC_BF", command[2])
        self.assertNotIn("EF_RUNTIME", command[2])

    def test_empty_filter_remains_a_filter(self):
        self.case["filter_id"] = "none"
        command = run.query_command(self.args, self.bundle, self.case, "BATCHES")
        self.assertIn("(@filter:{f1})", command[2])

    def test_parse_valid_and_empty_replies(self):
        self.assertEqual(run.parse_reply([0], "doc:"), ([], []))
        reply = [2, b"doc:7", b"#0.5", b"doc:8", b"#2"]
        self.assertEqual(run.parse_reply(reply, "doc:"), ([7, 8], [0.5, 2.0]))

    def test_reject_malformed_and_nonfinite_replies(self):
        for reply in ([1, b"else:7", b"#0"], [1, b"doc:7"],
                      [1, b"doc:7", b"#nan"], [1, b"doc:7", b"$0.5"]):
            with self.subTest(reply=reply), self.assertRaises(ValueError):
                run.parse_reply(reply, "doc:")

    def test_query_uses_flex_compatible_distance_reply(self):
        command = run.query_command(self.args, self.bundle, self.case, "BATCHES")
        self.assertIn("NOCONTENT", command)
        self.assertIn("WITHSORTKEYS", command)
        self.assertNotIn("RETURN", command)

    def test_disk_schema_has_explicit_parameters(self):
        self.args.rerank = "true"
        command = run.create_command(self.args, self.bundle)
        position = command.index("HNSW")
        self.assertEqual(command[position + 1], 14)
        self.assertEqual(command[-2:], ["RERANK", "TRUE"])

    def test_rerank_applies_to_queries_on_reused_indexes(self):
        self.args.rerank = "false"
        for policy in run.POLICIES:
            command = run.query_command(self.args, self.bundle, self.case, policy)
            self.assertIn("RERANK FALSE", command[2])

    def test_tiered_backend_mode_is_verified(self):
        info = {"BACKEND_INDEX": [b"IS_DISK", 1]}
        run.verify_engine(info, "disk")
        with self.assertRaises(ValueError):
            run.verify_engine(info, "ram")
        with self.assertRaises(ValueError):
            run.verify_engine({}, "disk")

    def test_partial_timeout_configuration_is_rejected(self):
        class Client:
            def config_get(self, name):
                return {b"search-on-timeout": b"return"}

        with self.assertRaisesRegex(ValueError, "partial timeout"):
            run.require_fail_on_timeout(Client())

    def test_reuse_refuses_different_bundle_without_querying(self):
        class Client:
            def get(self, key):
                return b'{"bundle_id":"different"}'

        self.args.metadata_key = "manifest"
        self.bundle.bundle_id = "expected"
        with self.assertRaisesRegex(ValueError, "bundle_id"):
            run.reuse(Client(), self.args, self.bundle)

    def test_transport_failure_is_not_retried(self):
        client = run.connection(self.args)
        retry = client.connection_pool.connection_kwargs["retry"]
        attempts = []

        def fail():
            attempts.append(1)
            raise redis.TimeoutError("injected timeout")

        with self.assertRaises(redis.TimeoutError):
            retry.call_with_retry(fail, lambda error: None)
        self.assertEqual(len(attempts), 1)
        client.close()

    def test_readiness_waits_for_background_indexing(self):
        class Client:
            polls = 0

            def execute_command(self, *command):
                if command[0] == "FT.INFO":
                    return [b"num_docs", 2, b"indexing", 0]
                if command[1] == "WORKERS":
                    return [b"totalPendingJobs", 0, b"numJobsInProgress", 0]
                self.polls += 1
                return [b"INDEX_SIZE", 2, b"BACKGROUND_INDEXING", int(self.polls == 1)]

        client = Client()
        with patch.object(run.time, "sleep"):
            run.wait_ready(client, self.args, 2)
        self.assertEqual(client.polls, 2)

    def test_disk_readiness_waits_for_pending_and_running_repairs(self):
        class Client:
            polls = 0

            def execute_command(self, *command):
                if command[0] == "FT.INFO":
                    return [b"num_docs", 2, b"indexing", 0]
                if command[1] == "WORKERS":
                    self.polls += 1
                    return [b"totalPendingJobs", int(self.polls == 1),
                            b"numJobsInProgress", int(self.polls == 2)]
                return [b"IS_DISK", 1, b"BACKGROUND_INDEXING", -1,
                        b"FRONTEND_INDEX", [b"INDEX_SIZE", 0],
                        b"BACKEND_INDEX", [b"IS_DISK", 1, b"INDEX_SIZE", 2]]

        self.args.engine = "disk"
        client = Client()
        with patch.object(run.time, "sleep"):
            run.wait_ready(client, self.args, 2)
        self.assertEqual(client.polls, 3)

    def test_disk_unset_indexing_flag_accepts_idle_workers(self):
        class Client:
            def execute_command(self, *command):
                if command[0] == "FT.INFO":
                    return [b"num_docs", 2, b"indexing", 0]
                if command[1] == "WORKERS":
                    return [b"totalPendingJobs", 0, b"numJobsInProgress", 0]
                return [b"IS_DISK", 1, b"INDEX_SIZE", 2, b"BACKGROUND_INDEXING", -1]

        self.args.engine = "disk"
        with patch.object(run.time, "sleep") as sleep:
            run.wait_ready(Client(), self.args, 2)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
