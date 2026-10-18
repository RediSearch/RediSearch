#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "summarize_alter_vectors", Path(__file__).with_name("summarize_alter_vectors.py")
)
SUMMARY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARY)


def trial(label, repetition, elapsed, rate=0):
    return {
        "label": label, "repetition": repetition, "scan_observed_seconds": elapsed,
        "writer_target_ops_per_second": rate, "writer_completed": rate * elapsed,
        "writer_effective_ops_per_second": rate, "writer_missed_slots": 0,
        "writer_first_completion_seconds": 0.001 if rate else None,
        "module_sha256": ("b" if label == "after" else "a") * 64,
        "scenario": "add-vector", "documents": 100, "dim": 4,
        "preexisting_vector_fields": 1, "seed": 42, "workers": 0,
        "optimize_partial_update": "true", "poll_interval_seconds": 0.01,
        "redis_version": "8.9.0", "redis_git_sha1": "12345678", "hostname": "bench",
        "modules": [{"name": "search", "ver": 999999, "path": f"/{label}.so",
                     "args": ["WORKERS", "0", "OPTIMIZE_PARTIAL_UPDATE", "true"]}],
        "counter_deltas": dict(zip(SUMMARY.COUNTERS, (100, 100))),
    }


class SummaryTest(unittest.TestCase):
    def setUp(self):
        self.comparison = [trial(label, rep, elapsed)
                           for label, times in (("before", (10, 20, 100)), ("after", (20, 10, 90)))
                           for rep, elapsed in enumerate(times)]
        self.control = [trial(label, rep, elapsed)
                        for label, elapsed in (("control_A", 20), ("control_B", 22))
                        for rep in range(3)]

    def report(self, **kwargs):
        with tempfile.TemporaryDirectory() as directory:
            main = Path(directory) / "comparison.jsonl"
            control = Path(directory) / "control.jsonl"
            main.write_text("".join(json.dumps(row) + "\n" for row in self.comparison))
            control.write_text("".join(json.dumps(row) + "\n" for row in self.control))
            return SUMMARY.summarize(main, control, **kwargs)

    def test_ratio_of_medians_and_individual_pairs(self):
        report = self.report(expected_repetitions=3, expected_writer_rates=[0])
        # Ratios of medians give zero; the median paired change would be -10%.
        self.assertIn("| 0 | 3 | 20.000000 | 20.000000 | +0.00% | 20.000000 | 22.000000 | +10.00% |", report)
        self.assertIn("| 0 | 0 | +100.00% | +10.00% |", report)
        self.assertIn("| 0 | 1 | -50.00% | +10.00% |", report)
        self.assertIn("| 0 | 2 | -10.00% | +10.00% |", report)
        self.assertIn("cannot establish a universal benefit or regression", report)

    def test_writer_rate_has_no_control_extrapolation(self):
        self.comparison += [trial(label, rep, 10, 10)
                            for label in ("before", "after") for rep in range(3)]
        report = self.report(expected_repetitions=3, expected_writer_rates=[0, 10])
        self.assertIn("| 10 | 3 | 10.000000 | 10.000000 | +0.00% | — | — | — |", report)
        self.assertIn("| 10 | 0 | before | 100 | 100 | 100 | 10.00 | 0 |", report)

    def test_missing_pair(self):
        self.comparison.pop()
        with self.assertRaisesRegex(ValueError, "incomplete pair"):
            self.report()

    def test_missing_repetition_in_all_series(self):
        self.comparison = [row for row in self.comparison if row["repetition"] != 1]
        with self.assertRaisesRegex(ValueError, "contiguous"):
            self.report()

    def test_expected_series_detects_uniform_truncation(self):
        for rows in (self.comparison, self.control):
            rows[:] = [row for row in rows if row["repetition"] < 2]
        with self.assertRaisesRegex(ValueError, "repetition count differs"):
            self.report(expected_repetitions=3)
        with self.assertRaisesRegex(ValueError, "writer rates differ"):
            self.report(expected_writer_rates=[0, 10])

    def test_duplicate_trial(self):
        self.comparison.append(copy.deepcopy(self.comparison[0]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.report()

    def test_mixed_config_or_runtime(self):
        for field, value in (("dim", 128), ("redis_git_sha1", "87654321"), ("hostname", "other")):
            with self.subTest(field=field):
                original = self.comparison[0][field]
                self.comparison[0][field] = value
                with self.assertRaisesRegex(ValueError, "mixed benchmark configuration/runtime"):
                    self.report()
                self.comparison[0][field] = original

    def test_runtime_module_arguments_must_match(self):
        self.control[0]["modules"][0]["args"] = ["WORKERS", "1"]
        with self.assertRaisesRegex(ValueError, "mixed benchmark configuration/runtime"):
            self.report()

    def test_main_and_control_runtime_must_match(self):
        for row in self.control:
            row["redis_version"] = "other"
        with self.assertRaisesRegex(ValueError, "main/control benchmark configuration/runtime differ"):
            self.report()

    def test_module_identity(self):
        self.comparison[0]["module_sha256"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "mixed module hashes"):
            self.report()
        self.comparison[0]["module_sha256"] = "a" * 64
        for row in self.control:
            row["module_sha256"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "exact same module hash as before"):
            self.report()

    def test_control_must_be_identical_binary(self):
        for row in self.control:
            if row["label"] == "control_B":
                row["module_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "exact same module hash as before"):
            self.report()

    def test_invalid_measurements_and_missing_fields(self):
        cases = (("scan_observed_seconds", 0), ("scan_observed_seconds", float("nan")),
                 ("writer_effective_ops_per_second", 3), ("writer_missed_slots", -1),
                 ("repetition", True), ("module_sha256", "unknown"), ("counter_deltas", {}))
        for field, value in cases:
            with self.subTest(field=field, value=value):
                original = self.comparison[0][field]
                self.comparison[0][field] = value
                with self.assertRaises(ValueError):
                    self.report()
                self.comparison[0][field] = original
        del self.comparison[0]["hostname"]
        with self.assertRaisesRegex(ValueError, "missing configuration/runtime field hostname"):
            self.report()

    def test_empty_or_malformed_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.jsonl"
            for text in ("", "{}\n", "not JSON\n", "[]\n"):
                with self.subTest(text=text):
                    path.write_text(text)
                    with self.assertRaisesRegex(ValueError, str(path)):
                        SUMMARY.read_trials(path, ("before", "after"))


if __name__ == "__main__":
    unittest.main()
