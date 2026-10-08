# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import coverage_summary  # noqa: E402


class CoverageSummaryTests(unittest.TestCase):
    @patch("coverage_summary.subprocess.run")
    def test_lcov_branch_counts(self, run: unittest.mock.Mock) -> None:
        run.return_value = subprocess.CompletedProcess(
            args=["lcov"],
            returncode=0,
            stdout="  branches...: 62.5% (5 of 8 branches)\n",
        )

        self.assertEqual(
            coverage_summary.lcov_branch_counts(Path("coverage.info")), (5, 8)
        )

    def test_cobertura_branch_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory, "coverage.xml")
            report.write_text(
                '<coverage branches-covered="7" branches-valid="10"/>',
                encoding="utf-8",
            )

            self.assertEqual(coverage_summary.cobertura_branch_counts(report), (7, 10))

    def test_missing_cobertura_branch_data_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory, "coverage.xml")
            report.write_text("<coverage/>", encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError, "does not contain Cobertura branch totals"
            ):
                coverage_summary.cobertura_branch_counts(report)

    @patch("coverage_summary.subprocess.run")
    def test_missing_lcov_branch_data_is_rejected(
        self, run: unittest.mock.Mock
    ) -> None:
        run.return_value = subprocess.CompletedProcess(
            args=["lcov"], returncode=0, stdout="lines...: 100.0% (3 of 3 lines)\n"
        )

        with self.assertRaisesRegex(ValueError, "does not contain LCOV branch data"):
            coverage_summary.lcov_branch_counts(Path("coverage.info"))

    def test_invalid_branch_totals_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "reports no branches"):
            coverage_summary.validate_branch_counts(Path("coverage.xml"), 0, 0)

        with self.assertRaisesRegex(ValueError, "reports invalid branch counts"):
            coverage_summary.validate_branch_counts(Path("coverage.xml"), 6, 5)

    def test_render_summary(self) -> None:
        self.assertIn(
            "| C unit | 3 | 4 | 75.00% |",
            coverage_summary.render_summary([("C unit", 3, 4)]),
        )


if __name__ == "__main__":
    unittest.main()
