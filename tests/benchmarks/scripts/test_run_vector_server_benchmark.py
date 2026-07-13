#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_vector_server_benchmark as benchmark


class RunVectorServerBenchmarkTest(unittest.TestCase):
    def test_pairs_to_dict_supports_resp2_and_resp3(self) -> None:
        expected = {"num_docs": 10, "indexing": 0}
        self.assertEqual(
            benchmark.pairs_to_dict([b"num_docs", 10, b"indexing", 0]), expected
        )
        self.assertEqual(
            benchmark.pairs_to_dict({b"num_docs": 10, b"indexing": 0}), expected
        )

    def test_search_result_count_supports_resp2_and_resp3(self) -> None:
        self.assertEqual(benchmark.search_result_count([10, b"doc:1"]), 10)
        self.assertEqual(benchmark.search_result_count({"total_results": 10}), 10)
        self.assertEqual(benchmark.search_result_count({b"total_results": 10}), 10)


if __name__ == "__main__":
    unittest.main()
