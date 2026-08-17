#!/usr/bin/env python3
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("generate_dropindex_writer_contention_dataset.py")


def load_generator_module():
    if not SCRIPT.exists():
        raise AssertionError(f"generator script does not exist: {SCRIPT}")

    spec = importlib.util.spec_from_file_location("generate_dropindex_writer_contention_dataset", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_rows(path):
    with path.open(newline="", encoding="utf-8") as infile:
        return list(csv.reader(infile))


class GenerateDropIndexWriterContentionDatasetTest(unittest.TestCase):
    def test_setup_file_creates_indexes_before_loading_documents(self):
        generator = load_generator_module()

        with tempfile.TemporaryDirectory() as tmp:
            setup_file = Path(tmp) / "setup.csv"
            generator.write_setup_file(
                setup_file,
                index_count=2,
                docs_per_index=3,
                payload_bytes=12,
            )

            rows = read_rows(setup_file)

        self.assertEqual(8, len(rows))
        self.assertEqual(
            [
                "WRITE",
                "create-index",
                "1",
                "FT.CREATE",
                "idx:1",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:1:",
                "SCHEMA",
                "body",
                "TEXT",
            ],
            rows[0],
        )
        self.assertEqual("FT.CREATE", rows[1][3])
        self.assertEqual(["WRITE", "load-doc", "1", "HSET", "doc:1:1", "body"], rows[2][:6])
        self.assertEqual(12, len(rows[2][6]))
        self.assertEqual(["WRITE", "load-doc", "1", "HSET", "doc:2:3", "body"], rows[-1][:6])

    def test_benchmark_file_interleaves_each_drop_with_bounded_writer_batch(self):
        generator = load_generator_module()

        with tempfile.TemporaryDirectory() as tmp:
            bench_file = Path(tmp) / "bench.csv"
            generator.write_benchmark_file(
                bench_file,
                index_count=2,
                writes_per_drop=2,
            )

            rows = read_rows(bench_file)

        self.assertEqual(
            [
                ["WRITE", "drop-index", "1", "FT._DROPINDEXIFX", "idx:1", "_FORCEKEEPDOCS"],
                ["WRITE", "foreground-write", "1", "INCR", "writer:1:1"],
                ["WRITE", "foreground-write", "1", "INCR", "writer:1:2"],
                ["WRITE", "drop-index", "1", "FT._DROPINDEXIFX", "idx:2", "_FORCEKEEPDOCS"],
                ["WRITE", "foreground-write", "1", "INCR", "writer:2:1"],
                ["WRITE", "foreground-write", "1", "INCR", "writer:2:2"],
            ],
            rows,
        )

    def test_generate_files_uses_expected_benchmark_names(self):
        generator = load_generator_module()

        with tempfile.TemporaryDirectory() as tmp:
            setup_file, benchmark_file = generator.generate_files(
                Path(tmp),
                dataset_name="tiny-dropindex-gil",
                index_count=1,
                docs_per_index=1,
                writes_per_drop=1,
                payload_bytes=8,
            )

            self.assertTrue(setup_file.exists())
            self.assertTrue(benchmark_file.exists())
            self.assertEqual(
                "tiny-dropindex-gil.redisearch.commands.SETUP.csv",
                setup_file.name,
            )
            self.assertEqual(
                "tiny-dropindex-gil.redisearch.commands.BENCH.csv",
                benchmark_file.name,
            )


if __name__ == "__main__":
    unittest.main()
