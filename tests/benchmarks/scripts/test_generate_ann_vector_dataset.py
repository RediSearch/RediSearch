#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import csv
import sys
import tempfile
import unittest
from pathlib import Path

import h5py
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_ann_vector_dataset as generator


def read_binary_csv(path: Path) -> list[list[bytes]]:
    with path.open("r", encoding="latin-1", newline="") as source:
        return [[field.encode("latin-1") for field in row] for row in csv.reader(source)]


class GenerateAnnVectorDatasetTest(unittest.TestCase):
    def test_binary_csv_round_trip(self) -> None:
        fields = [b"READ", b'comma,quote"newline\r\nnull\x00high\xff']
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "binary.csv"
            with path.open("wb") as output:
                generator.write_csv_row(output, fields)
            self.assertEqual(read_binary_csv(path), [fields])

    def test_generates_real_setup_knn_and_per_query_range_rows(self) -> None:
        train = np.arange(24, dtype=np.float32).reshape(6, 4)
        test = np.arange(12, dtype=np.float32).reshape(3, 4) / 10
        distances = np.array(
            [[0.01, 0.02, 0.03], [0.11, 0.12, 0.13], [0.21, 0.22, 0.23]],
            dtype=np.float32,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_path = root / "source.hdf5"
            with h5py.File(source_path, "w") as dataset:
                dataset.create_dataset("train", data=train)
                dataset.create_dataset("test", data=test)
                dataset.create_dataset("distances", data=distances)

            setup_path = root / "setup.csv"
            knn_path = root / "knn.csv"
            range_path = root / "range.csv"
            with h5py.File(source_path, "r") as dataset:
                validated = generator.validate_dataset(dataset, 5, 2, 2)
                (
                    train_data,
                    test_data,
                    distance_data,
                    vector_count,
                    query_count,
                    dimension,
                ) = validated
                self.assertEqual((vector_count, query_count, dimension), (5, 2, 4))
                generator.write_setup(setup_path, train_data, vector_count, "smoke", 2)
                generator.write_knn_queries(knn_path, test_data, query_count)
                min_radius, max_radius = generator.write_range_queries(
                    range_path, test_data, distance_data, query_count, 2
                )

            setup_rows = read_binary_csv(setup_path)
            knn_rows = read_binary_csv(knn_path)
            range_rows = read_binary_csv(range_path)
            self.assertEqual(len(setup_rows), 5)
            self.assertEqual(len(knn_rows), 2)
            self.assertEqual(len(range_rows), 2)
            self.assertEqual(setup_rows[0][3:6], [b"HSET", b"smoke:0", b"vector"])
            self.assertEqual(setup_rows[0][6], train[0].astype("<f4").tobytes())
            self.assertEqual(
                knn_rows[0][3:6],
                [b"FT.SEARCH", b"idx", b"*=>[KNN 10 @vector $query AS vector_distance]"],
            )
            self.assertEqual(knn_rows[0][9], test[0].astype("<f4").tobytes())
            self.assertLess(min_radius, max_radius)
            self.assertEqual(
                range_rows[0][3:6],
                [b"FT.SEARCH", b"idx", b"@vector:[VECTOR_RANGE $radius $query]"],
            )
            self.assertNotEqual(range_rows[0][9], range_rows[1][9])


if __name__ == "__main__":
    unittest.main()
