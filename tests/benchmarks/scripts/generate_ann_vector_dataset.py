#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Generate server-level vector benchmark inputs from an ANN-Benchmarks dataset."""

import argparse
import hashlib
import json
import math
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterable

import h5py
import numpy as np


DEFAULT_SOURCE = "https://ann-benchmarks.com/glove-100-angular.hdf5"
DEFAULT_DATASET_NAME = "glove-100-angular-1183514"
UNSAFE_LINE_BYTES = (b"\n"[0], b"\r"[0])


@dataclass
class SanitizationStats:
    adjusted_bytes: int = 0
    adjusted_vectors: int = 0
    max_absolute_delta: float = 0.0
    max_relative_delta: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert ANN-Benchmarks train/test vectors into binary-safe FTSB CSV files "
            "for RediSearch KNN and range-query macro benchmarks."
        )
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="ANN-Benchmarks HDF5 path or URL (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory in which generated benchmark inputs are written",
    )
    parser.add_argument(
        "--dataset-name",
        default=DEFAULT_DATASET_NAME,
        help="Prefix used for generated files and Redis keys (default: %(default)s)",
    )
    parser.add_argument(
        "--max-vectors",
        type=int,
        default=None,
        help="Optional training-vector limit for local smoke datasets",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=None,
        help="Optional query-vector limit for local smoke datasets",
    )
    parser.add_argument(
        "--range-neighbor-rank",
        type=int,
        default=100,
        help="Ground-truth neighbor rank used as each range-query radius (default: %(default)s)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=4096,
        help="Number of vectors converted to float32 at a time (default: %(default)s)",
    )
    return parser.parse_args()


def csv_field(value: str | bytes | int | float) -> bytes:
    if isinstance(value, bytes):
        raw = value
    elif isinstance(value, float):
        raw = format(value, ".9g").encode("ascii")
    else:
        raw = str(value).encode("utf-8")
    return b'"' + raw.replace(b'"', b'""') + b'"'


def write_csv_row(output: BinaryIO, fields: Iterable[str | bytes | int | float]) -> None:
    """Write an RFC 4180 row without decoding arbitrary vector bytes as text."""
    output.write(b",".join(csv_field(field) for field in fields))
    output.write(b"\r\n")


def download_source(url: str, destination: Path) -> None:
    print(f"Downloading {url} to {destination}", file=sys.stderr)
    request = urllib.request.Request(url, headers={"User-Agent": "RediSearch benchmark generator"})
    with urllib.request.urlopen(request) as response, destination.open("wb") as output:
        shutil.copyfileobj(response, output, length=1024 * 1024)


def resolve_source(source: str, temp_dir: Path) -> Path:
    if source.startswith(("http://", "https://")):
        destination = temp_dir / "dataset.hdf5"
        download_source(source, destination)
        return destination
    path = Path(source).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Dataset does not exist: {path}")
    return path


def line_safe_vectors(vectors: np.ndarray, stats: SanitizationStats) -> np.ndarray:
    """Avoid physical newlines because FTSB scans input one line at a time."""
    original = np.ascontiguousarray(vectors, dtype="<f4")
    converted = original.copy()
    raw = converted.view(np.uint8).reshape(converted.shape[0], -1)
    unsafe = (raw == UNSAFE_LINE_BYTES[0]) | (raw == UNSAFE_LINE_BYTES[1])
    if not unsafe.any():
        return converted

    stats.adjusted_bytes += int(np.count_nonzero(unsafe))
    stats.adjusted_vectors += int(np.count_nonzero(np.any(unsafe, axis=1)))
    raw[raw == UNSAFE_LINE_BYTES[0]] = UNSAFE_LINE_BYTES[0] + 1
    raw[raw == UNSAFE_LINE_BYTES[1]] = UNSAFE_LINE_BYTES[1] - 1

    changed_components = unsafe.reshape(converted.shape[0], converted.shape[1], 4).any(axis=2)
    absolute_delta = np.abs(converted[changed_components] - original[changed_components])
    relative_delta = absolute_delta / np.maximum(
        np.abs(original[changed_components]), np.finfo(np.float32).tiny
    )
    stats.max_absolute_delta = max(stats.max_absolute_delta, float(np.max(absolute_delta)))
    stats.max_relative_delta = max(stats.max_relative_delta, float(np.max(relative_delta)))
    if not np.isfinite(converted).all():
        raise ValueError("Line-safe FLOAT32 conversion produced a non-finite component")
    return converted


def vector_blob(vector: np.ndarray, stats: SanitizationStats) -> bytes:
    converted = line_safe_vectors(np.asarray(vector).reshape(1, -1), stats)[0]
    if not np.isfinite(converted).all():
        raise ValueError("Dataset contains a non-finite vector component")
    return converted.tobytes(order="C")


def write_setup(
    output_path: Path,
    train: h5py.Dataset,
    vector_count: int,
    dataset_name: str,
    batch_size: int,
    stats: SanitizationStats,
) -> None:
    with output_path.open("wb") as output:
        for batch_start in range(0, vector_count, batch_size):
            batch_end = min(batch_start + batch_size, vector_count)
            vectors = np.asarray(train[batch_start:batch_end], dtype="<f4")
            if not np.isfinite(vectors).all():
                raise ValueError(
                    f"Dataset contains non-finite values in vectors {batch_start}:{batch_end}"
                )
            vectors = line_safe_vectors(vectors, stats)
            for offset, vector in enumerate(vectors):
                doc_id = batch_start + offset
                write_csv_row(
                    output,
                    (
                        "SETUP_WRITE",
                        "vector-load",
                        1,
                        "HSET",
                        f"{dataset_name}:{doc_id}",
                        "vector",
                        vector.tobytes(order="C"),
                    ),
                )
            print(f"Wrote {batch_end:,}/{vector_count:,} setup vectors", file=sys.stderr)


def write_knn_queries(
    output_path: Path, test: h5py.Dataset, query_count: int, stats: SanitizationStats
) -> None:
    query = "*=>[KNN 10 @vector $query AS vector_distance]"
    with output_path.open("wb") as output:
        for query_id in range(query_count):
            write_csv_row(
                output,
                (
                    "READ",
                    "knn-10",
                    1,
                    "FT.SEARCH",
                    "idx",
                    query,
                    "PARAMS",
                    2,
                    "query",
                    vector_blob(test[query_id], stats),
                    "NOCONTENT",
                    "DIALECT",
                    2,
                    "LIMIT",
                    0,
                    10,
                ),
            )


def write_range_queries(
    output_path: Path,
    test: h5py.Dataset,
    distances: h5py.Dataset,
    query_count: int,
    neighbor_rank: int,
    stats: SanitizationStats,
) -> tuple[float, float]:
    query = "@vector:[VECTOR_RANGE $radius $query]"
    radii = np.asarray(distances[:query_count, neighbor_rank - 1], dtype=np.float64)
    if not np.isfinite(radii).all() or np.any(radii < 0):
        raise ValueError("Ground-truth distances contain invalid range radii")

    # Include the neighbor at the selected rank despite float serialization rounding.
    radii = np.nextafter(radii, math.inf)
    with output_path.open("wb") as output:
        for query_id, radius in enumerate(radii):
            write_csv_row(
                output,
                (
                    "READ",
                    f"range-{neighbor_rank}",
                    1,
                    "FT.SEARCH",
                    "idx",
                    query,
                    "PARAMS",
                    4,
                    "radius",
                    float(radius),
                    "query",
                    vector_blob(test[query_id], stats),
                    "NOCONTENT",
                    "DIALECT",
                    2,
                    "LIMIT",
                    0,
                    max(1000, neighbor_rank * 2),
                ),
            )
    return float(np.min(radii)), float(np.max(radii))


def file_metadata(path: Path) -> dict[str, int | str]:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return {
        "name": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def validate_dataset(
    dataset: h5py.File, max_vectors: int | None, max_queries: int | None, neighbor_rank: int
) -> tuple[h5py.Dataset, h5py.Dataset, h5py.Dataset, int, int, int]:
    for name in ("train", "test", "distances"):
        if name not in dataset:
            raise ValueError(f"ANN dataset is missing the '{name}' array")

    train = dataset["train"]
    test = dataset["test"]
    distances = dataset["distances"]
    if train.ndim != 2 or test.ndim != 2 or distances.ndim != 2:
        raise ValueError("Expected two-dimensional train, test, and distances arrays")
    if train.shape[1] != test.shape[1]:
        raise ValueError("Train and test vector dimensions differ")
    if distances.shape[0] < test.shape[0]:
        raise ValueError("Ground-truth distances do not cover all query vectors")
    if neighbor_rank < 1 or neighbor_rank > distances.shape[1]:
        raise ValueError(
            f"Range neighbor rank must be between 1 and {distances.shape[1]}, got {neighbor_rank}"
        )

    vector_count = min(train.shape[0], max_vectors) if max_vectors is not None else train.shape[0]
    query_count = min(test.shape[0], max_queries) if max_queries is not None else test.shape[0]
    if vector_count < 1 or query_count < 1:
        raise ValueError("At least one train vector and one query vector are required")
    return train, test, distances, vector_count, query_count, train.shape[1]


def main() -> int:
    args = parse_args()
    if args.max_vectors is not None and args.max_vectors < 1:
        raise ValueError("--max-vectors must be positive")
    if args.max_queries is not None and args.max_queries < 1:
        raise ValueError("--max-queries must be positive")
    if args.batch_size < 1:
        raise ValueError("--batch-size must be positive")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="redisearch-ann-") as temp_dir_name:
        source_path = resolve_source(args.source, Path(temp_dir_name))
        with h5py.File(source_path, "r") as dataset:
            train, test, distances, vector_count, query_count, dimension = validate_dataset(
                dataset, args.max_vectors, args.max_queries, args.range_neighbor_rank
            )
            setup_path = args.output_dir / f"{args.dataset_name}.redisearch.commands.SETUP.csv"
            knn_path = args.output_dir / f"{args.dataset_name}.redisearch.commands.BENCH.KNN.csv"
            range_path = (
                args.output_dir / f"{args.dataset_name}.redisearch.commands.BENCH.RANGE.csv"
            )
            sanitization = SanitizationStats()

            write_setup(
                setup_path,
                train,
                vector_count,
                args.dataset_name,
                args.batch_size,
                sanitization,
            )
            write_knn_queries(knn_path, test, query_count, sanitization)
            min_radius, max_radius = write_range_queries(
                range_path,
                test,
                distances,
                query_count,
                args.range_neighbor_rank,
                sanitization,
            )

    manifest = {
        "source": args.source,
        "dataset_name": args.dataset_name,
        "vector_count": int(vector_count),
        "query_count": int(query_count),
        "dimension": int(dimension),
        "data_type": "FLOAT32",
        "distance_metric": "COSINE",
        "range_neighbor_rank": args.range_neighbor_rank,
        "range_radius_min": min_radius,
        "range_radius_max": max_radius,
        "ftsb_line_sanitization": {
            "adjusted_bytes": sanitization.adjusted_bytes,
            "adjusted_vectors": sanitization.adjusted_vectors,
            "max_absolute_delta": sanitization.max_absolute_delta,
            "max_relative_delta": sanitization.max_relative_delta,
        },
        "files": [file_metadata(path) for path in (setup_path, knn_path, range_path)],
    }
    manifest_path = args.output_dir / f"{args.dataset_name}.manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, urllib.error.URLError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
