#!/usr/bin/env python3
"""
JSON+Vector Ingestion Dataset Generator for RediSearch Benchmarks.

Emits a CSV of ``JSON.SET`` commands suitable for ``ftsb_redisearch`` loading,
each storing a JSON document with a vector field and a few numeric fields.
The resulting SETUP file is intended to measure the ``FT.CREATE`` + JSON
ingestion path end-to-end (see MOD-14943).

Row format (matches existing RedisJSON SETUP files on
``s3://benchmarks.redislabs/redisearch/datasets/``)::

    WRITE,W1,1,JSON.SET,<key>,<path>,<json>

Usage::

    python3 generate_json_vector_dataset.py \
        --num-docs 100000 \
        --dim 384 \
        --output-dir ./output \
        --dataset-name 100K-json-vector

Produces ``<output-dir>/<dataset-name>.redisjson.commands.SETUP.csv``.
"""

import argparse
import csv
import json
import os
import random
import sys
from pathlib import Path


def _rand_vec(dim: int, rng: random.Random) -> list:
    # JSON-serialisable floats; rounded to keep the CSV compact without
    # meaningfully affecting either ingestion cost or vector semantics.
    return [round(rng.uniform(-1.0, 1.0), 4) for _ in range(dim)]


def _rand_numeric(rng: random.Random) -> dict:
    return {
        "n_int": rng.randint(-1_000_000, 1_000_000),
        "n_float": round(rng.uniform(-1000.0, 1000.0), 4),
    }


def _iter_rows(num_docs: int, dim: int, seed: int, key_prefix: str):
    rng = random.Random(seed)
    for i in range(num_docs):
        doc = {"v": _rand_vec(dim, rng), **_rand_numeric(rng)}
        # Match the existing convention of using the legacy root path ``.``
        # rather than ``$`` — both are accepted by RedisJSON for simple roots
        # and this keeps the dataset consistent with other SETUP files.
        yield ["WRITE", "W1", "1", "JSON.SET",
               f"{key_prefix}{i}", ".", json.dumps(doc, separators=(",", ":"))]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--num-docs", type=int, required=True,
                   help="Number of JSON documents to emit.")
    p.add_argument("--dim", type=int, default=384,
                   help="Vector dimensionality (default: 384).")
    p.add_argument("--seed", type=int, default=0xDA7A,
                   help="PRNG seed for reproducible output (default: 0xDA7A).")
    p.add_argument("--key-prefix", default="doc:jv:",
                   help="Redis key prefix (default: doc:jv:).")
    p.add_argument("--dataset-name", required=True,
                   help="Base name; produces <name>.redisjson.commands.SETUP.csv.")
    p.add_argument("--output-dir", default=".",
                   help="Output directory (default: current directory).")
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{args.dataset_name}.redisjson.commands.SETUP.csv"

    # QUOTE_MINIMAL is enough because csv handles the embedded quotes in the
    # JSON payload by doubling them, matching the format produced by the
    # existing datasets (e.g. 10K-singlevalue-numeric-json).
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        for row in _iter_rows(args.num_docs, args.dim, args.seed, args.key_prefix):
            w.writerow(row)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"Wrote {args.num_docs} rows to {out_path} ({size_mb:.1f} MiB)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
