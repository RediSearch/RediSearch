#!/usr/bin/env python3
"""
MS MARCO Dataset Generator for RediSearch Benchmarks.

Generates CSV files with Redis HSET commands for data ingestion
and FT.SEARCH commands for query benchmarks.

Features:
- Processes extracted tar shards directly
- Adds 64 tags with varying cardinality (HIGH/MEDIUM/LOW)
- Deterministic tag assignment via CRC32 hash
- Buffered I/O for fast disk writes

Usage:
    python3 generate_msmarco_dataset.py \\
        --shards-dir ./extracted/msmarco_v2_doc \\
        --sample-pct 50 \\
        --output-dir ./output
"""

import argparse
import csv
import gzip
import os
import re
import sys
import tarfile
import zlib
from glob import glob
from pathlib import Path
from typing import Iterator, List, Tuple, Optional

# Use orjson if available (3-5x faster than stdlib json)
try:
    import orjson
    def json_loads(s):
        return orjson.loads(s)
except ImportError:
    import json
    def json_loads(s):
        return json.loads(s)

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not installed
    def tqdm(iterable, **kwargs):
        total = kwargs.get('total')
        desc = kwargs.get('desc', '')
        for i, item in enumerate(iterable):
            if i % 100000 == 0:
                print(f"{desc}: {i:,} processed...")
            yield item


# =============================================================================
# TAG GENERATION (64 tags with varying cardinality) - OPTIMIZED
# =============================================================================

# Tag cardinality configuration:
# - HIGH (t00-t07):   8 tags,  each ~40-50% of docs
# - MEDIUM (t08-t23): 16 tags, each ~10-20% of docs
# - LOW (t24-t63):    40 tags, each ~2-5% of docs

# Pre-computed tag strings for speed
ALL_TAGS = tuple(f"t{i:02d}" for i in range(64))

# Probability thresholds (as integers 0-100 for faster comparison)
HIGH_THRESH = 45    # 45% chance per tag (t00-t07)
MEDIUM_THRESH = 15  # 15% chance per tag (t08-t23)
LOW_THRESH = 3      # 3% chance per tag (t24-t63)

# Pre-computed tag suffixes as bytes for faster hashing
TAG_SUFFIXES = tuple(f":{t}".encode('utf-8') for t in ALL_TAGS)


def generate_tags_for_doc(doc_id: str) -> str:
    """
    Generate tags for a document with varying cardinality.
    OPTIMIZED: Uses single encode, bitwise operations, pre-computed suffixes.

    Uses deterministic hashing so the same doc_id always gets the same tags.
    Each document gets 1-6 tags on average (~3 tags per doc).
    """
    tags = []
    doc_id_bytes = doc_id.encode('utf-8')
    base_hash = zlib.crc32(doc_id_bytes)

    # HIGH cardinality tags (t00-t07): 45% each
    for i in range(8):
        h = zlib.crc32(TAG_SUFFIXES[i], base_hash) % 100
        if h < HIGH_THRESH:
            tags.append(ALL_TAGS[i])

    # MEDIUM cardinality tags (t08-t23): 15% each
    for i in range(8, 24):
        h = zlib.crc32(TAG_SUFFIXES[i], base_hash) % 100
        if h < MEDIUM_THRESH:
            tags.append(ALL_TAGS[i])

    # LOW cardinality tags (t24-t63): 3% each
    for i in range(24, 64):
        h = zlib.crc32(TAG_SUFFIXES[i], base_hash) % 100
        if h < LOW_THRESH:
            tags.append(ALL_TAGS[i])

    # Ensure at least one tag
    if not tags:
        tags.append(ALL_TAGS[base_hash & 7])  # Fast modulo 8

    return ",".join(tags)


def escape_redis_string(s: str) -> str:
    """Escape special characters for Redis protocol."""
    if s is None:
        return ""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ').replace('\r', '')


def should_sample_doc(doc_id: str, sample_pct: int) -> bool:
    """
    Deterministic sampling based on doc_id hash.
    Same as perf team's approach for reproducibility.
    """
    return (zlib.crc32(doc_id.encode('utf-8')) % 100) < sample_pct


# =============================================================================
# SHARD PROCESSING (from extracted tar)
# =============================================================================

def iter_docs_from_shards(shards_dir: Path) -> Iterator[dict]:
    """
    Iterate over documents from extracted .gz shard files.
    OPTIMIZED: Uses binary read mode for faster orjson parsing.
    """
    shard_files = sorted(glob(str(shards_dir / "msmarco_doc_*.gz")))

    if not shard_files:
        raise FileNotFoundError(f"No shard files found in {shards_dir}")

    print(f"  Found {len(shard_files)} shard files")

    for shard_path in shard_files:
        # Use binary mode - orjson handles bytes directly
        with gzip.open(shard_path, 'rb') as f:
            for line in f:
                try:
                    doc = json_loads(line)
                    yield doc
                except Exception:
                    continue


def iter_docs_from_tar(tar_path: Path) -> Iterator[dict]:
    """
    Iterate over documents directly from tar file (extracts on-the-fly).
    Slower than pre-extracted shards but works without extraction step.
    """
    with tarfile.open(tar_path, 'r') as tar:
        for member in tar.getmembers():
            if member.name.endswith('.gz'):
                f = tar.extractfile(member)
                if f is None:
                    continue
                with gzip.open(f, 'rb') as gz:
                    for line in gz:
                        try:
                            doc = json_loads(line)
                            yield doc
                        except Exception:
                            continue


def generate_setup_commands_from_shards(
    shards_dir: Optional[Path],
    tar_path: Optional[Path],
    output_file: Path,
    doc_limit: int,
    sample_pct: int = 100,
    key_prefix: str = "doc:"
) -> Tuple[int, dict]:
    """
    Generate SETUP.csv file with HSET commands from tar shards.
    Includes 64 tags with varying cardinality.

    Args:
        shards_dir: Directory containing extracted .gz shard files
        tar_path: Path to tar file (used if shards_dir not provided)
        output_file: Path to output CSV file
        doc_limit: Maximum number of documents to generate
        sample_pct: Percentage of documents to sample (1-100)
        key_prefix: Redis key prefix (default: "doc:")

    Returns:
        Tuple of (doc_count, tag_stats dict)
    """
    print(f"Generating SETUP commands with 64 tags...")
    print(f"  Sample percentage: {sample_pct}%")
    print(f"  Document limit: {doc_limit:,}")

    # Choose document source
    if shards_dir and shards_dir.exists():
        print(f"  Source: extracted shards in {shards_dir}")
        docs_iter = iter_docs_from_shards(shards_dir)
    elif tar_path and tar_path.exists():
        print(f"  Source: tar file {tar_path}")
        docs_iter = iter_docs_from_tar(tar_path)
    else:
        raise FileNotFoundError("No valid source: provide --shards-dir or --tar-path")

    doc_count = 0
    tag_counts = {f"t{i:02d}": 0 for i in range(64)}

    # Use larger buffer (64MB) for faster disk writes
    BUFFER_SIZE = 64 * 1024 * 1024

    with open(output_file, 'w', encoding='utf-8', newline='', buffering=BUFFER_SIZE) as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

        # Wrap with progress bar (estimate ~12M total docs)
        docs_iter = tqdm(docs_iter, total=min(doc_limit, 12000000),
                        desc="Generating", unit="docs", unit_scale=True)

        for doc in docs_iter:
            doc_id = doc.get("docid")
            if not doc_id:
                continue

            # Apply sampling
            if sample_pct < 100 and not should_sample_doc(doc_id, sample_pct):
                continue

            if doc_count >= doc_limit:
                break

            # Generate tags for this document
            tags = generate_tags_for_doc(doc_id)

            # Track tag distribution
            for tag in tags.split(","):
                if tag in tag_counts:
                    tag_counts[tag] += 1

            # Build Redis key
            doc_key = f"{key_prefix}{doc_id}"

            # Extract fields
            url = escape_redis_string(doc.get("url", ""))
            title = escape_redis_string(doc.get("title", ""))
            headings = escape_redis_string(doc.get("headings", ""))
            body = escape_redis_string(doc.get("body", ""))

            # Write HSET command row
            writer.writerow([
                "WRITE", "W1", "1", "HSET", doc_key,
                "doc_id", doc_id,
                "url", url,
                "title", title,
                "headings", headings,
                "body", body,
                "tags", tags
            ])

            doc_count += 1

    print(f"✓ Generated {doc_count:,} SETUP commands with tags")
    return doc_count, tag_counts


# =============================================================================
# QUERY GENERATION (predefined benchmark queries)
# =============================================================================

# Benchmark queries from Confluence (Search - Search Profiles Queries)
# These cover different query complexity tiers
BENCHMARK_QUERIES = {
    "baseline": [
        "@title:cardiology",
        "@title:diabetes",
        "covid",
        "wikipedia",
        "health",
    ],
    "phrase": [
        '"credit card"',
    ],
    "and": [
        "(@title:diabetes @body:insulin)",
        "(@title:health @headings:guideline @body:study)",
        "(@url:wikipedia @body:covid @headings:vaccine)",
        "(@title:health @url:nih @headings:guideline @body:study @body:trial)",
    ],
    "or": [
        "(@title:cardiology|@title:diabetes)",
        "(@body:covid|@body:sars|@body:influenza|@body:mers|@body:rsv)",
    ],
    "not": [
        "health -@url:wikipedia",
        "covid -@url:wikipedia -@body:influenza -@body:sars -@body:masks",
    ],
    "tag": [
        "@tags:{t01} health",
        "@tags:{t01} covid",
        "(@tags:{t01|t02}) @title:diabetes",
        "@tags:{t01} health -@url:wikipedia",
    ],
}


def generate_query_commands(
    output_file: Path,
    query_category: str,
    index_name: str,
    num_queries: int = 100000
) -> int:
    """
    Generate BENCH.QUERY_*.csv file with FT.SEARCH commands.
    Uses predefined benchmark queries that cover different complexity tiers.

    Args:
        output_file: Path to output CSV file
        query_category: Category of queries (baseline, phrase, and, or, not, tag, all)
        index_name: RediSearch index name
        num_queries: Number of queries to generate (cycles through available queries)

    Returns:
        Number of queries generated
    """
    print(f"Generating {query_category} query commands...")

    # Get queries for this category
    if query_category == "all":
        queries = []
        for cat_queries in BENCHMARK_QUERIES.values():
            queries.extend(cat_queries)
    elif query_category in BENCHMARK_QUERIES:
        queries = BENCHMARK_QUERIES[query_category]
    else:
        print(f"  Warning: Unknown query category '{query_category}'")
        return 0

    if not queries:
        print(f"  Warning: No queries found for category {query_category}")
        return 0

    print(f"  Found {len(queries)} unique queries in category")

    # Write query commands (cycle through queries to reach num_queries)
    query_count = 0
    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

        for i in range(num_queries):
            query_text = queries[i % len(queries)]

            # Format: "READ","R1","1","FT.SEARCH","index","query","NOCONTENT","LIMIT","0","10"
            # NOCONTENT avoids loading field values from keyspace
            writer.writerow([
                "READ", "R1", "1", "FT.SEARCH", index_name,
                query_text, "NOCONTENT", "LIMIT", "0", "10"
            ])
            query_count += 1

    print(f"✓ Generated {query_count:,} {query_category} queries (with NOCONTENT)")
    return query_count


def print_tag_stats(tag_counts: dict, doc_count: int):
    """Print tag distribution statistics."""
    print("\n" + "="*70)
    print("TAG DISTRIBUTION STATISTICS")
    print("="*70)

    # Group by cardinality tier using index ranges
    high_tags = {k: v for k, v in tag_counts.items() if k in ALL_TAGS[:8]}
    medium_tags = {k: v for k, v in tag_counts.items() if k in ALL_TAGS[8:24]}
    low_tags = {k: v for k, v in tag_counts.items() if k in ALL_TAGS[24:64]}

    def print_tier(name, tags):
        if not tags:
            return
        counts = list(tags.values())
        avg_pct = (sum(counts) / len(counts) / doc_count * 100) if doc_count > 0 else 0
        min_pct = (min(counts) / doc_count * 100) if doc_count > 0 else 0
        max_pct = (max(counts) / doc_count * 100) if doc_count > 0 else 0
        print(f"\n{name} ({len(tags)} tags):")
        print(f"  Average: {avg_pct:.1f}% of docs")
        print(f"  Range: {min_pct:.1f}% - {max_pct:.1f}%")
        print(f"  Sample: {list(tags.items())[:3]}")

    print_tier("HIGH cardinality (t00-t07)", high_tags)
    print_tier("MEDIUM cardinality (t08-t23)", medium_tags)
    print_tier("LOW cardinality (t24-t63)", low_tags)

    # Total tags per doc estimate
    total_tag_assignments = sum(tag_counts.values())
    avg_tags_per_doc = total_tag_assignments / doc_count if doc_count > 0 else 0
    print(f"\nAverage tags per document: {avg_tags_per_doc:.2f}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate MS MARCO dataset for RediSearch benchmarks from tar shards"
    )

    # Source options (mutually exclusive)
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--shards-dir",
        type=Path,
        help="Directory containing extracted msmarco_doc_XX.gz shard files"
    )
    source_group.add_argument(
        "--tar-path",
        type=Path,
        help="Path to msmarco_v2_doc.tar file (slower, extracts on-the-fly)"
    )

    parser.add_argument(
        "--doc-limit",
        type=int,
        default=5000000,
        help="Maximum number of documents to generate (default: 5M for 200GB cluster)"
    )
    parser.add_argument(
        "--sample-pct",
        type=int,
        default=100,
        choices=range(1, 101),
        metavar="[1-100]",
        help="Percentage of documents to sample (default: 100, use 50 for ~6M docs)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./output"),
        help="Output directory for generated files"
    )
    parser.add_argument(
        "--dataset-name",
        type=str,
        default=None,
        help="Dataset name prefix for output files (default: auto-generated)"
    )
    parser.add_argument(
        "--index-name",
        type=str,
        default="ms_marco_idx",
        help="RediSearch index name (default: ms_marco_idx)"
    )
    parser.add_argument(
        "--key-prefix",
        type=str,
        default="doc:",
        help="Redis key prefix (default: 'doc:')"
    )
    parser.add_argument(
        "--num-queries",
        type=int,
        default=100000,
        help="Number of queries per category to generate (default: 100K)"
    )
    parser.add_argument(
        "--skip-queries",
        action="store_true",
        help="Skip query file generation (only generate SETUP.csv)"
    )

    args = parser.parse_args()

    # Auto-generate dataset name if not provided
    if args.dataset_name is None:
        doc_suffix = f"{args.doc_limit // 1000000}M" if args.doc_limit >= 1000000 else f"{args.doc_limit // 1000}K"
        args.dataset_name = f"{doc_suffix}-msmarco-documents"

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"MS MARCO Dataset Generator (with 64 tags)")
    print(f"{'='*70}")
    print(f"  Source: {args.shards_dir or args.tar_path}")
    print(f"  Document limit: {args.doc_limit:,}")
    print(f"  Sample percentage: {args.sample_pct}%")
    print(f"  Output directory: {args.output_dir}")
    print(f"  Output name prefix: {args.dataset_name}")
    print(f"  Key prefix: {args.key_prefix}")
    print(f"{'='*70}\n")

    # Generate SETUP commands with tags
    setup_file = args.output_dir / f"{args.dataset_name}.redisearch.commands.SETUP.csv"
    doc_count, tag_counts = generate_setup_commands_from_shards(
        shards_dir=args.shards_dir,
        tar_path=args.tar_path,
        output_file=setup_file,
        doc_limit=args.doc_limit,
        sample_pct=args.sample_pct,
        key_prefix=args.key_prefix
    )

    # Print tag statistics
    print_tag_stats(tag_counts, doc_count)

    # Generate query commands
    if not args.skip_queries:
        print("\n")
        query_categories = ["baseline", "phrase", "and", "or", "not", "tag", "all"]
        for category in query_categories:
            query_file = args.output_dir / f"{args.dataset_name}.redisearch.commands.BENCH.QUERY_{category}.csv"
            generate_query_commands(query_file, category, args.index_name, args.num_queries)

    print(f"\n{'='*70}")
    print(f"✓ Dataset generation complete!")
    print(f"{'='*70}")
    print(f"Documents: {doc_count:,}")
    print(f"Output directory: {args.output_dir}")
    print(f"\nGenerated files:")
    for file in sorted(args.output_dir.glob(f"{args.dataset_name}*")):
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"  - {file.name} ({size_mb:.1f} MB)")

    print(f"\nSchema for FT.CREATE:")
    print(f"  FT.CREATE {args.index_name} ON HASH PREFIX 1 {args.key_prefix} SCHEMA \\")
    print(f"    url TEXT \\")
    print(f"    title TEXT \\")
    print(f"    headings TEXT \\")
    print(f"    body TEXT \\")
    print(f'    tags TAG SEPARATOR ","')

    print(f"\nNext steps:")
    print(f"  1. Review generated files")
    print(f"  2. Upload to S3: aws s3 cp {args.output_dir}/ s3://benchmarks.redislabs/redisearch/datasets/{args.dataset_name}/ --recursive")
    print()


if __name__ == "__main__":
    main()

