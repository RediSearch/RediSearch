#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""
MS MARCO Dataset Generator for RediSearch Benchmarks.

Generates CSV files with Redis HSET or JSON.SET commands for data ingestion
and FT.SEARCH commands for query benchmarks.

Features:
- Processes extracted tar shards directly
- HASH (HSET) or JSON (JSON.SET) document format via --doc-format
- Adds 64 tags with varying cardinality (HIGH/MEDIUM/LOW)
- Adds 4 NUMERIC fields (n_uniform, n_uniform_small, n_cat, doc_len)
- Adds 2 GEO fields (g_uniform, g_cluster) and writes the GEO queries' match
  counts to <dataset-name>.query_match_counts.md
- Deterministic tag/numeric assignment via CRC32 hash, geo via BLAKE2b
- Buffered I/O for fast disk writes

Usage:
    # HASH dataset (default)
    python3 generate_msmarco_dataset.py \\
        --shards-dir ./extracted/msmarco_v2_doc \\
        --sample-pct 50 \\
        --output-dir ./output

    # JSON dataset (emits JSON.SET; tags stay a scalar comma-separated string)
    python3 generate_msmarco_dataset.py \\
        --shards-dir ./extracted/msmarco_v2_doc \\
        --sample-pct 50 \\
        --doc-format json \\
        --output-dir ./output
"""

import argparse
import bisect
import csv
import gzip
import hashlib
import math
import os
import re
import sys
import tarfile
import zlib
from array import array
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Iterator, List, Tuple, Optional

# Use orjson if available (3-5x faster than stdlib json)
try:
    import orjson
    def json_loads(s):
        return orjson.loads(s)
    def json_dumps(obj):
        # orjson.dumps returns bytes; decode to str for the CSV writer.
        return orjson.dumps(obj).decode("utf-8")
except ImportError:
    import json
    def json_loads(s):
        return json.loads(s)
    def json_dumps(obj):
        # Compact separators keep the serialized document small (6M docs).
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

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


# =============================================================================
# NUMERIC FIELD GENERATION (deterministic, like tags)
# =============================================================================

# Suffix-hashed like the tags so the synthetic fields stay statistically
# decoupled — reusing base_hash % k directly for several fields would couple
# them arithmetically (e.g. n_cat would be a function of n_uniform).
N_UNIFORM_HI_SUFFIX = b":n_uniform_hi"
N_UNIFORM_LO_SUFFIX = b":n_uniform"
N_UNIFORM_SMALL_SUFFIX = b":n_uniform_small"
N_CAT_SUFFIX = b":n_cat"

# n_uniform spans [-100_000_000, 100_000_000] inclusive.
N_UNIFORM_SPAN = 200_000_001
N_UNIFORM_OFFSET = 100_000_000


def generate_numeric_fields_for_doc(doc_id: str, body: str) -> Tuple[int, int, int, int]:
    """
    Deterministic NUMERIC field values for a document.

    Returns (n_uniform, n_uniform_small, n_cat, doc_len):
    - n_uniform: uniform in [-100000000, 100000000] — large scale, mostly
      unique values (~2e8 possible values vs ~6M docs); exact selectivities
      for range queries.
    - n_uniform_small: uniform in [0, 100000) — same uniformity at lower
      cardinality (~60 docs share each value at 6M docs), isolating the
      duplicates-per-value axis at matched selectivities.
    - n_cat: uniform in [0, 10) — extreme duplication, ~10% of docs per value
    - doc_len: raw body length — natural skewed distribution. Computed from
      the raw body (before HASH escaping / JSON normalization) so the value is
      identical across both document formats.
    """
    base_hash = zlib.crc32(doc_id.encode('utf-8'))
    # Two independent CRC32s combined into 64 bits: a single 32-bit hash
    # taken modulo ~2e8 would carry a ~2.5% modulo bias, breaking the exact
    # selectivity guarantee.
    hi = zlib.crc32(N_UNIFORM_HI_SUFFIX, base_hash)
    lo = zlib.crc32(N_UNIFORM_LO_SUFFIX, base_hash)
    n_uniform = ((hi << 32) | lo) % N_UNIFORM_SPAN - N_UNIFORM_OFFSET
    n_uniform_small = zlib.crc32(N_UNIFORM_SMALL_SUFFIX, base_hash) % 100_000
    n_cat = zlib.crc32(N_CAT_SUFFIX, base_hash) % 10
    return n_uniform, n_uniform_small, n_cat, len(body)


# =============================================================================
# GEO FIELD GENERATION (deterministic, like tags)
# =============================================================================

# Both GEO fields live inside this lon/lat box. It is large enough to hold a
# circle matching 10% of g_uniform away from the edges.
GEO_BOX_LON_MIN, GEO_BOX_LON_MAX = -10.0, 30.0
GEO_BOX_LAT_MIN, GEO_BOX_LAT_MAX = 35.0, 60.0
_GEO_BOX_SIN_LAT_MIN = math.sin(math.radians(GEO_BOX_LAT_MIN))
_GEO_BOX_SIN_LAT_MAX = math.sin(math.radians(GEO_BOX_LAT_MAX))

# g_cluster hotspots as (lon, lat), heaviest first. Hotspot k draws a share of
# the docs proportional to 1 / (k + 1) (Zipf, s = 1). The hotspots are far apart
# relative to GEO_CLUSTER_SIGMA_KM, so a query circle around one hotspot only
# matches that hotspot's docs.
GEO_HOTSPOTS = (
    (2.3522, 48.8566),    # Paris
    (-0.1276, 51.5072),   # London
    (-3.7038, 40.4168),   # Madrid
    (13.4050, 52.5200),   # Berlin
    (12.4964, 41.9028),   # Rome
    (2.1734, 41.3851),    # Barcelona
    (9.1900, 45.4642),    # Milan
    (11.5820, 48.1351),   # Munich
    (16.3738, 48.2082),   # Vienna
    (21.0122, 52.2297),   # Warsaw
    (4.9041, 52.3676),    # Amsterdam
    (14.4378, 50.0755),   # Prague
    (19.0402, 47.4979),   # Budapest
    (12.5683, 55.6761),   # Copenhagen
    (-9.1393, 38.7223),   # Lisbon
    (18.0686, 59.3293),   # Stockholm
)
_GEO_HOTSPOT_CDF = tuple(
    sum(1 / (j + 1) for j in range(k + 1)) / sum(1 / (j + 1) for j in range(len(GEO_HOTSPOTS)))
    for k in range(len(GEO_HOTSPOTS))
)
# Gaussian spread of each hotspot, per axis.
GEO_CLUSTER_SIGMA_KM = 20.0
# Matches the Earth radius Redis uses for GEO distances (see geo_distance_m).
_KM_PER_DEGREE = 6372.797560856 * math.pi / 180


def _hash_unit(doc_id_bytes: bytes, suffix: bytes) -> float:
    """
    Deterministic uniform float in [0, 1) for a doc, independent per suffix.

    Uses BLAKE2b, not suffixed CRC32 like the tags and NUMERIC fields: CRC32
    is linear, so draws for equal-length suffixes differ by a constant XOR.
    That makes lon a function of lat and leaves most of the box empty.
    """
    digest = hashlib.blake2b(doc_id_bytes + suffix, digest_size=8).digest()
    return (int.from_bytes(digest, "big") >> 11) / (1 << 53)


def generate_geo_fields_for_doc(doc_id: str) -> Tuple[str, str]:
    """
    Deterministic GEO field values for a document, as "lon,lat" strings.

    Returns (g_uniform, g_cluster):
    - g_uniform: uniform by area inside the GEO_BOX_* box, so a circle fully
      inside the box matches cap_area / box_area of the docs.
    - g_cluster: a Gaussian around one of GEO_HOTSPOTS (Zipf-weighted),
      clipped to the box. Produces the dense geohash cells uniform data lacks.

    The strings are emitted verbatim in both HASH and JSON documents.
    """
    doc_id_bytes = doc_id.encode('utf-8')

    # Sampling sin(lat) uniformly is what makes the density uniform by area;
    # uniform lat would oversample the north of the box.
    lon = GEO_BOX_LON_MIN + _hash_unit(doc_id_bytes, b":g_uniform_lon") * (
        GEO_BOX_LON_MAX - GEO_BOX_LON_MIN)
    lat = math.degrees(math.asin(
        _GEO_BOX_SIN_LAT_MIN + _hash_unit(doc_id_bytes, b":g_uniform_lat") * (
            _GEO_BOX_SIN_LAT_MAX - _GEO_BOX_SIN_LAT_MIN)))
    g_uniform = f"{lon:.6f},{lat:.6f}"

    hotspot = bisect.bisect_right(_GEO_HOTSPOT_CDF, _hash_unit(doc_id_bytes, b":g_cluster_hotspot"))
    hotspot = min(hotspot, len(GEO_HOTSPOTS) - 1)  # guards CDF rounding below 1.0
    center_lon, center_lat = GEO_HOTSPOTS[hotspot]
    # Box-Muller; 1 - u keeps the log argument in (0, 1].
    radius = math.sqrt(-2 * math.log(1 - _hash_unit(doc_id_bytes, b":g_cluster_r")))
    angle = 2 * math.pi * _hash_unit(doc_id_bytes, b":g_cluster_angle")
    dx_km = GEO_CLUSTER_SIGMA_KM * radius * math.cos(angle)
    dy_km = GEO_CLUSTER_SIGMA_KM * radius * math.sin(angle)
    lat = center_lat + dy_km / _KM_PER_DEGREE
    lon = center_lon + dx_km / (_KM_PER_DEGREE * math.cos(math.radians(center_lat)))
    lon = min(max(lon, GEO_BOX_LON_MIN), GEO_BOX_LON_MAX)
    lat = min(max(lat, GEO_BOX_LAT_MIN), GEO_BOX_LAT_MAX)
    g_cluster = f"{lon:.6f},{lat:.6f}"

    return g_uniform, g_cluster


def escape_redis_string(s: str) -> str:
    """Escape special characters for Redis protocol (HSET field values)."""
    if s is None:
        return ""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ').replace('\r', '')


def normalize_text(s: str) -> str:
    """
    Normalize whitespace for a field value used in a JSON document.

    Mirrors the newline/CR handling of escape_redis_string so the indexed text
    is identical between the HASH and JSON datasets, but leaves quote/backslash
    escaping to the JSON serializer (json_dumps), which handles it correctly.
    """
    if s is None:
        return ""
    return s.replace('\n', ' ').replace('\r', '')


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

    A `tar_path` of "-" reads the tar from stdin, so it can be piped from a
    download without storing it. Members are read in archive order, which
    for msmarco_v2_doc.tar is the sorted shard order iter_docs_from_shards
    uses, so both sources emit the same rows.
    """
    if str(tar_path) == "-":
        tar = tarfile.open(fileobj=sys.stdin.buffer, mode='r|')
    else:
        tar = tarfile.open(tar_path, mode='r|')
    with tar:
        for member in tar:
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
    key_prefix: str = "doc:",
    doc_format: str = "hash",
    geo_docs: Optional["GeoDocs"] = None
) -> Tuple[int, dict]:
    """
    Generate SETUP.csv file with HSET or JSON.SET commands from tar shards.
    Includes 64 tags with varying cardinality.

    Args:
        shards_dir: Directory containing extracted .gz shard files
        tar_path: Path to tar file (used if shards_dir not provided)
        output_file: Path to output CSV file
        doc_limit: Maximum number of documents to generate
        sample_pct: Percentage of documents to sample (1-100)
        key_prefix: Redis key prefix (default: "doc:")
        doc_format: "hash" emits HSET commands, "json" emits JSON.SET commands.
            In "json" mode the same fields are stored under a single JSON
            document; ``tags`` stays a single comma-separated scalar string
            (never a JSON array) to honor the no-multivalue constraint.
        geo_docs: If given, records every emitted doc's GEO values, in file
            order, for count_geo_matches.

    Returns:
        Tuple of (doc_count, tag_stats dict)
    """
    print(f"Generating SETUP commands ({doc_format.upper()}) with 64 tags...")
    print(f"  Sample percentage: {sample_pct}%")
    print(f"  Document limit: {doc_limit:,}")

    # Choose document source
    if shards_dir and shards_dir.exists():
        print(f"  Source: extracted shards in {shards_dir}")
        docs_iter = iter_docs_from_shards(shards_dir)
    elif tar_path and (str(tar_path) == "-" or tar_path.exists()):
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

            # Generate tags and numeric fields for this document
            tags = generate_tags_for_doc(doc_id)
            n_uniform, n_uniform_small, n_cat, doc_len = generate_numeric_fields_for_doc(
                doc_id, doc.get("body") or "")
            g_uniform, g_cluster = generate_geo_fields_for_doc(doc_id)
            if geo_docs is not None:
                geo_docs.append(g_uniform, g_cluster, n_cat)

            # Track tag distribution
            for tag in tags.split(","):
                if tag in tag_counts:
                    tag_counts[tag] += 1

            # Build Redis key
            doc_key = f"{key_prefix}{doc_id}"

            if doc_format == "json":
                # Build a single JSON document. tags stays a scalar
                # comma-separated string (no JSON array) so that indexing it as
                # TAG with an explicit SEPARATOR "," splits it identically to the
                # HASH dataset (JSON TAG fields otherwise default to no separator).
                # json_dumps handles escaping; normalize_text only strips
                # newlines/CR so the indexed text matches the HASH dataset.
                json_doc = json_dumps({
                    "doc_id": doc_id,
                    "url": normalize_text(doc.get("url", "")),
                    "title": normalize_text(doc.get("title", "")),
                    "headings": normalize_text(doc.get("headings", "")),
                    "body": normalize_text(doc.get("body", "")),
                    "tags": tags,
                    # JSON numbers (not strings) so NUMERIC indexing works.
                    "n_uniform": n_uniform,
                    "n_uniform_small": n_uniform_small,
                    "n_cat": n_cat,
                    "doc_len": doc_len,
                    # JSON GEO fields index a "lon,lat" string.
                    "g_uniform": g_uniform,
                    "g_cluster": g_cluster,
                })
                writer.writerow([
                    "WRITE", "W1", "1", "JSON.SET", doc_key, "$", json_doc
                ])
            else:
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
                    "tags", tags,
                    "n_uniform", n_uniform,
                    "n_uniform_small", n_uniform_small,
                    "n_cat", n_cat,
                    "doc_len", doc_len,
                    "g_uniform", g_uniform,
                    "g_cluster", g_cluster
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

# NUMERIC benchmark queries. Each group runs as its own benchmark, and every
# group's queries return result sets of roughly the SAME size (within ~2x):
# the exported metrics are per-group aggregates, so a group mixing (say) 1%
# and 50% selectivities would have its ops/sec and latency quantiles
# dominated by the large query, hiding regressions in the small one. Shape
# variants (duplication, OR, AND) are therefore compared inside a group
# against a plain range of the same result size.
#
# Field value distributions (assigned in generate_numeric_fields_for_doc):
#   n_uniform        uniform over the 2e8+1 ints in [-1e8, 1e8]. Values are
#                    effectively unique at ~6M docs, so a range of width W
#                    matches a W/2e8 fraction of the docs: selectivities
#                    below are exact by construction (in expectation).
#   n_uniform_small  uniform over [0, 1e5): same selectivity math, but ~60
#                    docs share each value at 6M docs (duplicated postings).
#   n_cat            uniform over [0, 10): 10 huge postings, ~10% of docs
#                    per value.
#   doc_len          raw body length: natural skewed distribution, so its
#                    selectivities are measured, not constructed (see the
#                    generated dataset's query_match_counts ground truth).
#
# Numeric ranges are valid under the default dialect — no DIALECT argument.
NUMERIC_QUERY_GROUPS = {
    # ~0.001% (tens of docs at a 2M load)
    "numeric-point": [
        "@n_uniform:[0 1999]",           # needle range over unique values
        "@n_uniform_small:[500 500]",    # exact point, one duplicated value
    ],
    # ~1-2%: operator/shape variants vs a plain range of the same size
    "numeric-narrow": [
        "@n_uniform:[0 1999999]",        # plain 1% range (in-group reference)
        "@n_uniform_small:[0 999]",      # 1% over duplicated values
        "(@n_uniform:[0 1999999] | @n_uniform:[50000000 51999999])",  # OR, 2%
        "(@n_uniform:[0 19999999] @n_uniform_small:[0 9999])",  # AND: 10% ^ 10% = 1%
    ],
    # ~3-5%: the natural (skewed) distribution
    "numeric-doclen": [
        "@doc_len:[0 1000]",             # short docs
        "@doc_len:[50000 +inf]",         # long-tail docs
    ],
    # ~10%: same result size, opposite index shape
    "numeric-wide": [
        "@n_uniform:[0 19999999]",       # scan 10% of a unique-valued range tree
        "@n_cat:[3 3]",                  # read one huge posting (1 of 10 values)
    ],
}
BENCHMARK_QUERIES.update(NUMERIC_QUERY_GROUPS)
# Combined pool (ad-hoc runs; CI uses the per-group workloads above).
BENCHMARK_QUERIES["numeric"] = [q for qs in NUMERIC_QUERY_GROUPS.values() for q in qs]


@dataclass(frozen=True)
class GeoCircle:
    field: str
    lon: float
    lat: float
    radius: float
    unit: str  # "m" or "km"

    def text(self) -> str:
        return f"@{self.field}:[{self.lon:.10g} {self.lat:.10g} {self.radius:.10g} {self.unit}]"

    @property
    def radius_m(self) -> float:
        return self.radius * {"m": 1.0, "km": 1000.0}[self.unit]


@dataclass(frozen=True)
class GeoQuery:
    """A doc matches if it is inside any of `circles` and, if set, has `n_cat`."""
    circles: Tuple[GeoCircle, ...]
    n_cat: Optional[int] = None

    def text(self) -> str:
        query = " | ".join(c.text() for c in self.circles)
        if len(self.circles) > 1:
            query = f"({query})"
        if self.n_cat is not None:
            query = f"({query} @n_cat:[{self.n_cat} {self.n_cat}])"
        return query


def _uniform(lon: float, lat: float, radius: float, unit: str, n_cat: Optional[int] = None) -> GeoQuery:
    return GeoQuery((GeoCircle("g_uniform", lon, lat, radius, unit),), n_cat)


def _cluster(hotspot: int, radius: float, unit: str) -> GeoQuery:
    return GeoQuery((GeoCircle("g_cluster", *GEO_HOTSPOTS[hotspot], radius, unit),))


# GEO benchmark queries, grouped by result size like NUMERIC_QUERY_GROUPS.
#
# g_uniform radii come from the spherical cap formula: a circle fully inside
# the GEO_BOX_* box matches cap_area / box_area of the docs (the box is
# ~8.29M km^2). g_cluster radii are tuned against GEO_HOTSPOTS and
# GEO_CLUSTER_SIGMA_KM, and their selectivities are measured: see the
# generated dataset's query_match_counts.md.
#
# GEO filters are valid under the default dialect — no DIALECT argument.
GEO_QUERY_GROUPS = {
    "geo-point": [
        _uniform(10, 47.5, 5.1, "km"),   # needle, fine geohash step, sparse cells
        _cluster(0, 200, "m"),           # needle, fine step, dense cells
    ],
    "geo-narrow": [
        _uniform(10, 47.5, 162, "km"),   # plain 1% radius (in-group reference)
        GeoQuery((GeoCircle("g_uniform", 0, 45, 115, "km"),
                  GeoCircle("g_uniform", 20, 52, 115, "km"))),  # OR of two disjoint 0.5% circles
        _uniform(10, 47.5, 514, "km", n_cat=3),  # AND: 10% ^ 10% = 1%
        _cluster(7, 16, "km"),           # skewed, mid-weight hotspot
    ],
    "geo-wide": [
        _uniform(10, 47.5, 514, "km"),   # coarse step, many rejects at the circle edge
        _cluster(0, 18, "km"),           # few dense cells, one large posting per range
    ],
}
# Target fraction of a benchmark load each group's queries should match.
# count_geo_matches accepts within 2x of it either way.
GEO_GROUP_SELECTIVITY = {
    "geo-point": 0.00001,
    "geo-narrow": 0.01,
    "geo-wide": 0.1,
}
assert GEO_GROUP_SELECTIVITY.keys() == GEO_QUERY_GROUPS.keys()
BENCHMARK_QUERIES.update(
    {group: [q.text() for q in queries] for group, queries in GEO_QUERY_GROUPS.items()})
# Combined pool (ad-hoc runs; CI uses the per-group workloads above).
BENCHMARK_QUERIES["geo"] = [q for group in GEO_QUERY_GROUPS for q in BENCHMARK_QUERIES[group]]

# Docs loaded by each msmarco benchmark: the first `requests` rows of
# SETUP.csv in the benchmark YAMLs.
BENCHMARK_LOAD_DOCS = 2_000_000


# =============================================================================
# GEO MATCH COUNTS (ground truth for the GEO queries)
# =============================================================================

# Mirror the geohash constants in src/redisearch_rs/geo/src/hash/mod.rs, so the
# offline counts decide edge points exactly as Redis does.
_GEO_STEP_SIZE = float(1 << 26)
_GEO_LAT_MIN, _GEO_LAT_MAX = -85.05112878, 85.05112878
_GEO_LONG_MIN, _GEO_LONG_MAX = -180.0, 180.0
_EARTH_RADIUS_M = 6372797.560856


def geohash_cell_center(lon: float, lat: float) -> Tuple[float, float]:
    """The point Redis compares against a GEO filter for a stored (lon, lat)."""
    max_fixed = (1 << 26) - 1
    lat_fixed = min(int((lat - _GEO_LAT_MIN) / (_GEO_LAT_MAX - _GEO_LAT_MIN) * _GEO_STEP_SIZE), max_fixed)
    lon_fixed = min(int((lon - _GEO_LONG_MIN) / (_GEO_LONG_MAX - _GEO_LONG_MIN) * _GEO_STEP_SIZE), max_fixed)
    lon = _GEO_LONG_MIN + ((lon_fixed + 0.5) / _GEO_STEP_SIZE) * (_GEO_LONG_MAX - _GEO_LONG_MIN)
    lat = _GEO_LAT_MIN + ((lat_fixed + 0.5) / _GEO_STEP_SIZE) * (_GEO_LAT_MAX - _GEO_LAT_MIN)
    return (min(max(lon, _GEO_LONG_MIN), _GEO_LONG_MAX),
            min(max(lat, _GEO_LAT_MIN), _GEO_LAT_MAX))


def geo_distance_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Haversine distance, as computed by Redis's haversine_distance."""
    lat1r, lon1r = math.radians(lat1), math.radians(lon1)
    lat2r, lon2r = math.radians(lat2), math.radians(lon2)
    u = math.sin((lat2r - lat1r) / 2)
    v = math.sin((lon2r - lon1r) / 2)
    return 2 * _EARTH_RADIUS_M * math.asin(
        min(math.sqrt(u * u + math.cos(lat1r) * math.cos(lat2r) * v * v), 1.0))


class GeoDocs:
    """GEO values of the emitted docs, in file order, as Redis decodes them."""

    FIELDS = ("g_uniform", "g_cluster")

    def __init__(self):
        self.lon = {f: array('d') for f in self.FIELDS}
        self.lat = {f: array('d') for f in self.FIELDS}
        self.n_cat = array('B')

    def append(self, g_uniform: str, g_cluster: str, n_cat: int):
        for field, value in zip(self.FIELDS, (g_uniform, g_cluster)):
            lon, lat = (float(x) for x in value.split(","))
            lon, lat = geohash_cell_center(lon, lat)
            self.lon[field].append(lon)
            self.lat[field].append(lat)
        self.n_cat.append(n_cat)

    def __len__(self):
        return len(self.n_cat)


def _count_matches(docs: GeoDocs, query: GeoQuery, subset: int) -> Tuple[int, int]:
    """Returns (matches over all docs, matches over the first `subset` docs)."""
    hits = bytearray(len(docs))
    for c in query.circles:
        lons, lats = docs.lon[c.field], docs.lat[c.field]
        # Bounding-box prefilter with a 1% margin; geo_distance_m decides.
        dlat = math.degrees(c.radius_m / _EARTH_RADIUS_M) * 1.01
        dlon = dlat / math.cos(math.radians(min(abs(c.lat) + dlat, 89.0)))
        lat_lo, lat_hi = c.lat - dlat, c.lat + dlat
        lon_lo, lon_hi = c.lon - dlon, c.lon + dlon
        for i in range(len(docs)):
            lat = lats[i]
            if lat_lo <= lat <= lat_hi:
                lon = lons[i]
                if lon_lo <= lon <= lon_hi and geo_distance_m(c.lon, c.lat, lon, lat) <= c.radius_m:
                    hits[i] = 1
    if query.n_cat is not None:
        for i, n_cat in enumerate(docs.n_cat):
            if n_cat != query.n_cat:
                hits[i] = 0
    return sum(hits), sum(hits[:subset])


# g_uniform occupancy grid: equal-area cells, so each holds 1 / cells of docs.
_GEO_GRID_LON_CELLS, _GEO_GRID_LAT_CELLS = 8, 5
# Max relative deviation of any cell from its expected count. Loose enough for
# sampling noise at a benchmark load, tight enough to catch correlated draws.
GEO_GRID_MAX_DEVIATION = 0.05


def _g_uniform_grid_deviation(docs: GeoDocs, subset: int) -> float:
    """Max relative deviation of g_uniform's equal-area grid cells, first `subset` docs."""
    cells = [0] * (_GEO_GRID_LON_CELLS * _GEO_GRID_LAT_CELLS)
    lons, lats = docs.lon["g_uniform"], docs.lat["g_uniform"]
    for i in range(subset):
        x = (lons[i] - GEO_BOX_LON_MIN) / (GEO_BOX_LON_MAX - GEO_BOX_LON_MIN)
        y = (math.sin(math.radians(lats[i])) - _GEO_BOX_SIN_LAT_MIN) / (
            _GEO_BOX_SIN_LAT_MAX - _GEO_BOX_SIN_LAT_MIN)
        col = min(max(int(x * _GEO_GRID_LON_CELLS), 0), _GEO_GRID_LON_CELLS - 1)
        row = min(max(int(y * _GEO_GRID_LAT_CELLS), 0), _GEO_GRID_LAT_CELLS - 1)
        cells[row * _GEO_GRID_LON_CELLS + col] += 1
    expected = subset / len(cells)
    return max(abs(c - expected) for c in cells) / expected if subset else 0.0


def write_geo_match_counts(output_file: Path, docs: GeoDocs) -> bool:
    """
    Writes each GEO query's match count over all docs and over one benchmark
    load (the first BENCHMARK_LOAD_DOCS docs) as a markdown table.

    Also reports how evenly g_uniform fills the box (_g_uniform_grid_deviation).

    Returns False if a full benchmark load is available and some query falls
    outside its GEO_GROUP_SELECTIVITY band, or the grid deviation exceeds
    GEO_GRID_MAX_DEVIATION. Smaller runs skip both checks.
    """
    subset = min(len(docs), BENCHMARK_LOAD_DOCS)
    check_bands = subset == BENCHMARK_LOAD_DOCS
    checks_ok = True
    lines = [
        "# GEO query match counts",
        "",
        f"All docs: {len(docs):,}. Benchmark load: first {subset:,} docs.",
        "",
        "| Group | Query | All docs | Benchmark load | % of load | Target band | In band |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for group, queries in GEO_QUERY_GROUPS.items():
        target = GEO_GROUP_SELECTIVITY[group] * subset
        lo, hi = target / 2, target * 2
        for query in queries:
            print(f"  Counting matches: {query.text()}")
            total, in_load = _count_matches(docs, query, subset)
            if not check_bands:
                verdict = "not checked"
            elif lo <= in_load <= hi:
                verdict = "yes"
            else:
                verdict = "**NO**"
                checks_ok = False
            pct = in_load / subset * 100 if subset else 0.0
            # GFM splits table cells on "|" even inside code spans.
            cell = query.text().replace("|", "\\|")
            lines.append(
                f"| {group} | `{cell}` | {total:,} | {in_load:,} | {pct:.4f}% "
                f"| {lo:,.0f} – {hi:,.0f} | {verdict} |")
    deviation = _g_uniform_grid_deviation(docs, subset)
    grid_ok = deviation <= GEO_GRID_MAX_DEVIATION
    checks_ok = checks_ok and (grid_ok or not check_bands)
    lines += [
        "",
        f"g_uniform spread: {_GEO_GRID_LON_CELLS} x {_GEO_GRID_LAT_CELLS} equal-area cells over the "
        f"benchmark load, max deviation from the expected count {deviation:.2%} "
        f"(limit {GEO_GRID_MAX_DEVIATION:.0%}): "
        + ("not checked" if not check_bands else "ok" if grid_ok else "**FAILED**"),
    ]
    if not check_bands:
        lines += ["", f"Band and spread checks need at least {BENCHMARK_LOAD_DOCS:,} docs."]
    output_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✓ Wrote GEO match counts to {output_file}")
    return checks_ok


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
        for cat, cat_queries in BENCHMARK_QUERIES.items():
            # "numeric" and "geo" duplicate their per-group pools — skip them.
            if cat in ("numeric", "geo"):
                continue
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
    # MS MARCO bodies contain NUL characters. Python 3.11's csv writer emits
    # them raw, as in the published datasets; older versions raise
    # "need to escape, but no escapechar set" on the first such row.
    if sys.version_info < (3, 11):
        sys.exit(f"Python 3.11+ is required (running {sys.version.split()[0]}): "
                 "older csv modules cannot write the NUL characters in MS MARCO bodies")

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
        help="Path to msmarco_v2_doc.tar file (slower, extracts on-the-fly), "
             "or '-' to read the tar from stdin"
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
        "--doc-format",
        type=str,
        default="hash",
        choices=["hash", "json"],
        help="Document storage format: 'hash' emits HSET commands, "
             "'json' emits JSON.SET commands (default: hash)"
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

    # Auto-generate dataset name if not provided. The JSON dataset gets its own
    # prefix (…-msmarco-json-documents) so it lives under a separate S3 path and
    # does not collide with the HASH dataset.
    if args.dataset_name is None:
        doc_suffix = f"{args.doc_limit // 1000000}M" if args.doc_limit >= 1000000 else f"{args.doc_limit // 1000}K"
        format_infix = "json-" if args.doc_format == "json" else ""
        args.dataset_name = f"{doc_suffix}-msmarco-{format_infix}documents"

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
    print(f"  Document format: {args.doc_format.upper()}")
    print(f"{'='*70}\n")

    # Generate SETUP commands with tags
    setup_file = args.output_dir / f"{args.dataset_name}.redisearch.commands.SETUP.csv"
    geo_docs = None if args.skip_queries else GeoDocs()
    doc_count, tag_counts = generate_setup_commands_from_shards(
        shards_dir=args.shards_dir,
        tar_path=args.tar_path,
        output_file=setup_file,
        doc_limit=args.doc_limit,
        sample_pct=args.sample_pct,
        key_prefix=args.key_prefix,
        doc_format=args.doc_format,
        geo_docs=geo_docs
    )

    # Print tag statistics
    print_tag_stats(tag_counts, doc_count)

    # Generate query commands
    geo_in_band = True
    if not args.skip_queries:
        print("\n")
        # Both HASH and JSON index tags (tags TAG SEPARATOR ","), so every query
        # category — including tag predicates — is valid for both formats.
        query_categories = ["baseline", "phrase", "and", "or", "not", "tag",
                            "numeric", *NUMERIC_QUERY_GROUPS,
                            "geo", *GEO_QUERY_GROUPS, "all"]
        for category in query_categories:
            query_file = args.output_dir / f"{args.dataset_name}.redisearch.commands.BENCH.QUERY_{category}.csv"
            generate_query_commands(query_file, category, args.index_name, args.num_queries)

        print("\n")
        geo_in_band = write_geo_match_counts(
            args.output_dir / f"{args.dataset_name}.query_match_counts.md", geo_docs)

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
    if args.doc_format == "json":
        print(f"  FT.CREATE {args.index_name} ON JSON PREFIX 1 {args.key_prefix} SCHEMA \\")
        print(f"    $.url       AS url       TEXT \\")
        print(f"    $.title     AS title     TEXT \\")
        print(f"    $.headings  AS headings  TEXT \\")
        print(f"    $.body      AS body      TEXT \\")
        print(f'    $.tags            AS tags            TAG SEPARATOR "," \\')
        print(f"    $.n_uniform       AS n_uniform       NUMERIC \\")
        print(f"    $.n_uniform_small AS n_uniform_small NUMERIC \\")
        print(f"    $.n_cat           AS n_cat           NUMERIC \\")
        print(f"    $.doc_len         AS doc_len         NUMERIC \\")
        print(f"    $.g_uniform       AS g_uniform       GEO \\")
        print(f"    $.g_cluster       AS g_cluster       GEO")
    else:
        print(f"  FT.CREATE {args.index_name} ON HASH PREFIX 1 {args.key_prefix} SCHEMA \\")
        print(f"    url TEXT \\")
        print(f"    title TEXT \\")
        print(f"    headings TEXT \\")
        print(f"    body TEXT \\")
        print(f'    tags TAG SEPARATOR "," \\')
        print(f"    n_uniform NUMERIC \\")
        print(f"    n_uniform_small NUMERIC \\")
        print(f"    n_cat NUMERIC \\")
        print(f"    doc_len NUMERIC \\")
        print(f"    g_uniform GEO \\")
        print(f"    g_cluster GEO")
    # tags is stored as a comma-separated scalar string in both HASH and JSON and
    # indexed as TAG. JSON TAG fields default to no separator, so the explicit
    # SEPARATOR "," is required for JSON to split the scalar identically to HASH.
    print(f'  # tags: comma-separated scalar string; indexed as TAG (explicit SEPARATOR "," for JSON).')

    print(f"\nNext steps:")
    print(f"  1. Review generated files")
    print(f"  2. Upload to S3: aws s3 cp {args.output_dir}/ s3://benchmarks.redislabs/redisearch/datasets/{args.dataset_name}/ --recursive")
    print()

    if not geo_in_band:
        print("✗ GEO checks failed (size bands or g_uniform spread); see query_match_counts.md")
        sys.exit(1)


if __name__ == "__main__":
    main()

