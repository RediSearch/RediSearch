#!/usr/bin/env python3
"""Create the LTK ``idx:acs`` index on a Redis Cluster."""

# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import argparse
import ipaddress
import os
import sys
import time
from typing import Any

INDEX_NAME = "idx:acs"
INDEX_WAIT_TIMEOUT_SEC = 7_200.0
INDEX_POLL_INTERVAL_SEC = 5.0
SCHEMA_FIELDS: list[list[str]] = [
    ["profile_id", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["post_id", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["product_id", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["type", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["iscore", "NUMERIC", "SORTABLE", "UNF"],
    ["iscore2", "NUMERIC", "SORTABLE", "UNF"],
    ["iscore3", "NUMERIC", "SORTABLE", "UNF"],
    ["updated_at", "NUMERIC", "SORTABLE", "UNF"],
    ["profile_display_name", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    [
        "profile_display_name",
        "AS",
        "profile_display_name_txt",
        "TEXT",
        "WEIGHT",
        "5",
        "WITHSUFFIXTRIE",
    ],
    ["profile_display_name_text", "TEXT", "WEIGHT", "5", "WITHSUFFIXTRIE"],
    ["country_code", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["country_region", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["country_state", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["region", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["lvl0s", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF"],
    ["lvl0s", "AS", "lvl0s_txt", "TEXT", "WEIGHT", "1", "WITHSUFFIXTRIE"],
    ["lvl1s", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF"],
    ["lvl1s", "AS", "lvl1s_txt", "TEXT", "WEIGHT", "1", "WITHSUFFIXTRIE"],
    ["lvl2s", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF"],
    ["lvl2s", "AS", "lvl2s_txt", "TEXT", "WEIGHT", "1", "WITHSUFFIXTRIE"],
    ["brands", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF"],
    [
        "brands",
        "AS",
        "brands_txt",
        "TEXT",
        "WEIGHT",
        "1",
        "NOSTEM",
        "WITHSUFFIXTRIE",
    ],
    ["retailer_names", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF"],
    [
        "retailer_names",
        "AS",
        "retailer_names_txt",
        "TEXT",
        "WEIGHT",
        "1",
        "NOSTEM",
        "WITHSUFFIXTRIE",
    ],
    ["searchable", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["segment", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["has_mentions_enabled", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["full_name", "TEXT", "WEIGHT", "10", "NOSTEM", "WITHSUFFIXTRIE"],
    ["alternate_names", "TEXT", "WEIGHT", "5", "NOSTEM", "WITHSUFFIXTRIE"],
    ["bio", "TEXT", "WEIGHT", "1", "WITHSUFFIXTRIE"],
    ["created_at", "NUMERIC", "SORTABLE", "UNF"],
    ["media_type", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["post_type", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["styled", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["storytelling", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["post_tags", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF"],
    ["post_tags", "AS", "post_tags_txt", "TEXT", "WEIGHT", "1", "WITHSUFFIXTRIE"],
    ["retailer_ids", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF"],
    ["caption", "TEXT", "WEIGHT", "1", "WITHSUFFIXTRIE"],
    ["has_products", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["product_ids", "TAG", "SEPARATOR", "|", "SORTABLE", "UNF", "NOINDEX"],
    ["name", "TEXT", "WEIGHT", "3", "WITHSUFFIXTRIE"],
    ["description", "TEXT", "WEIGHT", "1", "WITHSUFFIXTRIE"],
    ["retailer_id", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["price", "NUMERIC", "SORTABLE", "UNF"],
    ["currency", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["available", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["rp_id", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["posts_index_name", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    ["product_reference_id", "TAG", "SEPARATOR", ",", "SORTABLE", "UNF"],
    [
        "text_vector16",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT16",
        "DIM",
        "1024",
        "DISTANCE_METRIC",
        "COSINE",
        "M",
        "16",
        "EF_CONSTRUCTION",
        "200",
    ],
    [
        "multimodal_vector16",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT16",
        "DIM",
        "1536",
        "DISTANCE_METRIC",
        "COSINE",
        "M",
        "16",
        "EF_CONSTRUCTION",
        "200",
    ],
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard_ip", type=ipaddress.ip_address)
    parser.add_argument("--port", type=int, default=7000)
    parser.add_argument("--username", default=os.getenv("REDIS_USERNAME"))
    parser.add_argument(
        "--password",
        default=os.getenv("REDIS_PASSWORD") or os.getenv("REDISCLI_AUTH"),
        help="Redis password (prefer REDIS_PASSWORD instead of the command line)",
    )
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--connect-timeout", type=float, default=10.0)
    parser.add_argument("--index-wait-timeout", type=float, default=INDEX_WAIT_TIMEOUT_SEC)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.connect_timeout <= 0:
        parser.error("--connect-timeout must be positive")
    if args.index_wait_timeout <= 0:
        parser.error("--index-wait-timeout must be positive")
    return args


def module_names(modules: Any) -> set[str]:
    names: set[str] = set()
    for module in modules:
        if isinstance(module, dict):
            name = module.get("name") or module.get(b"name")
        else:
            values = list(module)
            name = next(
                (
                    values[i + 1]
                    for i in range(0, len(values) - 1, 2)
                    if values[i] in ("name", b"name")
                ),
                None,
            )
        if isinstance(name, bytes):
            name = name.decode()
        if name is not None:
            names.add(str(name).lower())
    return names


def cluster_is_healthy(cluster_info: Any) -> bool:
    if isinstance(cluster_info, dict):
        state = cluster_info.get("cluster_state") or cluster_info.get(b"cluster_state")
        if isinstance(state, bytes):
            state = state.decode()
        return state == "ok"
    if isinstance(cluster_info, bytes):
        cluster_info = cluster_info.decode()
    return "cluster_state:ok" in str(cluster_info).splitlines()


def info_dict(response: Any) -> dict[str, Any]:
    if isinstance(response, dict):
        return response
    values = list(response)
    return dict(zip(values[::2], values[1::2]))


def wait_for_index(client: Any, timeout_sec: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_sec
    while True:
        info = info_dict(client.execute_command("FT.INFO", INDEX_NAME))
        if "indexing" not in info:
            raise RuntimeError(f"{INDEX_NAME} FT.INFO response has no indexing status")
        indexing = int(info["indexing"])
        percent_value = info.get("percent_indexed")
        percent_indexed = float(percent_value) if percent_value is not None else None
        if indexing == 0 and (percent_indexed is None or percent_indexed >= 1.0):
            return info
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"{INDEX_NAME} did not finish indexing within {timeout_sec:.0f}s "
                f"(indexing={indexing}, percent_indexed={percent_indexed})"
            )
        print(
            f"Waiting for {INDEX_NAME}: indexing={indexing} "
            f"percent_indexed={percent_indexed} num_docs={info.get('num_docs')}"
        )
        time.sleep(INDEX_POLL_INTERVAL_SEC)


def main() -> int:
    args = parse_args()
    try:
        import redis
    except ImportError:
        print(
            "error: redis-py is required; install it with 'python3 -m pip install redis'",
            file=sys.stderr,
        )
        return 2

    client = redis.Redis(
        host=str(args.shard_ip),
        port=args.port,
        username=args.username,
        password=args.password,
        ssl=args.tls,
        socket_connect_timeout=args.connect_timeout,
        socket_timeout=args.connect_timeout,
        decode_responses=True,
    )

    try:
        print(f"Checking Redis Cluster shard {args.shard_ip}:{args.port}...")
        if not client.ping():
            raise RuntimeError("shard did not respond to PING")
        if not cluster_is_healthy(client.execute_command("CLUSTER", "INFO")):
            raise RuntimeError("cluster_state is not ok")

        loaded_modules = module_names(client.module_list())
        if "rejson" not in loaded_modules:
            raise RuntimeError("RedisJSON is not loaded on the shard")
        if "search" not in loaded_modules:
            raise RuntimeError("RediSearch is not loaded on the shard")

        if len(SCHEMA_FIELDS) != 53:
            raise RuntimeError(f"idx:acs schema has {len(SCHEMA_FIELDS)} fields instead of 53")
        ft_create_idx = [
            "FT.CREATE",
            INDEX_NAME,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "A#",
            "SCHEMA",
            *(token for field in SCHEMA_FIELDS for token in field),
        ]

        print(f"Creating {INDEX_NAME}...")
        result = client.execute_command(*ft_create_idx)
        if result != "OK":
            raise RuntimeError(f"FT.CREATE returned an unexpected response: {result!r}")

        print(f"Waiting for {INDEX_NAME} to finish indexing...")
        info = wait_for_index(client, args.index_wait_timeout)
        indexing_failures = int(info.get("hash_indexing_failures", 0))
        if indexing_failures:
            raise RuntimeError(
                f"{INDEX_NAME} completed with hash_indexing_failures={indexing_failures}"
            )
        print(f"{INDEX_NAME} indexed {info.get('num_docs')} documents")
    except (redis.exceptions.RedisError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    finally:
        client.close()

    print(f"{INDEX_NAME} is ready on {args.shard_ip}:{args.port}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
