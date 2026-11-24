# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1), version 1; or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import random
import struct

from RLTest import Env


def test_numeric_knn_decision_tree():
    env = Env(env="oss", decodeResponses=True, moduleArgs="WORKERS 0 MIN_OPERATION_WORKERS 0")
    conn = env.getConnection()
    flag = "search-_numeric-knn-decision-tree"
    env.expect("CONFIG", "GET", flag).equal([flag, "no"])
    env.expect("CONFIG", "SET", "search-enable-unstable-features", "yes").ok()
    env.expect(
        "FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "d:", "SCHEMA", "a", "NUMERIC",
        "b", "NUMERIC", "v", "VECTOR", "HNSW", 10, "TYPE", "FLOAT32", "DIM", 4,
        "DISTANCE_METRIC", "COSINE", "M", 8, "EF_CONSTRUCTION", 32,
    ).ok()
    rng = random.Random(20500)
    with conn.pipeline(transaction=False) as pipeline:
        for i in range(60_000):
            pipeline.hset(f"d:{i}", mapping={
                "a": i, "b": 59_999 - i,
                "v": struct.pack("<4f", *(rng.random() for _ in range(4))),
            })
            if i % 512 == 511:
                pipeline.execute()
        pipeline.execute()

    blob = struct.pack("<4f", .2, .3, .5, .7)
    cases = (
        "@a:[(200 20000]",
        "@a:[(200 20000] @b:[(42000 59900]",
        "@a:[(200 10000] | @a:[(40000 59000]",
    )
    for search_mode in ("HYBRID_POLICY ADHOC_BF", "EF_RUNTIME 128"):
        for shape in cases:
            query = f"({shape})=>[KNN 25 @v $blob {search_mode} AS dist]"
            args = (query, "SORTBY", "dist", "ASC", "RETURN", 1, "dist", "LIMIT", 0, 25,
                    "PARAMS", 2, "blob", blob, "DIALECT", 4, "TIMEOUT", 10000)
            env.expect("CONFIG", "SET", flag, "no").ok()
            expected = conn.execute_command("FT.SEARCH", "idx", *args)
            env.expect("CONFIG", "SET", flag, "yes").ok()
            actual, profile = conn.execute_command("FT.PROFILE", "idx", "SEARCH", "QUERY", *args)
            env.assertEqual(actual, expected)
            env.assertContains("DENSE-NUMERIC-FILTER", repr(profile))

    # A hole in the doc-ID domain makes the dense path ineligible.
    conn.delete("d:111")
    _, profile = conn.execute_command("FT.PROFILE", "idx", "SEARCH", "QUERY", *args)
    env.assertFalse("DENSE-NUMERIC-FILTER" in repr(profile))
