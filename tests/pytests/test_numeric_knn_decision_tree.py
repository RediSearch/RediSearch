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
    dim = 64
    vector_format = f"<{dim}f"
    env.expect("CONFIG", "GET", flag).equal([flag, "no"])
    env.expect("CONFIG", "SET", "search-enable-unstable-features", "yes").ok()
    env.expect(
        "FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "d:", "SCHEMA", "a", "NUMERIC",
        "b", "NUMERIC", "v", "VECTOR", "HNSW", 10, "TYPE", "FLOAT32", "DIM", dim,
        "DISTANCE_METRIC", "COSINE", "M", 8, "EF_CONSTRUCTION", 32,
    ).ok()
    rng = random.Random(20500)
    with conn.pipeline(transaction=False) as pipeline:
        for i in range(60_000):
            pipeline.hset(f"d:{i}", mapping={
                "a": i, "b": 59_999 - i,
                "v": struct.pack(vector_format, *(rng.random() for _ in range(dim))),
            })
            if i % 512 == 511:
                pipeline.execute()
        pipeline.execute()

    blob = struct.pack(vector_format, *(rng.random() for _ in range(dim)))
    cases = (
        "@a:[(200 20000]",
        "@a:[-inf (20000]",
        "@a:[(200 20000] @b:[(42000 59900]",
        "@a:[(200 10000] | @a:[(40000 59000]",
    )
    for search_mode in ("HYBRID_POLICY ADHOC_BF", "EF_RUNTIME 128",
                        "HYBRID_POLICY BATCHES EF_RUNTIME 128 BATCH_SIZE 25"):
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

    # Each range is broad and their intersection is narrow on these correlated fields.
    narrow = "(@a:[(1000 18000] @b:[(10000 43500])=>[KNN 25 @v $blob {} AS dist]"
    narrow_args = ("SORTBY", "dist", "ASC", "RETURN", 1, "dist", "LIMIT", 0, 25,
                   "PARAMS", 2, "blob", blob, "DIALECT", 4, "TIMEOUT", 10000)
    env.expect("CONFIG", "SET", flag, "no").ok()
    exact = conn.execute_command("FT.SEARCH", "idx", narrow.format("HYBRID_POLICY ADHOC_BF"),
                                 *narrow_args)
    env.expect("CONFIG", "SET", flag, "yes").ok()
    actual, profile = conn.execute_command("FT.PROFILE", "idx", "SEARCH", "QUERY",
                                           narrow.format("EF_RUNTIME 128"), *narrow_args)
    env.assertEqual(actual, exact)
    env.assertContains("DENSE-NUMERIC-FILTER", repr(profile))
    env.assertContains("HYBRID_ADHOC_BF", repr(profile))

    batches = narrow.format("HYBRID_POLICY BATCHES EF_RUNTIME 128 BATCH_SIZE 25")
    env.expect("CONFIG", "SET", flag, "no").ok()
    expected = conn.execute_command("FT.SEARCH", "idx", batches, *narrow_args)
    env.expect("CONFIG", "SET", flag, "yes").ok()
    actual, profile = conn.execute_command("FT.PROFILE", "idx", "SEARCH", "QUERY", batches,
                                           *narrow_args)
    env.assertEqual(actual, expected)
    env.assertContains("HYBRID_BATCHES", repr(profile))

    # A hole in the doc-ID domain makes the dense path ineligible.
    conn.delete("d:111")
    _, profile = conn.execute_command("FT.PROFILE", "idx", "SEARCH", "QUERY", *args)
    env.assertFalse("DENSE-NUMERIC-FILTER" in repr(profile))
