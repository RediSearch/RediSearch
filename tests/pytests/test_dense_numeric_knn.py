# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1), version 1; or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import random
import struct

from RLTest import Env


def test_dense_numeric_knn():
    """Dense numeric checks preserve ranked replies, including after a cursor yields."""
    env = Env(env="oss", decodeResponses=True, moduleArgs="WORKERS 0 MIN_OPERATION_WORKERS 0")
    conn = env.getConnection()
    flag = "search-_dense-numeric-knn"
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
        "@a:[(43000 59900]",
        "@a:[(200 20000] @b:[(42000 59900]",
        "@a:[(200 10000] | @a:[(40000 59000]",
    )
    for search_mode in ("HYBRID_POLICY ADHOC_BF", "HYBRID_POLICY BATCHES BATCH_SIZE 64", "EF_RUNTIME 128"):
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

    env.expect("CONFIG", "SET", "search-enable-unstable-features", "no").ok()
    actual, profile = conn.execute_command("FT.PROFILE", "idx", "SEARCH", "QUERY", *args)
    env.assertEqual(actual, expected)
    env.assertFalse("DENSE-NUMERIC-FILTER" in repr(profile))
    env.expect("CONFIG", "GET", flag).equal([flag, "yes"])
    env.expect("CONFIG", "SET", "search-enable-unstable-features", "yes").ok()

    first_pages = []
    cursor_query = query.replace("EF_RUNTIME 128", "HYBRID_POLICY ADHOC_BF")
    for enabled in ("no", "yes"):
        env.expect("CONFIG", "SET", flag, enabled).ok()
        first_pages.append(conn.execute_command(
            "FT.AGGREGATE", "idx", cursor_query, "LOAD", 1, "@__key", "LIMIT", 0, 25,
            "WITHCURSOR", "COUNT", 1, "PARAMS", 2, "blob", blob, "DIALECT", 4,
        ))

    def revision():
        fields = conn.execute_command("_FT.DEBUG", "NUMIDX_SUMMARY", "idx", "a")
        return dict(zip(fields[::2], fields[1::2]))["revisionId"]

    # Split a numeric leaf between cursor pages without adding any vectors.
    before = revision()
    with conn.pipeline(transaction=False) as pipeline:
        for i in range(10_000):
            pipeline.hset(f"d:new:{i}", "a", 21_000 + i / 10_000)
        pipeline.execute()
    env.assertNotEqual(revision(), before)

    # Replacing an indexed value assigns a fresh doc ID and leaves a hole.
    conn.hset("d:500", "a", 20_500)
    conn.delete("d:111")
    cursor_replies = []
    for page, cursor in first_pages:
        env.assertNotEqual(cursor, 0)
        rows = page[1:]
        while cursor:
            page, cursor = conn.execute_command("FT.CURSOR", "READ", "idx", cursor, "COUNT", 3)
            rows.extend(page[1:])
        env.assertEqual(len(rows), 25)
        cursor_replies.append(rows)
    env.assertEqual(*cursor_replies)

    actual, profile = conn.execute_command("FT.PROFILE", "idx", "SEARCH", "QUERY", *args)
    env.assertFalse("DENSE-NUMERIC-FILTER" in repr(profile))
    env.expect("CONFIG", "SET", flag, "no").ok()
    env.assertEqual(actual, conn.execute_command("FT.SEARCH", "idx", *args))
    env.expect("CONFIG", "SET", "search-enable-unstable-features", "no").ok()
