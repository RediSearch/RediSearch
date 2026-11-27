# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import struct

from RLTest import Env
from common import skip, to_dict, waitForIndex


@skip(cluster=True)
def test_hnsw_hybrid_policy_selects_initial_mode_and_preserves_query_override():
    env = Env(moduleArgs="WORKERS 0")
    conn = env.getConnection()
    config = "search-_hnsw-hybrid-policy"
    env.expect("CONFIG", "GET", config).equal([config, "legacy"])
    env.expect("FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "doc:", "SCHEMA",
               "tag", "TAG", "vector", "VECTOR", "HNSW", 6,
               "TYPE", "FLOAT32", "DIM", 2, "DISTANCE_METRIC", "L2").ok()
    with conn.pipeline(transaction=False) as pipe:
        for i in range(200):
            pipe.hset(f"doc:{i}", mapping={
                "tag": "yes" if i < 150 else "no",
                "vector": struct.pack("<ff", i, 0),
            })
        pipe.execute()
    waitForIndex(env, "idx")

    def query(policy=None):
        options = f" HYBRID_POLICY {policy}" if policy else " EF_RUNTIME 200"
        expression = f"(@tag:{{yes}})=>[KNN 70 @vector $v{options} AS dist]"
        results, profile = conn.execute_command(
            "FT.PROFILE", "idx", "SEARCH", "QUERY", expression,
            "PARAMS", 2, "v", struct.pack("<ff", 0, 0),
            "SORTBY", "dist", "ASC", "LIMIT", 0, 70, "NOCONTENT", "DIALECT", 2,
        )
        vector = to_dict(to_dict(to_dict(profile)["Shards"][0])["Iterators profile"])
        return results, vector["Vector search mode"]

    expected = [70] + [f"doc:{i}" for i in range(70)]
    env.assertEqual(query(), (expected, "HYBRID_ADHOC_BF"))
    env.expect("CONFIG", "SET", config, "invalid").error()
    env.expect("CONFIG", "SET", config, "trained").ok()
    env.expect("CONFIG", "GET", config).equal([config, "trained"])
    env.assertEqual(query(), (expected, "HYBRID_BATCHES"))
    env.assertEqual(query("ADHOC_BF"), (expected, "HYBRID_ADHOC_BF"))
    env.expect("CONFIG", "SET", config, "latency").ok()
    env.expect("CONFIG", "GET", config).equal([config, "latency"])
    env.assertEqual(query(), (expected, "HYBRID_ADHOC_BF"))
    env.assertEqual(query("ADHOC_BF"), (expected, "HYBRID_ADHOC_BF"))
    env.expect("CONFIG", "SET", config, "legacy").ok()
    env.assertEqual(query(), (expected, "HYBRID_ADHOC_BF"))
