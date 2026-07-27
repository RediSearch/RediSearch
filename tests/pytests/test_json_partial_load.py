# -*- coding: utf-8 -*-
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *


@skip(no_json=True)
def test_per_field_load_keeps_document_when_jsonpath_misses(env):
    """Per-field load: a missing JSONPath match for one returned field must
    not drop the entire document from the result set."""
    env.expect(
        "FT.CREATE",
        "idx",
        "ON",
        "JSON",
        "SCHEMA",
        "$.name",
        "AS",
        "name",
        "TEXT",
        "$.optional",
        "AS",
        "optional",
        "TEXT",
    ).ok()

    # `$.optional` does not exist on this document.
    env.cmd("JSON.SET", "doc:1", "$", '{"name": "alice"}')
    waitForIndex(env, "idx")

    res = env.cmd("FT.SEARCH", "idx", "@name:alice", "RETURN", "2", "name", "optional")

    # Document still appears in results.
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], "doc:1")

    # Present field is loaded; absent field either does not appear in the
    # returned fields list, or appears as an empty string. Either is fine —
    # the contract is that the document was not dropped.
    fields = res[2]
    fields_dict = dict(zip(fields[::2], fields[1::2]))
    env.assertEqual(fields_dict.get("name"), "alice")
    env.assertIn(fields_dict.get("optional"), (None, ""))


@skip(no_json=True)
def test_per_field_load_keeps_document_when_array_first_match_is_empty(env):
    """Per-field load: a JSONPath that matches an empty array (so the
    "first element" case yields no value) must not drop the document."""
    env.expect(
        "FT.CREATE",
        "idx",
        "ON",
        "JSON",
        "SCHEMA",
        "$.name",
        "AS",
        "name",
        "TEXT",
        "$.tags",
        "AS",
        "tags",
        "TAG",
    ).ok()

    # `$.tags` matches the empty array; the loader resolves to "no first element".
    env.cmd("JSON.SET", "doc:1", "$", '{"name": "bob", "tags": []}')
    waitForIndex(env, "idx")

    res = env.cmd("FT.SEARCH", "idx", "@name:bob", "RETURN", "2", "name", "tags")

    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], "doc:1")

    fields = res[2]
    fields_dict = dict(zip(fields[::2], fields[1::2]))
    env.assertEqual(fields_dict.get("name"), "bob")


@skip(no_json=True)
def test_doc_level_load_returns_root_for_matching_document(env):
    """Doc-level load: when no `RETURN` clause is given, the JSON root (`$`)
    is loaded and the document appears in the result."""
    env.expect(
        "FT.CREATE", "idx", "ON", "JSON", "SCHEMA", "$.name", "AS", "name", "TEXT"
    ).ok()

    env.cmd("JSON.SET", "doc:1", "$", '{"name": "carol"}')
    waitForIndex(env, "idx")

    res = env.cmd("FT.SEARCH", "idx", "@name:carol")
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], "doc:1")
    env.assertGreater(len(res[2]), 0)


@skip(no_json=True)
def test_per_field_load_resolves_each_document_across_many(env):
    """The returned field's JSONPath is compiled once and reused for every
    document in the result — each document must still resolve to its own value,
    not a stale or shared one."""
    env.expect(
        "FT.CREATE",
        "idx",
        "ON",
        "JSON",
        "SCHEMA",
        "$.name",
        "AS",
        "name",
        "TEXT",
        "$.city",
        "AS",
        "city",
        "TEXT",
    ).ok()

    n = 50
    for i in range(n):
        env.cmd("JSON.SET", "doc:%d" % i, "$", '{"name": "n%d", "city": "city%d"}' % (i, i))
    waitForIndex(env, "idx")

    res = env.cmd("FT.SEARCH", "idx", "*", "RETURN", "1", "city", "LIMIT", "0", str(n))

    env.assertEqual(res[0], n)
    # res: [count, key1, fields1, key2, fields2, ...]. Every document must map to
    # its own `city`, confirming the reused compiled path is evaluated per-doc.
    seen = 0
    for k in range(1, len(res), 2):
        key, fields = res[k], res[k + 1]
        fields_dict = dict(zip(fields[::2], fields[1::2]))
        i = int(key.split(":")[1])
        env.assertEqual(fields_dict.get("city"), "city%d" % i)
        seen += 1
    env.assertEqual(seen, n)
