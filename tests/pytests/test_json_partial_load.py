# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
import json


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
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, [1, "doc:1", ["$", {"name": "carol"}]])


@skip(cluster=True, no_json=True)
def test_return_json_paths_across_documents(env):
    """Explicit RETURN mixes compiled paths, multi-values, and the __key sentinel."""
    env.expect(
        "FT.CREATE", "idx", "ON", "JSON", "SCHEMA",
        "$.name", "AS", "name", "TEXT",
        "$.order", "AS", "order", "NUMERIC", "SORTABLE",
    ).ok()
    conn = getConnectionByEnv(env)
    for order, name, price, tags in [
        (1, "alice", 10, ["red", "blue"]),
        (2, "bob", 20, ["green"]),
    ]:
        conn.execute_command(
            "JSON.SET", f"doc:{order}", "$",
            json.dumps({"order": order, "name": name, "price": price, "tags": tags}),
        )

    env.expect(
        "FT.SEARCH", "idx", "*", "SORTBY", "order", "ASC",
        "RETURN", "9", "name", "$.price", "AS", "price",
        "$.tags", "AS", "tags", "__key", "$.missing", "DIALECT", "3",
    ).equal([
        2,
        "doc:1", ["name", '["alice"]', "price", "[10]",
                  "tags", '[["red","blue"]]', "__key", "doc:1"],
        "doc:2", ["name", '["bob"]', "price", "[20]",
                  "tags", '[["green"]]', "__key", "doc:2"],
    ])


@skip(cluster=True, no_json=True)
def test_multiple_json_load_stages_keep_their_path_order(env):
    """Each LOAD stage has its own cache, reused for documents and cursor reads."""
    env.expect(
        "FT.CREATE", "idx", "ON", "JSON", "SCHEMA", "$.name", "AS", "name", "TEXT",
    ).ok()
    conn = getConnectionByEnv(env)
    for name, price in [("alice", 10), ("bob", 20), ("carol", 30)]:
        conn.execute_command(
            "JSON.SET", f"doc:{name}", "$", json.dumps({"name": name, "price": price}),
        )

    reply = env.cmd(
        "FT.AGGREGATE", "idx", "*",
        "LOAD", "4", "$.price", "AS", "price", "__key",
        "APPLY", "@price * 2", "AS", "double",
        "SORTBY", "2", "@price", "ASC",
        "LOAD", "4", "name", "$.missing", "AS", "optional",
        "DIALECT", "2",
        "WITHCURSOR", "COUNT", "1",
    )
    rows = []
    while True:
        batch, cursor = reply
        rows.extend(batch[1:])
        if cursor == 0:
            break
        reply = env.cmd("FT.CURSOR", "READ", "idx", cursor, "COUNT", "1")

    env.assertEqual(rows, [
        ["price", "10", "__key", "doc:alice", "double", "20", "name", "alice"],
        ["price", "20", "__key", "doc:bob", "double", "40", "name", "bob"],
        ["price", "30", "__key", "doc:carol", "double", "60", "name", "carol"],
    ])
