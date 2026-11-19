# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Setup and comparison helpers for test_multi_value_aggregation.py, which keeps
only the queries and the results they must return."""

import json
import shlex
from common import getConnectionByEnv


def index_docs(env, *docs, sortable=False):
    """A fresh JSON index `idx` over `docs`, each stored as doc:<name>.

    `name` is single-valued; `tags` (TAG) and `nums` (NUMERIC) are multi-value.
    """
    env.flush()
    multi_value_options = ['SORTABLE'] if sortable else []
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TAG',
               '$.tags[*]', 'AS', 'tags', 'TAG', *multi_value_options,
               '$.nums[*]', 'AS', 'nums', 'NUMERIC', *multi_value_options).ok()
    conn = getConnectionByEnv(env)
    for doc in docs:
        conn.execute_command('JSON.SET', f'doc:{doc["name"]}', '$', json.dumps(doc))


def aggregate(env, args, query='*', dialect=2):
    """The rows of `FT.AGGREGATE idx <query> <args>`, as dicts in a canonical order."""
    reply = env.cmd('FT.AGGREGATE', 'idx', query, *shlex.split(args), 'DIALECT', dialect)
    return _canonical(reply[1:])


def expect_rows(env, args, rows, query='*', dialects=(1, 2, 3)):
    """Asserts that `FT.AGGREGATE idx <query> <args>` returns `rows`, in any order
    and in each of `dialects`."""
    for dialect in dialects:
        env.assertEqual(aggregate(env, args, query, dialect), _canonical_dicts(rows),
                        message=f'DIALECT {dialect}', depth=1)


def _canonical(reply_rows):
    return _canonical_dicts([dict(zip(row[::2], row[1::2])) for row in reply_rows])


def _canonical_dicts(rows):
    # GROUPBY returns its groups in no defined order.
    return sorted(rows, key=lambda row: repr(sorted(row.items(), key=repr)))
