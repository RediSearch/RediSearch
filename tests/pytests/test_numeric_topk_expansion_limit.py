import json

from common import *
from includes import *


@skip(cluster=True, no_json=True)
def testFilteredRetryReachesMatchPastMultivalueInflatedRanges(env):
    # Regression guard for the numeric top-k expand-and-retry window limit.
    #
    # A multivalue numeric field indexes one entry per array element, so a single
    # document is counted once per value range. The window expansion must not cap
    # its retry limit at the unique document count, or a selective filter whose
    # only match sits in a low-scored range past the inflated high-range
    # cumulative gets dropped, returning fewer results than exist.
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.vals[*]', 'AS', 'v', 'NUMERIC',
               '$.tag', 'AS', 'tag', 'TAG').ok()

    # Match doc: a single low value, carrying the filter tag. It is the only doc
    # passing @tag:{want}, so top-1 by DESC must return it regardless of sort.
    conn.execute_command('JSON.SET', 'doc:match', '$',
                         json.dumps({'vals': [0.5], 'tag': 'want'}))

    # Filler docs: 100 high values each, interleaved across the high span so each
    # doc lands in every high range, splitting the tree into several high leaves
    # that all hold all four fillers. A different tag makes the filter reject them.
    for d in range(1, 5):
        vals = [1000 + 100 * j + d for j in range(100)]
        conn.execute_command('JSON.SET', f'doc:f{d}', '$',
                             json.dumps({'vals': vals, 'tag': 'other'}))

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{want}',
                  'SORTBY', 'v', 'DESC', 'LIMIT', '0', '1',
                  'NOCONTENT', 'DIALECT', '4')

    # Correct: exactly the match doc. Buggy: 0 results, because the optimizer
    # stopped expanding before reaching the low range holding doc:match.
    env.assertEqual(res, [1, 'doc:match'])
