import json

from common import *
from includes import *


@skip(cluster=True, no_json=True)
def testFilteredRetryReachesMatchPastMultivalueInflatedRanges(env):
    # Numeric top-k over a multivalue field must not drop a low-valued match
    # when a selective filter forces the window to expand into low ranges.
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.vals[*]', 'AS', 'v', 'NUMERIC',
               '$.tag', 'AS', 'tag', 'TAG').ok()

    # The only doc matching @tag:{want}, at a low value.
    conn.execute_command('JSON.SET', 'doc:match', '$',
                         json.dumps({'vals': [0.5], 'tag': 'want'}))

    # Filler docs with high values, rejected by the filter. Each doc's values are
    # interleaved across the high span so it lands in every high range.
    for d in range(1, 5):
        vals = [1000 + 100 * j + d for j in range(100)]
        conn.execute_command('JSON.SET', f'doc:f{d}', '$',
                             json.dumps({'vals': vals, 'tag': 'other'}))

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{want}',
                  'SORTBY', 'v', 'DESC', 'LIMIT', '0', '1',
                  'NOCONTENT', 'DIALECT', '4')

    # Buggy result is 0 hits: expansion stopped before the low match range.
    env.assertEqual(res, [1, 'doc:match'])
