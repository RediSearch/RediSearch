# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

# The widest GROUPBY the argument parser accepts (MAX_GROUPBY_PROPERTIES).
WIDEST_GROUPBY = 2**16 - 1


@skip(cluster=True)
def test_widest_groupby_is_rejected_while_building_pipeline():
    env = Env(moduleArgs='WORKERS 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'f', 'TAG').ok()
    env.cmd('HSET', 'doc1', 'f', 'a')
    env.cmd('HSET', 'doc2', 'f', 'b')
    waitForIndex(env, 'idx')

    # Repeating one property passes argument parsing; the repetition is only
    # rejected while the grouper is built, after its per-property storage exists.
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', WIDEST_GROUPBY, *(['@f'] * WIDEST_GROUPBY),
               'REDUCE', 'COUNT', 0, 'AS', 'n').error().contains('specified more than once')

    # The server keeps serving grouping requests.
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 1, '@f',
                  'REDUCE', 'COUNT', 0, 'AS', 'n', 'SORTBY', 2, '@f', 'ASC')
    env.assertEqual(res[1:], [['f', 'a', 'n', '1'], ['f', 'b', 'n', '1']])
