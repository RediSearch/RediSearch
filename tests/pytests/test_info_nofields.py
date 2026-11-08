# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).


# With search-info-nofields (a load-time setting) FT.INFO <index> NOFIELDS leaves out the two sections
# that grow with the schema, `attributes` and `field statistics`, on the shard and on the
# coordinator. Everything else is unchanged. Without it FT.INFO is exactly as it was: one argument,
# the same COMMAND INFO and COMMAND DOCS, and the same arity error for anything else.

from common import *

PER_FIELD_SECTIONS = {'attributes', 'field statistics'}
# Values that move between two calls on a live server.
VOLATILE = {'total_indexing_time', 'number_of_admin_ops', 'percent_indexed', 'cleaning', 'gc_stats', 'cursor_stats',
            'indexing'}


CONFIG_ON = redis_config_file('search-info-nofields yes')


def _env_on(**kwargs):
    return Env(redisConfigFile=CONFIG_ON, **kwargs)


def _command_info(env):
    """COMMAND INFO and COMMAND DOCS of FT.INFO, read over RESP3, which both answer as maps."""
    # One string: the client parses the reply by the command it sees.
    conn = env.getConnection()
    [info] = conn.execute_command('COMMAND INFO FT.INFO')
    [docs] = conn.execute_command('COMMAND DOCS FT.INFO').values()
    return info[1], [a['name'] for a in docs['arguments']]


def _internal_connection(env):
    """A connection to a shard that may call the internal _FT.INFO."""
    conn = env.getConnection()  # one shard, not the cluster client
    conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    return conn


def _error(conn, *args):
    try:
        conn.execute_command(*args)
    except Exception as e:
        return str(e)
    return None


def _info(env, *args):
    res = env.cmd('FT.INFO', 'idx', *args)
    return dict(zip(res[::2], res[1::2]))


def _create(env, n_filler=40):
    schema = ['title', 'TEXT', 'tag', 'TAG', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
              'DIM', '2', 'DISTANCE_METRIC', 'L2', 'geom', 'GEOSHAPE', 'FLAT']
    for i in range(n_filler):
        schema += [f'n{i}', 'NUMERIC', 'SORTABLE']
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc:', 'SCHEMA', *schema).ok()
    conn = getConnectionByEnv(env)
    for i in range(20):
        conn.execute_command('HSET', f'doc:{i}', 'title', f'word{i}', 'tag', f't{i % 3}',
                             'n1', i, 'vec', b'\x00\x00\x80\x3f\x00\x00\x00\x00',
                             'geom', f'POINT({i} {i})')
    waitForIndex(env, 'idx')


def testNoFieldsOmitsOnlyThePerFieldSections():
    env = _env_on()
    _create(env)
    full = _info(env)
    env.assertContains('attributes', full)
    env.assertContains('field statistics', full)
    env.assertEqual(len(full['attributes']), 44)

    for arg in ('NOFIELDS', 'nofields', 'NoFields'):
        slim = _info(env, arg)
        env.assertEqual(set(full) - set(slim), PER_FIELD_SECTIONS, message=arg)
        env.assertEqual(set(slim) - set(full), set(), message=arg)
        for key in slim:
            if key in VOLATILE:
                continue
            env.assertEqual(slim[key], full[key], message=f'{arg}: {key}')
    # The geometry and vector totals are summed over the fields, which are not walked here.
    env.assertGreater(float(full['geoshapes_sz_mb']), 0)
    env.assertGreater(float(full['vector_index_sz_mb']), 0)


def testNoFieldsResp3():
    env = _env_on(protocol=3)
    _create(env)
    full = env.cmd('FT.INFO', 'idx')
    slim = env.cmd('FT.INFO', 'idx', 'NOFIELDS')
    env.assertEqual(set(full) - set(slim), PER_FIELD_SECTIONS)
    env.assertEqual(slim['num_docs'], full['num_docs'])
    env.assertEqual(slim['num_records'], full['num_records'])


def testNoFieldsOnAnIndexWithoutFields():
    env = _env_on()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
    slim = _info(env, 'NOFIELDS')
    env.assertEqual(slim['index_name'], 'idx')
    env.assertNotContains('attributes', slim)
    env.assertNotContains('field statistics', slim)


def testNoFieldsAtTheSchemaLimit():
    env = _env_on()
    schema = [x for i in range(SPEC_MAX_FIELDS) for x in (f'f{i}', 'NUMERIC', 'SORTABLE')]
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *schema).ok()
    slim = _info(env, 'NOFIELDS')
    env.assertEqual(slim['index_name'], 'idx')
    env.assertNotContains('attributes', slim)
    env.assertNotContains('field statistics', slim)
    full = _info(env)
    env.assertEqual(len(full['attributes']), SPEC_MAX_FIELDS)
    env.assertEqual(len(full['field statistics']), SPEC_MAX_FIELDS)


def testNoFieldsArgumentErrors():
    env = _env_on(enableDebugCommand=True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.INFO', 'nosuchindex', 'NOFIELDS').error().contains('Index not found')
    env.expect('FT.INFO').error().contains('wrong number of arguments')
    # The same error from the coordinator and, called directly, from a shard.
    env.expect('FT.INFO', 'idx', 'BOGUS').error().contains('Unknown FT.INFO argument')
    env.expect('FT.INFO', 'idx', 'NOFIELDS', 'NOFIELDS').error().contains('wrong number of arguments')
    shard = _internal_connection(env)
    env.assertContains('Unknown FT.INFO argument', _error(shard, '_FT.INFO', 'idx', 'BOGUS'))
    env.assertContains('Unknown FT.INFO argument', _error(shard, '_FT.INFO', 'idx', 'NOFIELDS', 'BOGUS'))
    env.assertContains('Unknown FT.INFO argument', _error(shard, '_FT.INFO', 'nosuchindex', 'BOGUS'))
    res = shard.execute_command('_FT.INFO', 'idx', 'NOFIELDS', '_WITH_INDEX_ERROR_TIME')
    env.assertNotContains('attributes', res[::2])
    res = shard.execute_command('_FT.INFO', 'idx', '_WITH_INDEX_ERROR_TIME')
    env.assertContains('attributes', res[::2])


def testCommandInfoWithTheOption():
    env = _env_on(protocol=3)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-info-nofields'), {'search-info-nofields': 'yes'})
    env.assertEqual(_command_info(env), (-2, ['index', 'nofields']))


def testCommandInfoWithoutTheOptionIsUnchanged():
    env = Env(protocol=3)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-info-nofields'), {'search-info-nofields': 'no'})
    env.assertEqual(_command_info(env), (2, ['index']))
    # Load-time only: the arity is fixed when the command is registered.
    env.expect('CONFIG', 'SET', 'search-info-nofields', 'yes').error()


def testWithoutTheOptionInfoTakesExactlyOneArgument():
    env = Env(enableDebugCommand=True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
    full = _info(env)
    env.assertContains('attributes', full)
    for args in (('NOFIELDS',), ('nofields',), ('BOGUS',), ('NOFIELDS', 'NOFIELDS'), ('a', 'b')):
        env.expect('FT.INFO', 'idx', *args).error().contains("wrong number of arguments for 'FT.INFO' command")
    env.expect('FT.INFO').error().contains("wrong number of arguments for 'FT.INFO' command")
    # A shard reads its arguments as it always did: the first one only means something if it is
    # the coordinator's, and everything else is ignored.
    shard = _internal_connection(env)
    for arg in ('NOFIELDS', 'BOGUS'):
        env.assertContains('attributes', shard.execute_command('_FT.INFO', 'idx', arg)[::2])


def testPlainInfoIsUnchangedByTheOption():
    env = _env_on()
    """The plain command keeps both sections, in the same order, before and after a NOFIELDS call."""
    _create(env, n_filler=5)
    before = env.cmd('FT.INFO', 'idx')[::2]
    _info(env, 'NOFIELDS')
    env.assertEqual(env.cmd('FT.INFO', 'idx')[::2], before)
    env.assertEqual(before[:3], ['index_name', 'index_options', 'index_definition'])
    env.assertEqual(before[3], 'attributes')
    env.assertEqual(before[-1], 'field statistics')
