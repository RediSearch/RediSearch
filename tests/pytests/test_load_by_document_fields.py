# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).


# `search-load-by-document-fields` loads a Hash by walking its own fields instead of probing
# every schema field. Whatever the setting, the index must end up identical, so every scenario runs
# once per setting and the observable results are compared.

from common import *

FLAG = 'search-load-by-document-fields'
# Wider than the schema size under which the flag has no effect.
N_FILLER = 96


def _set_flag(env, on):
    verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', FLAG, 'yes' if on else 'no')


def _search_ids(env, *args):
    res = env.cmd('FT.SEARCH', 'idx', *args, 'NOCONTENT', 'LIMIT', 0, 100)
    return [res[0]] + sorted(res[1:])


def _rows(res):
    return sorted(sorted(zip(r[::2], r[1::2])) for r in res[1:])


def _dump_hash(env):
    out = {}
    out['alias_tag'] = _search_ids(env, '@vv:{hello}')
    out['alias_text'] = _search_ids(env, '@txt:hello')
    out['missing'] = _search_ids(env, 'ismissing(@mm)', 'DIALECT', 2)
    out['empty'] = _search_ids(env, '@ee:{""}', 'DIALECT', 2)
    out['numeric'] = _search_ids(env, '@n3:[3 7]')
    out['sorted'] = env.cmd('FT.SEARCH', 'idx', '@n3:[0 100]', 'SORTBY', 'n3', 'DESC', 'NOCONTENT')
    out['all'] = _search_ids(env, '*')
    out['agg'] = _rows(env.cmd('FT.AGGREGATE', 'idx', '@n3:[0 100]', 'LOAD', '3', '@n3', '@vv', '@ee'))
    info = env.cmd('FT.INFO', 'idx')
    info = dict(zip(info[::2], info[1::2]))
    out['num_docs'] = info['num_docs']
    out['num_records'] = info['num_records']
    out['failures'] = info['hash_indexing_failures']
    return out


def _create_hash_index(env):
    schema = ['v', 'AS', 'vv', 'TAG',
              'v', 'AS', 'txt', 'TEXT',
              'm', 'AS', 'mm', 'TAG', 'INDEXMISSING',
              'e', 'AS', 'ee', 'TAG', 'INDEXEMPTY']
    for i in range(N_FILLER):
        schema += [f'n{i}', 'NUMERIC', 'SORTABLE']
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc:', 'SCHEMA', *schema).ok()


def _hash_scenario(env, flag_per_phase):
    """Runs the scenario, switching the flag to flag_per_phase[i] before phase i, and returns the
    results of every phase."""
    conn = getConnectionByEnv(env)
    env.flush()
    _create_hash_index(env)
    dumps = []

    _set_flag(env, flag_per_phase[0])
    conn.execute_command('HSET', 'doc:1', 'v', 'hello', 'n3', '3', 'n50', '50', 'e', '')
    conn.execute_command('HSET', 'doc:2', 'm', 'x', 'n3', '7', 'v', 'world')
    # No schema field at all: it is still a document of the index.
    conn.execute_command('HSET', 'doc:3', 'zzz', '1')
    conn.execute_command('HSET', 'doc:4', 'n95', '95', 'e', 'abc')
    conn.execute_command('HSET', 'doc:5', 'v', 'hello', 'n3', '3')
    # A field of a schema is only indexed under its exact name.
    conn.execute_command('HSET', 'doc:6', 'N3', '9', 'vv', 'hello', 'n10', '1', 'n11', '2')
    waitForIndex(env, 'idx')
    dumps.append(_dump_hash(env))

    _set_flag(env, flag_per_phase[1])
    # Non-schema field, an aliased path, a schema field and a deletion.
    conn.execute_command('HSET', 'doc:1', 'zzz', '5')
    waitForIndex(env, 'idx')
    dumps.append(_dump_hash(env))
    conn.execute_command('HSET', 'doc:1', 'v', 'bye')
    conn.execute_command('HDEL', 'doc:5', 'n3')
    conn.execute_command('HSET', 'doc:4', 'n95', '96', 'n94', '94')
    waitForIndex(env, 'idx')
    dumps.append(_dump_hash(env))

    _set_flag(env, flag_per_phase[2])
    # Field-level expiration: the expired field must not be indexed.
    conn.execute_command('HPEXPIRE', 'doc:6', 100, 'FIELDS', 1, 'n11')
    time.sleep(0.3)
    conn.execute_command('HSET', 'doc:6', 'n12', '3')
    waitForIndex(env, 'idx')
    dumps.append(_dump_hash(env))
    dumps.append(_search_ids(env, '@n11:[2 2]'))
    dumps.append(_search_ids(env, '@n10:[1 1]'))
    return dumps


@skip(redis_less_than='7.4.0')
def testHashSameResultsWithAndWithoutFlag(env):
    off = _hash_scenario(env, [False, False, False])
    on = _hash_scenario(env, [True, True, True])
    env.assertEqual(on, off)

    # Spot-check the results themselves, so that two equally wrong runs do not pass.
    first = on[0]
    env.assertEqual(first['alias_tag'], [2, 'doc:1', 'doc:5'])
    env.assertEqual(first['alias_text'], [2, 'doc:1', 'doc:5'])
    env.assertEqual(first['missing'], [5, 'doc:1', 'doc:3', 'doc:4', 'doc:5', 'doc:6'])
    env.assertEqual(first['numeric'], [3, 'doc:1', 'doc:2', 'doc:5'])
    env.assertEqual(first['all'], [6, 'doc:1', 'doc:2', 'doc:3', 'doc:4', 'doc:5', 'doc:6'])
    # doc:6 has the wrongly-cased N3 and vv, an alias rather than the path v: neither is indexed.
    env.assertEqual(first['empty'], [1, 'doc:1'])
    env.assertEqual(on[-2], [0])
    env.assertEqual(on[-1], [1, 'doc:6'])
    env.assertEqual(on[2]['alias_tag'], [1, 'doc:5'])


@skip(redis_less_than='7.4.0')
def testHashFlagSwitchedAtRuntime(env):
    off = _hash_scenario(env, [False, False, False])
    env.assertEqual(_hash_scenario(env, [True, False, True]), off)
    env.assertEqual(_hash_scenario(env, [False, True, False]), off)


@skip(redis_less_than='7.4.0')
def testHashAlterWhileFlagOn(env):
    """The path table follows FT.ALTER: a field added later is loaded by the next document."""
    conn = getConnectionByEnv(env)
    _set_flag(env, True)
    _create_hash_index(env)
    conn.execute_command('HSET', 'doc:1', 'late', 'abc', 'n1', '1')
    waitForIndex(env, 'idx')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'late', 'TAG').ok()
    waitForIndex(env, 'idx')
    env.assertEqual(_search_ids(env, '@late:{abc}'), [1, 'doc:1'])
    conn.execute_command('HSET', 'doc:2', 'late', 'abc')
    waitForIndex(env, 'idx')
    env.assertEqual(_search_ids(env, '@late:{abc}'), [2, 'doc:1', 'doc:2'])
    # A rejected ALTER leaves the table matching the unchanged schema.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'bad1', 'TAG', 'late', 'TAG').error()
    conn.execute_command('HSET', 'doc:3', 'bad1', 'zzz', 'late', 'abc')
    waitForIndex(env, 'idx')
    env.assertEqual(_search_ids(env, '@late:{abc}'), [3, 'doc:1', 'doc:2', 'doc:3'])
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'bad1', 'TAG').ok()
    conn.execute_command('HSET', 'doc:4', 'bad1', 'zzz')
    waitForIndex(env, 'idx')
    env.assertEqual(_search_ids(env, '@bad1:{zzz}'), [2, 'doc:3', 'doc:4'])


def _hfe_scenario(env, flag):
    """Hash field expiration while a document is loaded: by the scan of FT.ALTER, which indexes
    every document that is there, and by a plain HSET."""
    conn = getConnectionByEnv(env)
    env.flush()
    _set_flag(env, flag)
    _create_hash_index(env)
    conn.execute_command('HSET', 'doc:1', 'n1', '1', 'n2', '2', 'n3', '3', 'v', 'hello')
    conn.execute_command('HSET', 'doc:2', 'n1', '10', 'n2', '20', 'n3', '30')
    conn.execute_command('HSET', 'doc:3', 'n4', '4')
    conn.execute_command('HPEXPIRE', 'doc:1', 100, 'FIELDS', 1, 'n2')
    conn.execute_command('HPEXPIRE', 'doc:2', 100, 'FIELDS', 2, 'n1', 'n3')
    # A field that expires later than the load must still be indexed, and its expiration kept.
    conn.execute_command('HPEXPIRE', 'doc:3', 100000, 'FIELDS', 1, 'n4')
    time.sleep(0.3)
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'late', 'NUMERIC', 'SORTABLE').ok()
    waitForIndexFinishScan(env)
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'doc:4', 'n1', '100', 'late', '5')
    conn.execute_command('HPEXPIRE', 'doc:4', 100, 'FIELDS', 1, 'n1')
    time.sleep(0.3)
    conn.execute_command('HSET', 'doc:4', 'n5', '55')
    waitForIndex(env, 'idx')
    dumps = [_search_ids(env, f'@n{i}:[-inf +inf]') for i in range(1, 6)]
    dumps.append(_search_ids(env, '@late:[0 10]'))
    dumps.append(_search_ids(env, '@vv:{hello}'))
    dumps.append(_search_ids(env, '*'))
    return dumps


@skip(redis_less_than='7.4.0')
def testHashFieldExpirationSameResultsWithAndWithoutFlag(env):
    off = _hfe_scenario(env, False)
    on = _hfe_scenario(env, True)
    env.assertEqual(on, off)
    # n2 of doc:1, n1 and n3 of doc:2 and n1 of doc:4 are gone; the rest is indexed.
    env.assertEqual(on[0], [1, 'doc:1'])
    env.assertEqual(on[1], [1, 'doc:2'])
    env.assertEqual(on[2], [1, 'doc:1'])
    env.assertEqual(on[3], [1, 'doc:3'])
    env.assertEqual(on[4], [1, 'doc:4'])
    env.assertEqual(on[5], [1, 'doc:4'])
