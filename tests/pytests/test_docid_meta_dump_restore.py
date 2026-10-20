# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""DUMP/RESTORE and the per-key DocIdMeta ("D-ID") key metadata.

Redis includes module key metadata in DUMP (and MIGRATE / atomic slot migration)
payloads, serialized through the class's rdb_save callback, and RESTORE feeds it to
rdb_load before the key is created. DocIdMeta registers those callbacks only in disk
mode, which `_SIMULATE_IN_FLEX` reproduces on top of the in-memory engine: a restored
key then arrives carrying the source key's {specId -> docId} entries.

Every test asserts the behavior a user expects; tests that fail on the current code
document a hazard of transferring docIds.
"""

import os

from common import *


DISK_META_ENV_VAR = 'RS_TRACE_DOCIDMETA_RDB'


def flex_env():
    # _SIMULATE_IN_FLEX is applied after DocIdMeta_Init, so it never registers the RDB
    # callbacks; the (temporary, research-only) env var forces the disk-mode
    # registration. It is inherited by every redis-server RLTest spawns from here on.
    os.environ[DISK_META_ENV_VAR] = '1'
    return Env(moduleArgs='_SIMULATE_IN_FLEX true')


def create_index(env, prefix='doc:'):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'PREFIX', 1, prefix,
               'SCHEMA', 't', 'TEXT').ok()


def search(env, term='hello'):
    res = env.cmd('FT.SEARCH', 'idx', term, 'NOCONTENT')
    return [res[0]] + sorted(res[1:])


def dump(env, key):
    return env.cmd('DUMP', key, **{NEVER_DECODE: []})


@skip(cluster=True)
def test_memory_mode_restore_under_new_name():
    """Memory mode registers no rdb_save, so the payload carries no docId and the
    restored key is indexed under a fresh id."""
    os.environ.pop(DISK_META_ENV_VAR, None)
    env = Env()
    if env.env == 'existing-env':
        env.skip()
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')
    conn.execute_command('HSET', 'doc:2', 't', 'hello')

    env.expect('RESTORE', 'doc:3', 0, dump(env, 'doc:1')).ok()

    env.assertEqual(search(env), [3, 'doc:1', 'doc:2', 'doc:3'])
    env.assertEqual(get_internal_id(env, 'doc:3'), 3)


@skip(cluster=True)
def test_flex_restore_same_key_after_del():
    """DUMP k, DEL k, RESTORE k: the inherited docId was already deleted, so the
    reindex finds no live document under it and assigns a fresh id."""
    env = flex_env()
    if env.env == 'existing-env':
        env.skip()
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')
    payload = dump(env, 'doc:1')
    conn.execute_command('DEL', 'doc:1')

    env.expect('RESTORE', 'doc:1', 0, payload).ok()

    env.assertEqual(search(env), [1, 'doc:1'])
    env.assertEqual(get_internal_id(env, 'doc:1'), 2)


@skip(cluster=True)
def test_flex_restore_under_new_name_keeps_source_indexed():
    """RESTORE of doc:1's payload under doc:copy hands doc:copy doc:1's docId. The
    REPLACE reindex of doc:copy then treats that id as its own previous version and
    deletes it, removing doc:1 from the index although doc:1 is unchanged."""
    env = flex_env()
    if env.env == 'existing-env':
        env.skip()
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')
    conn.execute_command('HSET', 'doc:2', 't', 'hello')

    env.expect('RESTORE', 'doc:copy', 0, dump(env, 'doc:1')).ok()

    env.assertEqual(search(env), [3, 'doc:1', 'doc:2', 'doc:copy'])


@skip(cluster=True)
def test_flex_restore_to_unindexed_key_then_delete():
    """A payload restored under a name the index does not cover is never reindexed, so
    it keeps the source key's {specId -> docId} entry. Deleting it fires the DocIdMeta
    unlink callback, which deletes that docId - the source key's document."""
    env = flex_env()
    if env.env == 'existing-env':
        env.skip()
    create_index(env, prefix='doc:')
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')

    env.expect('RESTORE', 'other:1', 0, dump(env, 'doc:1')).ok()
    env.assertEqual(search(env), [1, 'doc:1'])
    conn.execute_command('DEL', 'other:1')

    env.assertEqual(search(env), [1, 'doc:1'])


@skip(cluster=True)
def test_flex_restore_from_another_server_with_same_spec_id():
    """specIds restart from 1 in every process, so the first index on two servers
    shares a specId. A payload dumped on one server and restored on the other passes
    the isSpecValid filter and its docId is applied to an unrelated local document."""
    env = flex_env()
    if env.env == 'existing-env':
        env.skip()
    # "Source server": doc:a2 gets docId 2 in the first index (specId 1).
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:a1', 't', 'hello')
    conn.execute_command('HSET', 'doc:a2', 't', 'hello')
    payload = dump(env, 'doc:a2')

    # "Destination server": an empty restart resets the specId counter.
    conn.execute_command('FLUSHALL')
    env.stop()
    env.start()
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:b1', 't', 'hello')
    conn.execute_command('HSET', 'doc:b2', 't', 'hello')

    env.expect('RESTORE', 'doc:a2', 0, payload).ok()

    env.assertEqual(search(env), [3, 'doc:a2', 'doc:b1', 'doc:b2'])


@skip(cluster=True)
def test_flex_restore_after_index_recreated():
    """Recreating the index gives it a new specId, so the payload's entry is skipped on
    load and the restored key is indexed from scratch."""
    env = flex_env()
    if env.env == 'existing-env':
        env.skip()
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')
    payload = dump(env, 'doc:1')
    env.expect('FT.DROPINDEX', 'idx').ok()
    create_index(env)
    conn.execute_command('HSET', 'doc:2', 't', 'hello')

    env.expect('RESTORE', 'doc:3', 0, payload).ok()

    env.assertEqual(search(env), [2, 'doc:2', 'doc:3'])


@skip(cluster=True)
def test_flex_restore_replace_with_stale_payload():
    """RESTORE REPLACE with an older dump of the same key: the old value is unlinked
    (deleting its current docId) and the payload's docId is already gone, so the key
    ends up indexed once under a fresh id."""
    env = flex_env()
    if env.env == 'existing-env':
        env.skip()
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')
    payload = dump(env, 'doc:1')
    conn.execute_command('HSET', 'doc:1', 't', 'world')

    env.expect('RESTORE', 'doc:1', 0, payload, 'REPLACE').ok()

    env.assertEqual(search(env), [1, 'doc:1'])
    env.assertEqual(search(env, 'world'), [0])


@skip(cluster=True)
def test_flex_copy_does_not_carry_docid():
    """COPY registers no copy callback, so the destination gets no DocIdMeta and both
    keys stay indexed - the contrast to RESTORE under a new name."""
    env = flex_env()
    if env.env == 'existing-env':
        env.skip()
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')

    conn.execute_command('COPY', 'doc:1', 'doc:copy')

    env.assertEqual(search(env), [2, 'doc:1', 'doc:copy'])
