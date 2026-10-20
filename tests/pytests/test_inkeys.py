# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import threading

from common import *
from RLTest import Env


@skip(cluster=True)
def test_inkeys_resolves_after_queued_reindex():
    """A reindex before the execution lock must not invalidate INKEYS membership."""
    for enabled in (False, True):
        # Startup-only acceleration and a worker are needed to exercise both handoffs.
        env = Env(moduleArgs=f'WORKERS 1 DOC_KEY_MAP {str(enabled).lower()} TIMEOUT 0')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC',
                   'SORTABLE', 'NOINDEX').ok()
        env.cmd('HSET', 'doc', 't', 'hello', 'n', 1)
        old_id = env.cmd(debug_cmd(), 'DOCIDTOID', 'idx', 'doc')
        point = 'BeforeSpecLock'
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
        results = []
        conn = env.getConnection()

        def query():
            try:
                results.append(conn.execute_command('FT.SEARCH', 'idx', 'hello',
                                                     'INKEYS', 1, 'doc', 'NOCONTENT'))
            except Exception as exc:
                results.append(exc)

        thread = threading.Thread(target=query, daemon=True)
        thread.start()
        try:
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                'query did not reach the execution handoff')
            env.cmd('HSET', 'doc', 'n', 2)
            env.assertNotEqual(env.cmd(debug_cmd(), 'DOCIDTOID', 'idx', 'doc'), old_id)
        finally:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
            thread.join(timeout=10)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.assertFalse(thread.is_alive(), message='query did not complete')
        env.assertEqual(results, [[1, 'doc']])
        env.stop()


def test_doc_key_map_lifecycle():
    """The optional map follows initial scans, updates, rename, deletion and reload."""
    # Each shard must maintain its own opt-in table, including during an initial scan.
    env = Env(moduleArgs='WORKERS 1 DOC_KEY_MAP true')
    conn = getConnectionByEnv(env)
    key = '{doc}:a'
    renamed = '{doc}:b'
    binary = '{doc}:a\0suffix'
    conn.execute_command('HSET', key, 't', 'hello', 'n', 1)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, '{doc}:', 'SCHEMA', 't', 'TEXT',
               'n', 'NUMERIC', 'SORTABLE').ok()
    waitForIndex(env, 'idx')

    for _ in env.reloadingIterator():
        env.expect('FT.SEARCH', 'idx', 'hello', 'INKEYS', 3, key, key, 'missing',
                   'NOCONTENT').equal([1, key])
        conn.execute_command('HSET', key, 't', 'hello', 'n', 2)
        env.expect('FT.SEARCH', 'idx', '@n:[2 2]', 'INKEYS', 1, key,
                   'NOCONTENT').equal([1, key])
        conn.execute_command('RENAME', key, renamed)
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([0])
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, renamed,
                   'NOCONTENT').equal([1, renamed])
        conn.execute_command('HSET', binary, 't', 'hello', 'n', 3)
        env.expect('FT.SEARCH', 'idx', '@n:[3 3]', 'INKEYS', 1, binary,
                   'NOCONTENT').equal([1, binary])
        conn.execute_command('DEL', binary, renamed)
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 2, binary, renamed,
                   'NOCONTENT').equal([0])
        conn.execute_command('HSET', key, 't', 'hello', 'n', 1)

    env.expect('FT.CREATE', 'idx2', 'PREFIX', 1, '{doc}:', 'FILTER', '@n > 0',
               'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC').ok()
    waitForIndex(env, 'idx2')
    conn.execute_command('HSET', key, 'n', -1)
    env.expect('FT.SEARCH', 'idx2', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([1, key])
    conn.execute_command('HSET', key, 'n', 1)
    env.expect('FT.DROPINDEX', 'idx').ok()
    env.expect('FT.SEARCH', 'idx2', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([1, key])
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, '{doc}:', 'SCHEMA', 't', 'TEXT').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('PEXPIREAT', key, 1)
    env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([0])
    env.expect('CONFIG', 'SET', 'search-doc-key-map', 'no').error().contains('immutable')
    env.stop()


@skip(cluster=True)
def test_doc_key_map_memory_and_inline():
    """Disabled acceleration allocates no map; inline execution uses enabled maps."""
    for enabled in (False, True):
        # WORKERS 0 also exercises resolution without a thread-safe context lock.
        env = Env(moduleArgs=f'WORKERS 0 DOC_KEY_MAP {str(enabled).lower()}')
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
        env.cmd('HSET', 'doc', 't', 'hello')
        env.expect('FT.SEARCH', 'idx', 'hello', 'INKEYS', 1, 'doc',
                   'NOCONTENT').equal([1, 'doc'])
        env.assertEqual(float(index_info(env, 'idx')['key_table_size_mb']) > 0, enabled)
        env.assertContains('IDS {1,}', env.cmd('FT.EXPLAIN', 'idx', 'hello',
                                             'INKEYS', 1, 'doc'))
        env.assertEqual(env.cmd('EVAL',
                               "return redis.call('FT.SEARCH', 'idx', 'hello', 'INKEYS', 1, 'doc', 'NOCONTENT')",
                               0), [1, 'doc'])
        env.cmd('HSET', '', 't', 'hello')
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, '', 'NOCONTENT').equal([1, ''])
        env.stop()


@skip(cluster=True)
def test_inkeys_timeout_before_resolution():
    """Timeout callbacks can finish while INKEYS is waiting for its execution lock."""
    import redis

    for enabled in (False, True):
        for policy in ('fail', 'return-strict'):
            env = Env(moduleArgs=f'WORKERS 1 DOC_KEY_MAP {str(enabled).lower()} TIMEOUT 0')
            skipIfNoEnableAssert(env)
            env.cmd('FLUSHALL')
            env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
            env.cmd('HSET', 'doc', 't', 'hello')
            conn = redis.Redis(single_connection_client=True,
                               decode_responses=True,
                               **{k: v for k, v in env.getConnection().connection_pool.connection_kwargs.items()
                                  if k != 'decode_responses'})
            client_id = conn.client_id()
            point = 'BeforeSpecLock'
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            results = []

            def query():
                try:
                    results.append(conn.execute_command('FT.SEARCH', 'idx', '*',
                                                         'INKEYS', 1, 'doc', 'NOCONTENT'))
                except Exception as exc:
                    results.append(str(exc))

            thread = threading.Thread(target=query, daemon=True)
            thread.start()
            try:
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                    'query did not reach the execution handoff')
                env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                thread.join(timeout=10)
                env.assertFalse(thread.is_alive(), message='timeout callback did not reply')
            finally:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=10)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            if policy == 'fail':
                env.assertEqual(len(results), 1)
                env.assertContains('Timeout', results[0])
            else:
                env.assertEqual(results, [[0]])
            env.expect('PING').equal(True)
            conn.close()
            env.stop()
