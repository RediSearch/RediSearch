# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from contextlib import contextmanager

from common import skip, to_dict, waitForIndex


@contextmanager
def himport_connection(env):
    # Fieldsets belong to a physical connection, not a connection pool.
    with env.getClusterConnectionIfNeeded().client() as conn:
        try:
            yield conn
        finally:
            conn.execute_command('HIMPORT', 'DISCARDALL')


def create_index(env, index='idx', prefix='doc:'):
    env.expect('FT.CREATE', index, 'ON', 'HASH', 'PREFIX', 1, prefix,
               'SCHEMA', 'title', 'TEXT', 'category', 'TAG',
               'price', 'NUMERIC', 'SORTABLE').ok()


def template_cases(env):
    limit = int(env.cmd('CONFIG GET', 'hash-max-listpack-value')['hash-max-listpack-value'])
    return [('template-listpack', 'short'), ('template-array', 'x' * (limit + 1))]


def assert_document(env, index, key, title, category, price, extra):
    query = f'@title:{title} @category:{{{category}}} @price:[{price} {price}]'
    env.expect('FT.SEARCH', index, query, 'NOCONTENT').equal([1, key])
    env.expect('FT.SEARCH', index, query, 'RETURN', 2, 'title', 'extra').equal(
        [1, key, ['title', title, 'extra', extra]])
    env.expect('FT.SEARCH', index, query, 'SORTBY', 'price', 'RETURN', 1, 'price').equal(
        [1, key, ['price', str(price)]])
    result = env.cmd('FT.SEARCH', index, query)
    env.assertEqual(result[:2], [1, key], message=result)
    env.assertEqual(to_dict(result[2]), {
        'title': title, 'category': category, 'price': str(price), 'extra': extra})
    env.expect('FT.AGGREGATE', index, query, 'LOAD', 2, '@title', '@extra').equal(
        [1, ['title', title, 'extra', extra]])


@skip(cluster=True, redis_less_than='8.10.0')
def test_himport_indexing_and_loading(env):
    """HIMPORT notifications and module hash reads support both template encodings."""
    create_index(env)
    with himport_connection(env) as conn:
        env.assertEqual(conn.execute_command('HIMPORT', 'PREPARE', 'fields',
                                            'title', 'price', 'extra', 'category'), 'OK')
        for encoding, extra in template_cases(env):
            key = f'doc:{encoding}'
            title = 'compact' if encoding == 'template-listpack' else 'expanded'
            env.assertEqual(conn.execute_command('HIMPORT', 'SET', key, 'fields',
                                                title, 12, extra, 'books'), 'OK')
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', key), encoding)
            assert_document(env, 'idx', key, title, 'books', 12, extra)


@skip(cluster=True, redis_less_than='8.10.0')
def test_himport_replacement(env):
    """Replacing a hash removes old postings, including fields absent from the new fieldset."""
    create_index(env)
    with himport_connection(env) as conn:
        conn.execute_command('HIMPORT', 'PREPARE', 'fields', 'title', 'category', 'price', 'extra')
        conn.execute_command('HIMPORT', 'PREPARE', 'unindexed', 'extra')
        for encoding, extra in template_cases(env):
            key = f'doc:{encoding}'
            conn.execute_command('HSET', key, 'title', 'original', 'category', 'old', 'price', 1)
            env.expect('FT.SEARCH', 'idx', '@title:original', 'NOCONTENT').equal([1, key])
            env.assertEqual(conn.execute_command('HIMPORT', 'SET', key, 'fields',
                                                'replacement', 'new', 2, extra), 'OK')
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', key), encoding)
            assert_document(env, 'idx', key, 'replacement', 'new', 2, extra)
            for query in ['@title:original', '@category:{old}', '@price:[1 1]']:
                env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT').equal([0])

            env.assertEqual(conn.execute_command('HIMPORT', 'SET', key, 'unindexed', extra), 'OK')
            for query in ['@title:replacement', '@category:{new}', '@price:[2 2]']:
                env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT').equal([0])
            conn.execute_command('DEL', key)


@skip(cluster=True, redis_less_than='8.10.0')
def test_himport_replacement_clears_ttl(env):
    """Replacing an expiring hash clears its key TTL and replaces its search postings."""
    create_index(env)
    with himport_connection(env) as conn:
        conn.execute_command('HIMPORT', 'PREPARE', 'fields', 'title', 'category', 'price', 'extra')
        expiration = int(conn.time()[0]) + 86400
        for encoding, extra in template_cases(env):
            key = f'doc:{encoding}'
            conn.execute_command('HSET', key, 'title', 'original', 'category', 'old', 'price', 1)
            env.assertEqual(conn.execute_command('EXPIREAT', key, expiration), 1)
            env.assertEqual(conn.execute_command('EXPIRETIME', key), expiration)
            env.expect('FT.SEARCH', 'idx', '@title:original', 'NOCONTENT').equal([1, key])

            env.assertEqual(conn.execute_command('HIMPORT', 'SET', key, 'fields',
                                                'replacement', 'new', 2, extra), 'OK')
            env.assertEqual(conn.execute_command('PTTL', key), -1)
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', key), encoding)
            assert_document(env, 'idx', key, 'replacement', 'new', 2, extra)
            for query in ['@title:original', '@category:{old}', '@price:[1 1]']:
                env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT').equal([0])
            conn.execute_command('DEL', key)


@skip(cluster=True, redis_less_than='8.10.0')
def test_himport_hash_mutations(env):
    """Ordinary hash writes and deletions reindex template-backed documents."""
    create_index(env)
    with himport_connection(env) as conn:
        conn.execute_command('HIMPORT', 'PREPARE', 'fields', 'title', 'price', 'extra')
        for encoding, extra in template_cases(env):
            key = f'doc:{encoding}'
            conn.execute_command('HIMPORT', 'SET', key, 'fields', 'original', 1, extra)
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', key), encoding)
            conn.execute_command('HSET', key, 'title', 'updated', 'category', 'added')
            conn.execute_command('HINCRBY', key, 'price', 2)
            assert_document(env, 'idx', key, 'updated', 'added', 3, extra)
            env.expect('FT.SEARCH', 'idx', '@title:original | @price:[1 1]',
                       'NOCONTENT').equal([0])
            conn.execute_command('HDEL', key, 'category')
            env.expect('FT.SEARCH', 'idx', '@category:{added}', 'NOCONTENT').equal([0])
            env.expect('FT.SEARCH', 'idx', '@title:updated', 'NOCONTENT').equal([1, key])
            conn.execute_command('DEL', key)
            env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([0])


@skip(cluster=True, redis_less_than='8.10.0')
def test_himport_backfill_and_reload(env):
    """Background indexing and RDB loading can read persisted template hashes."""
    cases = template_cases(env)
    with himport_connection(env) as conn:
        conn.execute_command('HIMPORT', 'PREPARE', 'fields', 'title', 'category', 'price', 'extra')
        for encoding, extra in cases:
            conn.execute_command('HIMPORT', 'SET', f'doc:{encoding}', 'fields',
                                 'compact' if encoding == 'template-listpack' else 'expanded',
                                 'books', 12, extra)
    create_index(env)
    waitForIndex(env, 'idx')
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for encoding, extra in cases:
            key = f'doc:{encoding}'
            env.assertEqual(env.cmd('OBJECT', 'ENCODING', key), encoding)
            assert_document(env, 'idx', key,
                            'compact' if encoding == 'template-listpack' else 'expanded',
                            'books', 12, extra)


@skip(cluster=True, redis_less_than='8.10.0')
def test_hash_auto_template_conversion(env):
    """HSET's opt-in conversion preserves indexing and field loading without HIMPORT."""
    create_index(env)
    config = env.cmd('CONFIG GET', 'hash-min-template-entries', 'hash-max-template-entries')
    conn = env.getClusterConnectionIfNeeded()
    try:
        env.cmd('CONFIG SET', 'hash-min-template-entries', 0, 'hash-max-template-entries', 0)
        cases = template_cases(env)
        for encoding, extra in cases:
            conn.execute_command('HSET', f'doc:{encoding}', 'title', 'original',
                                 'category', 'books', 'price', 12, 'extra', extra)
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', f'doc:{encoding}'),
                            'listpack' if encoding == 'template-listpack' else 'hashtable')
        env.cmd('CONFIG SET', 'hash-min-template-entries', 4)
        for encoding, extra in cases:
            key = f'doc:{encoding}'
            title = 'compact' if encoding == 'template-listpack' else 'expanded'
            conn.execute_command('HSET', key, 'title', title)
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', key), encoding)
            assert_document(env, 'idx', key, title, 'books', 12, extra)
        env.expect('FT.SEARCH', 'idx', '@title:original', 'NOCONTENT').equal([0])
    finally:
        env.cmd('CONFIG SET', *[arg for item in config.items() for arg in item])


@skip(cluster=True, redis_less_than='8.10.0')
def test_himport_restore(env):
    """RESTORE, also used by HIMPORT replication, indexes template hashes and replacements."""
    create_index(env)
    with himport_connection(env) as conn:
        conn.execute_command('HIMPORT', 'PREPARE', 'fields', 'title', 'category', 'price', 'extra')
        for encoding, extra in template_cases(env):
            conn.execute_command('HIMPORT', 'SET', 'source', 'fields', 'restored', 'books', 12, extra)
            payload = conn.dump('source')
            conn.execute_command('HSET', 'doc:restored', 'title', 'original')
            conn.execute_command('RESTORE', 'doc:restored', 0, payload, 'REPLACE')
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', 'doc:restored'), encoding)
            assert_document(env, 'idx', 'doc:restored', 'restored', 'books', 12, extra)
            env.expect('FT.SEARCH', 'idx', '@title:original', 'NOCONTENT').equal([0])
            conn.execute_command('DEL', 'doc:restored')


@skip(cluster=True, redis_less_than='8.10.0')
def test_hash_template_conversion_on_rdb_load(env):
    """Converting plain hashes while loading an RDB preserves Search results."""
    create_index(env)
    config = env.cmd('CONFIG GET', 'hash-min-template-entries',
                     'hash-rdb-load-min-template-entries', 'hash-rdb-load-max-template-entries',
                     'hash-rdb-load-template-disassembly-threshold')
    conn = env.getClusterConnectionIfNeeded()
    try:
        env.cmd('CONFIG SET', 'hash-min-template-entries', 0,
                'hash-rdb-load-min-template-entries', 4, 'hash-rdb-load-max-template-entries', 0,
                'hash-rdb-load-template-disassembly-threshold', 0)
        cases = template_cases(env)
        for encoding, extra in cases:
            key = f'doc:{encoding}'
            title = 'compact' if encoding == 'template-listpack' else 'expanded'
            conn.execute_command('HSET', key, 'title', title, 'category', 'books',
                                 'price', 12, 'extra', extra)
            env.assertEqual(conn.execute_command('OBJECT', 'ENCODING', key),
                            'listpack' if encoding == 'template-listpack' else 'hashtable')
            assert_document(env, 'idx', key, title, 'books', 12, extra)
        env.dumpAndReload()
        waitForIndex(env, 'idx')
        for encoding, extra in cases:
            key = f'doc:{encoding}'
            env.assertEqual(env.cmd('OBJECT', 'ENCODING', key), encoding)
            assert_document(env, 'idx', key,
                            'compact' if encoding == 'template-listpack' else 'expanded',
                            'books', 12, extra)
    finally:
        env.cmd('CONFIG SET', *[arg for item in config.items() for arg in item])
