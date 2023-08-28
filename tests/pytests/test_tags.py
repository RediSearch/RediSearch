# -*- coding: utf-8 -*-

from includes import *
from common import *

def search(env, r, *args):
    return r.execute_command('ft.search', *args)

def testTagIndex(env):
    r = env
    env.expect('ft.create', 'idx', 'ON', 'HASH','schema', 'title', 'text', 'tags', 'tag').ok()
    N = 10
    for n in range(N):
        env.expect('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                       'title', 'hello world term%d' % n, 'tags', 'foo bar,xxx,tag %d' % n).ok()
    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        res = env.cmd('ft.search', 'idx', 'hello world')
        env.assertEqual(10, res[0])

        res = env.cmd('ft.search', 'idx', 'foo bar')
        env.assertEqual(0, res[0])

        res = env.cmd('ft.search', 'idx', '@tags:{foo bar}')
        env.assertEqual(N, res[0])

        # inorder should not affect tags
        res = env.cmd(
            'ft.search', 'idx', '@tags:{tag 1} @tags:{foo bar}', 'slop', '0', 'inorder')
        env.assertEqual(1, res[0])

        for n in range(N - 1):
            res = env.cmd(
                'ft.search', 'idx', '@tags:{tag %d}' % n, 'nocontent')
            env.assertEqual(1, res[0])
            env.assertEqual('doc%d' % n, res[1])
            res = env.cmd(
                'ft.search', 'idx', '@tags:{tag\\ %d}' % n, 'nocontent')
            env.assertEqual(1, res[0])

            res = env.cmd(
                'ft.search', 'idx', 'hello world @tags:{tag\\ %d|tag %d}' % (n, n + 1), 'nocontent')
            env.assertEqual(2, res[0])
            res = py2sorted(res[1:])
            env.assertEqual('doc%d' % n, res[0])
            env.assertEqual('doc%d' % (n + 1), res[1])

            res = env.cmd(
                'ft.search', 'idx', 'term%d @tags:{tag %d}' % (n, n), 'nocontent')
            env.assertEqual(1, res[0])
            env.assertEqual('doc%d' % n, res[1])

def testSeparator(env):
    r = env
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'tags', 'tag', 'separator', ':').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'tags', 'x:hello world: fooz bar:foo,bar:BOO FAR').ok()
    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        for q in ('@tags:{hello world}', '@tags:{fooz bar}', '@tags:{foo\\,bar}', '@tags:{boo\\ far}', '@tags:{x}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(1, res[0])

def testTagPrefix(env):
    env.skipOnCluster()
    r = env
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'tags', 'tag', 'separator', ',').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields', 'title', 'hello world',
               'tags', 'hello world,hello-world,hell,jell').ok()
    env.expect('FT.DEBUG', 'dump_tagidx', 'idx', 'tags')    \
        .equal([['hell', [1]], ['hello world', [1]], ['hello-world', [1]], ['jell', [1]]])

    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        for q in ('@tags:{hello world}', '@tags:{hel*}', '@tags:{hello\\-*}', '@tags:{he*}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(res[0], 1)

def testTagFieldCase(env):
    r = env
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'TAgs', 'tag').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'TAgs', 'HELLO WORLD,FOO BAR').ok()
    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        env.assertListEqual([0], r.execute_command(
            'FT.SEARCH', 'idx', '@tags:{HELLO WORLD}'))
        env.assertListEqual([1, 'doc1'], r.execute_command(
            'FT.SEARCH', 'idx', '@TAgs:{HELLO WORLD}', 'NOCONTENT'))
        env.assertListEqual([1, 'doc1'], r.execute_command(
            'FT.SEARCH', 'idx', '@TAgs:{foo bar}', 'NOCONTENT'))
        env.assertListEqual([0], r.execute_command(
            'FT.SEARCH', 'idx', '@TAGS:{foo bar}', 'NOCONTENT'))

def testInvalidSyntax(env):
    r = env
    # invalid syntax
    with env.assertResponseError():
        r.execute_command(
            'ft.create', 'idx', 'ON', 'HASH',
            'schema', 'title', 'text', 'tags', 'tag', 'separator')
    with env.assertResponseError():
        r.execute_command(
            'ft.create', 'idx', 'ON', 'HASH',
            'schema', 'title', 'text', 'tags', 'tag', 'separator', "foo")
    with env.assertResponseError():
        r.execute_command(
            'ft.create', 'idx', 'ON', 'HASH',
            'schema', 'title', 'text', 'tags', 'tag', 'separator', "")

def testTagVals(env):
    r = env
    r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'tags', 'tag', 'othertags', 'tag')

    N = 100
    alltags = set()
    for n in range(N):
        tags = (f'foo {n}', f'bar {n}', 'x')
        alltags.add(tags[0])
        alltags.add(tags[1])
        alltags.add(tags[2])

        env.expect('ft.add', 'idx', f'doc{n}', 1.0, 'fields',
                   'tags', ','.join(tags), 'othertags', f'baz {int(n // 2)}').ok()
    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        res = r.execute_command('ft.tagvals', 'idx', 'tags')
        env.assertEqual(N * 2 + 1, len(res))

        env.assertEqual(alltags, set(res))

        res = r.execute_command('ft.tagvals', 'idx', 'othertags')
        env.assertEqual(N / 2, len(res))

        env.expect('ft.tagvals', 'idx').raiseError()
        env.expect('ft.tagvals', 'idx', 'idx', 'idx').raiseError()
        env.expect('ft.tagvals', 'fake_idx', 'tags').raiseError()
        env.expect('ft.tagvals', 'idx', 'fake_tags').raiseError()
        env.expect('ft.tagvals', 'idx', 'title').raiseError()

def testSearchNotExistsTagValue(env):
    # this test basically make sure we are not leaking
    env.expect('FT.CREATE idx ON HASH SCHEMA t TAG SORTABLE').ok()
    env.expect('FT.SEARCH idx @t:{val}').equal([0])

def testIssue1305(env):
    env.expect('FT.CREATE myIdx ON HASH SCHEMA title TAG').ok()
    env.expect('FT.ADD myIdx doc2 1.0 FIELDS title "work"').ok()
    env.expect('FT.ADD myIdx doc2 1.0 FIELDS title "hello"').error()
    env.expect('FT.ADD myIdx doc3 1.0 FIELDS title "hello"').ok()
    env.expect('FT.ADD myIdx doc1 1.0 FIELDS title "hello,work"').ok()
    expectedRes = {'doc2': ['nan', ['title', '"work"']], 'doc3' : ['nan', ['title', '"hello"']],
                   'doc1' : ['nan', ['title', '"hello,work"']]}
    res = env.cmd('ft.search', 'myIdx', '~@title:{wor} ~@title:{hell}', 'WITHSCORES')[1:]
    res = {res[i]:res[i + 1: i + 3] for i in range(0, len(res), 3)}
    env.assertEqual(res, expectedRes)

def testTagCaseSensitive(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE idx1 SCHEMA t TAG').ok()
    env.expect('FT.CREATE idx2 SCHEMA t TAG CASESENSITIVE').ok()
    env.expect('FT.CREATE idx3 SCHEMA t TAG SEPARATOR .').ok()
    env.expect('FT.CREATE idx4 SCHEMA t TAG SEPARATOR . CASESENSITIVE').ok()
    env.expect('FT.CREATE idx5 SCHEMA t TAG CASESENSITIVE SEPARATOR .').ok()

    conn.execute_command('HSET', 'doc1', 't', 'foo,FOO')
    conn.execute_command('HSET', 'doc2', 't', 'FOO')
    conn.execute_command('HSET', 'doc3', 't', 'foo')

    if not env.is_cluster():
        conn.execute_command('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx1', 't').equal([['foo', [1, 2, 3]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx2', 't').equal([['FOO', [1, 2]], ['foo', [1, 3]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx3', 't').equal([['foo', [2, 3]], ['foo,foo', [1]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx4', 't').equal([['FOO', [2]], ['foo', [3]], ['foo,FOO', [1]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx5', 't').equal([['FOO', [2]], ['foo', [3]], ['foo,FOO', [1]]])

    env.expect('FT.SEARCH', 'idx1', '@t:{FOO}')         \
        .equal([3, 'doc1', ['t', 'foo,FOO'], 'doc2', ['t', 'FOO'], 'doc3', ['t', 'foo']])
    env.expect('FT.SEARCH', 'idx1', '@t:{foo}')         \
        .equal([3, 'doc1', ['t', 'foo,FOO'], 'doc2', ['t', 'FOO'], 'doc3', ['t', 'foo']])

    env.expect('FT.SEARCH', 'idx2', '@t:{FOO}')         \
        .equal([2, 'doc1', ['t', 'foo,FOO'], 'doc2', ['t', 'FOO']])
    env.expect('FT.SEARCH', 'idx2', '@t:{foo}')         \
        .equal([2, 'doc1', ['t', 'foo,FOO'], 'doc3', ['t', 'foo']])

    conn.execute_command('HSET', 'doc1', 't', 'f o,F O')
    conn.execute_command('HSET', 'doc2', 't', 'F O')
    conn.execute_command('HSET', 'doc3', 't', 'f o')

    if not env.is_cluster():
        forceInvokeGC(env, 'idx1')
        forceInvokeGC(env, 'idx2')
        forceInvokeGC(env, 'idx3')
        forceInvokeGC(env, 'idx4')
        forceInvokeGC(env, 'idx5')

        env.expect('FT.DEBUG', 'dump_tagidx', 'idx1', 't').equal([['f o', [4, 5, 6]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx2', 't').equal([['F O', [4, 5]], ['f o', [4, 6]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx3', 't').equal([['f o', [5, 6]], ['f o,f o', [4]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx4', 't').equal([['F O', [5]], ['f o', [6]], ['f o,F O', [4]]])
        env.expect('FT.DEBUG', 'dump_tagidx', 'idx5', 't').equal([['F O', [5]], ['f o', [6]], ['f o,F O', [4]]])

    # not casesensitive
    env.expect('FT.SEARCH', 'idx1', '@t:{F\\ O}')         \
        .equal([3, 'doc1', ['t', 'f o,F O'], 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])
    env.expect('FT.SEARCH', 'idx1', '@t:{f\\ o}')         \
        .equal([3, 'doc1', ['t', 'f o,F O'], 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])

    # casesensitive
    env.expect('FT.SEARCH', 'idx2', '@t:{F\\ O}')         \
        .equal([2, 'doc1', ['t', 'f o,F O'], 'doc2', ['t', 'F O']])
    env.expect('FT.SEARCH', 'idx2', '@t:{f\\ o}')         \
        .equal([2, 'doc1', ['t', 'f o,F O'], 'doc3', ['t', 'f o']])

    # not casesensitive
    env.expect('FT.SEARCH', 'idx3', '@t:{f\\ o\\,f\\ o}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx3', '@t:{f\\ o\\,F\\ O}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx3', '@t:{F\\ O\\,F\\ O}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx3', '@t:{F\\ O}')         \
        .equal([2, 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])
    env.expect('FT.SEARCH', 'idx3', '@t:{f\\ o}')         \
        .equal([2, 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])

    # casesensitive
    env.expect('FT.SEARCH', 'idx4', '@t:{f\\ o\\,f\\ o}')         \
        .equal([0])
    env.expect('FT.SEARCH', 'idx4', '@t:{f\\ o\\,F\\ O}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx4', '@t:{F\\ O\\,F\\ O}')         \
        .equal([0])
    env.expect('FT.SEARCH', 'idx4', '@t:{F\\ O}')         \
        .equal([1, 'doc2', ['t', 'F O']])
    env.expect('FT.SEARCH', 'idx4', '@t:{f\\ o}')         \
        .equal([1, 'doc3', ['t', 'f o']])

def testTagGCClearEmpty(env):
    env.skipOnCluster()

    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'bar')
    conn.execute_command('HSET', 'doc3', 't', 'baz')
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([['bar', [2]], ['baz', [3]], ['foo', [1]]])
    env.expect('FT.SEARCH', 'idx', '@t:{foo}').equal([1, 'doc1', ['t', 'foo']])

    # delete two tags
    conn.execute_command('DEL', 'doc1')
    conn.execute_command('DEL', 'doc2')
    forceInvokeGC(env, 'idx')
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([['baz', [3]]])
    env.expect('FT.SEARCH', 'idx', '@t:{foo}').equal([0])

    # delete last tag
    conn.execute_command('DEL', 'doc3')
    forceInvokeGC(env, 'idx')
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([])

    # check term can be used after being empty
    conn.execute_command('HSET', 'doc4', 't', 'foo')
    conn.execute_command('HSET', 'doc5', 't', 'foo')
    env.expect('FT.SEARCH', 'idx', '@t:{foo}')  \
        .equal([2, 'doc4', ['t', 'foo'], 'doc5', ['t', 'foo']])

def testTagGCClearEmptyWithCursor(env):
    env.skipOnCluster()

    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'foo')
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([['foo', [1, 2]]])

    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '@t:{foo}', 'WITHCURSOR', 'COUNT', '1')
    env.assertEqual(res, [1, []])

    # delete both documents and run the GC to clean 'foo' inverted index
    env.expect('DEL', 'doc1').equal(1)
    env.expect('DEL', 'doc2').equal(1)

    forceInvokeGC(env, 'idx')

    # make sure the inverted index was cleaned
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([])

    # read from the cursor
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)

def testTagGCClearEmptyWithCursorAndMoreData(env):
    env.skipOnCluster()

    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'foo')
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([['foo', [1, 2]]])

    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '@t:{foo}', 'WITHCURSOR', 'COUNT', '1')
    env.assertEqual(res, [1, []])

    # delete both documents and run the GC to clean 'foo' inverted index
    env.expect('DEL', 'doc1').equal(1)
    env.expect('DEL', 'doc2').equal(1)

    forceInvokeGC(env, 'idx')

    # make sure the inverted index was cleaned
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([])

    # add data
    conn.execute_command('HSET', 'doc3', 't', 'foo')
    conn.execute_command('HSET', 'doc4', 't', 'foo')
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([['foo', [3, 4]]])

    # read from the cursor
    res, cursor = conn.execute_command('FT.CURSOR', 'READ', 'idx', cursor)
    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)

    # ensure later documents with same tag are read
    res = conn.execute_command('FT.AGGREGATE', 'idx', '@t:{foo}')
    env.assertEqual(res, [1, [], []])

@unstable
def testEmptyTagLeak(env):
    env.skipOnCluster()

    cycles = 1
    tags = 30

    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    pl = conn.pipeline()

    for i in range(cycles):
        for j in range(tags):
            x = j + i * tags
            pl.execute_command('HSET', 'doc{}'.format(x), 't', 'tag{}'.format(x))
        pl.execute()
        for j in range(tags):
            pl.execute_command('DEL', 'doc{}'.format(j + i * tags))
        pl.execute()
    forceInvokeGC(env, 'idx')
    env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([])
