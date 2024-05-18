# -*- coding: utf-8 -*-

from RLTest import Env
from includes import *
from common import *
import json

def search(env, r, *args):
    return r.execute_command('ft.search', *args)

def testTagIndex(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH','schema', 'title', 'text', 'tags', 'tag').ok()
    N = 10
    for n in range(N):
        env.expect('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                       'title', 'hello world term%d' % n, 'tags', 'foo bar,xxx,tag %d' % n).ok()
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
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
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'tags', 'tag', 'separator', ':').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'tags', 'x:hello world: fooz bar:foo,bar:BOO FAR').ok()
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for q in ('@tags:{hello world}', '@tags:{fooz bar}', '@tags:{foo\\,bar}', '@tags:{boo\\ far}', '@tags:{x}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(1, res[0])

@skip(cluster=True)
def testTagPrefix(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'tags', 'tag', 'separator', ',').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields', 'title', 'hello world',
               'tags', 'hello world,hello-world,hell,jell').ok()
    env.expect('FT.DEBUG', 'dump_tagidx', 'idx', 'tags')    \
        .equal([['hell', [1]], ['hello world', [1]], ['hello-world', [1]], ['jell', [1]]])

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for q in ('@tags:{hello world}', '@tags:{hel*}', '@tags:{hello\\-*}', '@tags:{he*}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(res[0], 1)

def testTagFieldCase(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'TAgs', 'tag').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'TAgs', 'HELLO WORLD,FOO BAR').ok()
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        env.assertEqual([0], env.cmd(
            'FT.SEARCH', 'idx', '@tags:{HELLO WORLD}'))
        env.assertEqual([1, 'doc1'], env.cmd(
            'FT.SEARCH', 'idx', '@TAgs:{HELLO WORLD}', 'NOCONTENT'))
        env.assertEqual([1, 'doc1'], env.cmd(
            'FT.SEARCH', 'idx', '@TAgs:{foo bar}', 'NOCONTENT'))
        env.assertEqual([0], env.cmd(
            'FT.SEARCH', 'idx', '@TAGS:{foo bar}', 'NOCONTENT'))

def testInvalidSyntax(env):
    # invalid syntax
    with env.assertResponseError():
        env.cmd(
            'ft.create', 'idx', 'ON', 'HASH',
            'schema', 'title', 'text', 'tags', 'tag', 'separator')
    with env.assertResponseError():
        env.cmd(
            'ft.create', 'idx', 'ON', 'HASH',
            'schema', 'title', 'text', 'tags', 'tag', 'separator', "foo")
    with env.assertResponseError():
        env.cmd(
            'ft.create', 'idx', 'ON', 'HASH',
            'schema', 'title', 'text', 'tags', 'tag', 'separator', "")

def testTagVals(env):
    env.cmd(
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
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('ft.tagvals', 'idx', 'tags')
        env.assertEqual(N * 2 + 1, len(res))

        env.assertEqual(alltags, set(res))

        res = env.cmd('ft.tagvals', 'idx', 'othertags')
        env.assertEqual(N / 2, len(res))

        env.expect('ft.tagvals', 'idx').error()
        env.expect('ft.tagvals', 'idx', 'idx', 'idx').error()
        env.expect('ft.tagvals', 'fake_idx', 'tags').error()
        env.expect('ft.tagvals', 'idx', 'fake_tags').error()
        env.expect('ft.tagvals', 'idx', 'title').error()

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

    if not env.isCluster():
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

    if not env.isCluster():
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

@skip(cluster=True)
def testTagGCClearEmpty(env):

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

@skip(cluster=True)
def testTagGCClearEmptyWithCursor(env):

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

@skip(cluster=True)
def testTagGCClearEmptyWithCursorAndMoreData(env):

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

@skip(cluster=True)
def testEmptyTagLeak(env):

    cycles = 1
    tags = 30

    conn = getConnectionByEnv(env)
    env.cmd('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
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

def test_empty_suffix_withsuffixtrie(env):
    """Tests that we don't leak when we search for a suffix with no entries in
    a TAG field indexed with the `WITHSUFFIXTRIE` optimization."""

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE').ok()

    # Populate with some data, so the query-iterator construction won't return early.
    conn.execute_command('HSET', 'h1', 't', '')

    # Search for a suffix with no entries
    cmd = 'FT.SEARCH idx_suffixtrie @t:{*pty}'.split(' ')
    expected = [0]
    res = env.cmd(*cmd)
    env.assertEqual(res, expected)

def testEmptyValueTags(env):
    """Tests that empty values are indexed properly"""

    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env = Env(moduleArgs="DEFAULT_DIALECT " + str(dialect))

        def testHashIndex(env, idx):
            """Performs a series of tests on a hash index"""

            conn = getConnectionByEnv(env)

            # Search for a single document, via its indexed empty value
            cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
            expected = [1, 'h1', ['t', '']]
            cmd_assert(env, cmd, expected)

            # Make sure the document is NOT returned when searching for a non-empty
            # value
            cmd = f'FT.SEARCH {idx}'.split(' ') + ['@t:{foo}']
            expected = [0]
            cmd_assert(env, cmd, expected)

            # ------------------------------ Negation ------------------------------
            # Search for a negation of a non-empty value, make sure the document is
            # returned
            cmd = f'FT.SEARCH {idx} -@t:{{foo}}'.split(' ')
            expected = [1, 'h1', ['t', '']]
            cmd_assert(env, cmd, expected)

            # Search for a negation of an empty value, make sure the document is NOT
            # returned
            cmd = f'FT.SEARCH {idx} -isempty(@t)'.split(' ')
            expected = [0]
            cmd_assert(env, cmd, expected)

            # ------------------------------- Union --------------------------------
            # Union of empty and non-empty values
            cmd = f'FT.SEARCH {idx}'.split(' ') + ['isempty(@t) | @t:{foo}']
            expected = [1, 'h1', ['t', '']]
            cmd_assert(env, cmd, expected)

            # ---------------------------- Intersection ----------------------------
            # Intersection of empty and non-empty values
            cmd = f'FT.SEARCH {idx}'.split(' ') + ['isempty(@t) @t:{foo}']
            expected = [0]
            cmd_assert(env, cmd, expected)

            # ------------------------------- Prefix -------------------------------
            # We shouldn't get the document when searching for a prefix of "__empty"
            cmd = f'FT.SEARCH {idx} @t:{{*pty}}'.split(' ')
            expected = [0]
            cmd_assert(env, cmd, expected)

            # ------------------------------- Suffix -------------------------------
            # We shouldn't get the document when searching for a suffix of "__empty"
            cmd = f'FT.SEARCH {idx} @t:{{__em*}}'.split(' ')
            expected = [0]
            cmd_assert(env, cmd, expected)

            # Add a document that will be found by the suffix search
            conn.execute_command('HSET', 'h2', 't', 'empty')
            cmd = f'FT.SEARCH {idx} @t:{{*pty}}'.split(' ')
            expected = [1, 'h2', ['t', 'empty']]
            cmd_assert(env, cmd, expected)
            conn.execute_command('DEL', 'h2')

            # -------------------- Combination with other fields -------------------
            cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello | isempty(@t)']
            expected = [1, 'h1', ['t', '']]
            cmd_assert(env, cmd, [1, 'h1', ['t', '']])

            cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello isempty(@t)']
            expected = [0]
            cmd_assert(env, cmd, [0])

            # Non-empty intersection with another field
            conn.execute_command('HSET', 'h1', 'text', 'hello')
            cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello isempty(@t)']
            expected = [1, 'h1', ['t', '', 'text', 'hello']]
            cmd_assert(env, cmd, expected)

            # Non-empty union with another field
            conn.execute_command('HSET', 'h2', 'text', 'love you', 't', 'movie')
            cmd = f'FT.SEARCH {idx}'.split(' ') + ['love | isempty(@t)', 'SORTBY', 'text', 'ASC']
            expected = [
                2, 'h1', ['text', 'hello', 't', ''], 'h2', ['text', 'love you', 't', 'movie']
            ]
            cmd_assert(env, cmd, expected)

            # TODO: Fix this, using DIALECT 5 it returns: Success (not an error)
            # Enable the test when we fix the issue MOD-6967
            if dialect == 2:
                # Checking the functionality of our pipeline with empty values
                # ------------------------------- APPLY --------------------------------
                # Populate with some data that we will be able to see the `APPLY`
                cmd = f'FT.AGGREGATE {idx} * LOAD 1 @t APPLY upper(@t) as upper_t SORTBY 4 @t ASC @upper_t ASC'.split(' ')
                expected = [
                    ANY, \
                    ['t', '', 'upper_t', ''], \
                    ['t', 'movie', 'upper_t', 'MOVIE']
                ]
                cmd_assert(env, cmd, expected)

                # ------------------------------ SORTBY --------------------------------
                cmd = f'FT.AGGREGATE {idx} * LOAD * SORTBY 2 @t ASC'.split(' ')
                expected = [
                    ANY, \
                    ['t', '', 'text', 'hello'], \
                    ['t', 'movie', 'text', 'love you']
                ]
                cmd_assert(env, cmd, expected)

                # Reverse order
                cmd = f'FT.AGGREGATE {idx} * LOAD * SORTBY 2 @t DESC'.split(' ')
                expected = [
                    ANY, \
                    ['t', 'movie', 'text', 'love you'], \
                    ['t', '', 'text', 'hello']
                ]
                cmd_assert(env, cmd, expected)

                # ------------------------------ GROUPBY -------------------------------
                conn.execute_command('HSET', 'h3', 't', 'movie')
                conn.execute_command('HSET', 'h4', 't', '')
                cmd = f'FT.AGGREGATE {idx} * GROUPBY 1 @t REDUCE COUNT 0 AS count'.split(' ')
                expected = [
                    ANY, \
                    ['t', '', 'count', '2'], \
                    ['t', 'movie', 'count', '2']
                ]
                cmd_assert(env, cmd, expected)

            # --------------------------- SEPARATOR --------------------------------
            # Remove added documents
            for i in range(2, 5):
                conn.execute_command('DEL', f'h{i}')

            # Validate that separated empty fields are indexed as empty as well
            conn.execute_command('HSET', 'h5', 't', ', bar')
            conn.execute_command('HSET', 'h6', 't', 'bat, ')
            conn.execute_command('HSET', 'h7', 't', 'bat,')
            conn.execute_command('HSET', 'h8', 't', 'bat, , bat2')
            conn.execute_command('HSET', 'h9', 't', ',')
            cmd = f'FT.SEARCH {idx} isempty(@t) SORTBY t ASC WITHCOUNT'.split(' ')
            expected = [
                6,
                'h1', ['t', '', 'text', 'hello'],
                'h9', ['t', ','],
                'h5', ['t', ', bar'],
                'h7', ['t', 'bat,'],
                'h6', ['t', 'bat, '],
                'h8', ['t', 'bat, , bat2']
            ]
            cmd_assert(env, cmd, expected)

            # Make sure we don't index h5, h6, h7 in case of a non-empty indexing
            # tag field
            env.cmd('FT.CREATE', 'temp_idx', 'SCHEMA', 't', 'TAG')
            cmd = f'FT.SEARCH temp_idx isempty(@t) SORTBY t ASC'.split(' ')
            expected = [0]
            cmd_assert(env, cmd, expected)
            env.cmd('FT.DROPINDEX', 'temp_idx')

            # ------------------------ Priority vs. Intersection -----------------------
            res = env.cmd('FT.SEARCH', idx, 'isempty(@t) -isempty(@t)')
            env.assertEqual(res, [0])

            res = env.cmd('FT.SEARCH', idx, '-isempty(@t) isempty(@t)')
            env.assertEqual(res, [0])

            res = env.cmd('FT.EXPLAINCLI', idx, '-isempty(@t) isempty(@t)')
            expected = [
                'INTERSECT {',
                '  NOT{',
                '    TAG:@t {',
                '      <ISEMPTY>', '    }',
                '  }',
                '  TAG:@t {',
                '    <ISEMPTY>',
                '  }',
                '}',
                ''
            ]
            env.assertEqual(res, expected)

            res = env.cmd('FT.EXPLAINCLI', idx, '@t:{bar} | @t:{foo} isempty(@t)')
            expected = [
                'UNION {',
                '  TAG:@t {',
                '    bar',
                '  }',
                '  INTERSECT {',
                '    TAG:@t {',
                '      foo',
                '    }',
                '    TAG:@t {',
                '      <ISEMPTY>',
                '    }',
                '  }',
                '}',
                ''
            ]
            env.assertEqual(res, expected)

            res = env.cmd('FT.EXPLAINCLI', idx, '-@t:{bar} | @t:{foo} isempty(@t)')
            expected = [
                'UNION {',
                '  NOT{',
                '    TAG:@t {',
                '      bar',
                '    }',
                '  }',
                '  INTERSECT {',
                '    TAG:@t {',
                '      foo',
                '    }',
                '    TAG:@t {',
                '      <ISEMPTY>',
                '    }',
                '  }',
                '}',
                ''
            ]
            env.assertEqual(res, expected)

            res = env.cmd('FT.EXPLAINCLI', idx, '@t:{bar} | -@t:{foo} -isempty(@t)')
            expected = [
                'UNION {',
                '  TAG:@t {',
                '    bar',
                '  }',
                '  INTERSECT {',
                '    NOT{',
                '      TAG:@t {',
                '        foo',
                '      }',
                '    }',
                '    NOT{',
                '      TAG:@t {',
                '        <ISEMPTY>',
                '      }',
                '    }',
                '  }',
                '}',
                ''
            ]
            env.assertEqual(res, expected)

            res = env.cmd('FT.EXPLAINCLI', idx, '-isempty(@t) isempty(@t) | @t:{bar}')
            expected = [
                'UNION {',
                '  INTERSECT {',
                '    NOT{',
                '      TAG:@t {',
                '        <ISEMPTY>',
                '      }',
                '    }',
                '    TAG:@t {',
                '      <ISEMPTY>',
                '    }',
                '  }',
                '  TAG:@t {',
                '    bar',
                '  }',
                '}',
                ''
            ]
            env.assertEqual(res, expected)

            res = env.cmd('FT.EXPLAINCLI', idx, 'isempty(@t) | -@t:{bar} -isempty(@t)')
            expected = [
                'UNION {',
                '  TAG:@t {',
                '    <ISEMPTY>',
                '  }',
                '  INTERSECT {',
                '    NOT{',
                '      TAG:@t {',
                '        bar',
                '      }',
                '    }',
                '    NOT{',
                '      TAG:@t {',
                '        <ISEMPTY>',
                '      }',
                '    }',
                '  }',
                '}',
                ''
            ]
            env.assertEqual(res, expected)

            res = env.cmd('FT.EXPLAINCLI', idx, '-isempty(@t) | -@t:{bar} isempty(@t)')
            expected = [
                'UNION {',
                '  NOT{',
                '    TAG:@t {',
                '      <ISEMPTY>',
                '    }',
                '  }',
                '  INTERSECT {',
                '    NOT{',
                '      TAG:@t {',
                '        bar',
                '      }',
                '    }',
                '    TAG:@t {',
                '      <ISEMPTY>',
                '    }',
                '  }',
                '}',
                ''
            ]
            env.assertEqual(res, expected)

        # Create an index with a TAG field, that also indexes empty strings, another
        # TAG field that doesn't index empty values, and a TEXT field
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'text', 'TEXT').ok()
        conn = getConnectionByEnv(env)
        conn.execute_command('HSET', 'h1', 't', '')
        testHashIndex(env, 'idx')
        env.flush()

        # ----------------------------- SORTABLE case ------------------------------
        # Create an index with a SORTABLE TAG field, that also indexes empty strings
        env.expect('FT.CREATE', 'idx_sortable', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'SORTABLE', 'text', 'TEXT').ok()
        conn.execute_command('HSET', 'h1', 't', '')

        testHashIndex(env, 'idx_sortable')
        env.flush()

        # --------------------------- WITHSUFFIXTRIE case --------------------------
        # Create an index with a TAG field, that also indexes empty strings, while
        # using a suffix trie
        env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'WITHSUFFIXTRIE', 'text', 'TEXT').ok()
        conn.execute_command('HSET', 'h1', 't', '')
        testHashIndex(env, 'idx_suffixtrie')
        env.flush()

        # ---------------------------------- JSON ----------------------------------
        def testJSONIndex(env, idx, dialect):
            conn = getConnectionByEnv(env)

            # Populate the db with a document that has an empty TAG field
            empty_j = {
            't': ''
            }
            empty_js = json.dumps(empty_j, separators=(',', ':'))
            env.expect('JSON.SET', 'j', '$', empty_js).equal('OK')

            # Search for a single document, via its indexed empty value
            cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
            if dialect >= 3:
                empty_js = json.dumps([empty_j], separators=(',', ':'))
            expected = [1, 'j', ['$', empty_js]]
            cmd_assert(env, cmd, expected)

            # Multi-value
            j = {
                't': ['a', '', 'c']
            }
            js = json.dumps(j, separators=(',', ':'))
            conn.execute_command('JSON.SET', 'j', '$', js)
            cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
            if dialect >= 3:
                js = json.dumps([j], separators=(',', ':'))
            expected = [1, 'j', ['$', js]]
            cmd_assert(env, cmd, expected)

            # Empty array
            # On sortable case, empty arrays are not indexed (MOD-6936)
            if idx != 'jidx_sortable':
                j = {
                    't': []
                }
                js = json.dumps(j, separators=(',', ':'))
                conn.execute_command('JSON.SET', 'j', '$', js)
                cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
                if dialect >= 3:
                    js = json.dumps([j], separators=(',', ':'))
                expected = [1, 'j', ['$', js]]
                cmd_assert(env, cmd, expected)

            # Empty object
            j = {
                't': {}
            }
            js = json.dumps(j, separators=(',', ':'))
            conn.execute_command('JSON.SET', 'j', '$', js)
            cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
            if dialect >= 3:
                js = json.dumps([j], separators=(',', ':'))
            expected = [1, 'j', ['$', js]]
            cmd_assert(env, cmd, expected)

        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY').ok()
        testJSONIndex(env, 'jidx', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_sortable', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY', 'SORTABLE').ok()
        testJSONIndex(env, 'jidx_sortable', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_suffix', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY', 'WITHSUFFIXTRIE').ok()
        testJSONIndex(env, 'jidx_suffix', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$arr[*]', 'AS', 'arr', 'TAG', 'ISEMPTY').ok()
        # Empty array values ["a", "", "c"] with explicit array components indexing
        arr = {
            'arr': ['a', '', 'c']
        }
        arrs = json.dumps(arr, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', arrs).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@arr)'.split(' ')
        if dialect >= 3:
            arrs = json.dumps([arr], separators=(',', ':'))
        expected = [1, 'j', ['$', arrs]]
        cmd_assert(env, cmd, expected)

        # Empty arrays shouldn't be indexed for this indexing mechanism
        arr = {
            'arr': []
        }
        arrs = json.dumps(arr, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', arrs).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@arr)'.split(' ')
        expected = [0]
        cmd_assert(env, cmd, expected)
        conn.execute_command('DEL', 'j')

        # Empty object shouldn't be indexed for this indexing mechanism (flatten, [*])
        obj = {
            'arr': {}
        }
        objs = json.dumps(obj, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', objs).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@arr)'.split(' ')
        expected = [0]
        cmd_assert(env, cmd, expected)

        # Searching for emptiness of a non-existing field should return an error
        obj = {
            'obj': {}
        }
        objs = json.dumps(obj, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', objs).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@obj)'.split(' ')
        expected = [0]
        env.expect(*cmd).error().contains('Syntax error: Field not found')

        env.flush()

        # Embedded empty object
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t.b', 'AS', 'b', 'TAG', 'ISEMPTY').ok()
        j = {
            "t": {"b": {}}
        }
        js = json.dumps(j, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', js).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@b)'.split(' ')
        if dialect >= 3:
            js = json.dumps([j], separators=(',', ':'))
        expected = [1, 'j', ['$', js]]
        cmd_assert(env, cmd, expected)

        # Embedded empty array
        j = {
            "t": {"b": []}
        }
        js = json.dumps(j, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', js).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@b)'.split(' ')
        if dialect >= 3:
            js = json.dumps([j], separators=(',', ':'))
        expected = [1, 'j', ['$', js]]
        cmd_assert(env, cmd, expected)

        env.flush()

        # An attempt to index a non-empty object as a TAG should fail (coverage)
        j = {
            "t": {"lala": "lali"}
        }
        js = json.dumps(j)
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TAG', 'ISEMPTY').ok()
        env.expect('JSON.SET', 'j', '$', js).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@t)'.split(' ')
        cmd_assert(env, cmd, [0])

        # Make sure we experienced an indexing failure, via `FT.INFO`
        info = index_info(env, 'jidx')
        env.assertEqual(info['hash_indexing_failures'], 1)

        env.flush()

        # Test that when we index many docs, we find the wanted portion of them upon
        # empty value indexing
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'ISEMPTY').ok()
        n_docs = 1000
        for i in range(n_docs):
            conn.execute_command('HSET', f'h{i}', 't', '' if i % 2 == 0 else f'{i}')
        res = env.cmd('FT.SEARCH', 'idx', 'isempty(@t)', 'WITHCOUNT', 'LIMIT', '0', '0')
        env.assertEqual(int(res[0]), 500)

def testTagAutoescaping(env):
    
    env = Env(moduleArgs = 'DEFAULT_DIALECT 5')

    # Create sample data
    env.cmd('HSET', '{doc}:1', 'tag', 'abc:1', 'id', '1')
    env.cmd('HSET', '{doc}:2', 'tag', 'xyz:2', 'id', '2')
    # two tags separated by comma
    env.cmd('HSET', '{doc}:3', 'tag', 'xyz:2,abc:1', 'id', '3')
    env.cmd('HSET', '{doc}:4', 'tag', 'abc:1-xyz:2', 'id', '4')
    env.cmd('HSET', '{doc}:5', 'tag', 'joe@mail.com', 'id', '5')
    env.cmd('HSET', '{doc}:6', 'tag', 'tag with {brackets}', 'id', '6')
    env.cmd('HSET', '{doc}:7', 'tag', 'abc:1|xyz:2', 'id', '7')
    # tag with octal number: '_12\100' == '_12@'
    env.cmd('HSET', '{doc}:8', 'tag', '_12\100', 'id', '8')
    env.cmd('HSET', '{doc}:9', 'tag', '-99999', 'id', '9')
    env.cmd('HSET', '{doc}:10', 'tag', 'ab(12)', 'id', '10')
    env.cmd('HSET', '{doc}:11', 'tag', 'a|b-c d', 'id', '11')
    # this test generates the tag: '_@12\\345'
    env.cmd('HSET', '{doc}:12', 'tag', '_@12\\345', 'id', '12')
    env.cmd('HSET', '{doc}:13', 'tag', '$literal', 'id', '13')
    # tags with leading and trailing spaces
    env.cmd('HSET', '{doc}:14', 'tag', '  with: space  ', 'id', '14')
    env.cmd('HSET', '{doc}:15', 'tag', '  leading:space', 'id', '15')
    env.cmd('HSET', '{doc}:16', 'tag', 'trailing:space  ', 'id', '16')
    # short tags
    env.cmd('HSET', '{doc}:17', 'tag', 'x', 'id', '17')
    env.cmd('HSET', '{doc}:18', 'tag', 'w', 'id', '18')
    env.cmd('HSET', '{doc}:19', 'tag', "w'", 'id', '19')
    env.cmd('HSET', '{doc}:20', 'tag', "w''", 'id', '20')
    # tag matching wildcard format
    env.cmd('HSET', '{doc}:21', 'tag', "w'?*1'", 'id', '21')

    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', '{doc}:',
               'SCHEMA', 'tag', 'TAG', 'id', 'NUMERIC', 'SORTABLE').ok()
    waitForIndex(env, 'idx')

    # Test exact match
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1}', 'NOCONTENT',
                  'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:1', '{doc}:3'])

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1|xyz:2}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:7'])

    # with dialect < 5, the pipe is an OR operator
    expected_result = [3, '{doc}:1', '{doc}:2', '{doc}:3']
    for dialect in [1, 2, 3, 4]:
        res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc\:1|xyz\:2}', 'NOCONTENT',
                      'SORTBY', 'id', 'ASC', 'DIALECT', dialect)
        env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{_12\100}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:8'])

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{_12@}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:8'])

    # escape character (backslash '\') is still a special character that needs
    # escaping
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{_@12\\\\345}')
    env.assertEqual(res, [1, '{doc}:12', ['tag', '_@12\\345', 'id', '12']])

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{ab(12)}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:10'])

    # Test tag with '-'
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1-xyz:2}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:4'])

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{-99999}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:9'])

    # Test tag with '|' and ' '
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{a|b-c d}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:11'])

    # AND Operator (INTERSECT queries)
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1} @tag:{xyz:2}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:3'])

    # Negation Queries (using dash "-")
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1} -@tag:{xyz:2}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:1'])

    # OR Operator (UNION queries)
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1-xyz:2} | @tag:{joe@mail.com}',
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:4', '{doc}:5'])

    # Optional Queries (using tiled "~")
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1} ~@tag:{xyz:2}',
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:1', '{doc}:3'])

    # Test exact match with brackets
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{tag with {brackets\\}}',
                  'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:6'])

    # Search with attributes
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{xyz:2}=>{$weight:5.0}',
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:2', '{doc}:3'])

    res = env.cmd('FT.SEARCH', 'idx',
                  '(@tag:{xyz:2} | @tag:{abc:1}) => { $weight: 5.0; }',
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [3, '{doc}:1', '{doc}:2', '{doc}:3'])

    res = env.cmd('FT.SEARCH', 'idx', 
                  '((@tag:{xyz:2}  @tag:{abc:1}) | @tag1:{val:3} (@tag2:{joe@mail.com} => { $weight:0.3 } )) => { $weight:0.2 }',
                  'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:3'])

    # Test prefix
    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{a-b-c*}')
    env.assertEqual(res, "TAG:@tag {\n  PREFIX{a-b-c*}\n}\n")

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{a-b-c\\*}')
    env.assertEqual(res, 'TAG:@tag {\n  a-b-c*\n}\n')

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{abc\\*yxv*}')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{abc*yxv*}\n}\n')

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{abc:?\\*yxv*}=>{$weight:3.4}',
                  'PARAMS', '2', 'abc', 'hello')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{abc:?*yxv*}\n} => { $weight: 3.4; }\n')

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{abc:?*}=>{$weight:3.4}',
                  'PARAMS', '2', 'abc', 'hello')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{abc:?*}\n} => { $weight: 3.4; }\n')

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{$abc*}=>{$weight:3.4}',
                  'PARAMS', '2', 'abc', 'hello')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{hello*}\n} => { $weight: 3.4; }\n')

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{abc:*}=>{$weight:3.4}',
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [4, '{doc}:1', '{doc}:3', '{doc}:4', '{doc}:7'])

    # Test suffix
    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{*a-b-c}')
    env.assertEqual(res, "TAG:@tag {\n  SUFFIX{*a-b-c}\n}\n")

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{\\*a-b-c}')
    env.assertEqual(res, 'TAG:@tag {\n  *a-b-c\n}\n')

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{*abc\\*yxv}')
    env.assertEqual(res, 'TAG:@tag {\n  SUFFIX{*abc*yxv}\n}\n')

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{*xyz:2}=>{$weight:3.4}',
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [4, '{doc}:2', '{doc}:3', '{doc}:4', '{doc}:7'])

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{*$param}',
                  'PARAMS', '2', 'param', 'xyz:2',
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [4, '{doc}:2', '{doc}:3', '{doc}:4', '{doc}:7'])

    # Test infix
    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{*a-b-c*}')
    env.assertEqual(res, "TAG:@tag {\n  INFIX{*a-b-c*}\n}\n")

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{\\*a-b-c\\*}')
    env.assertEqual(res, 'TAG:@tag {\n  *a-b-c*\n}\n')

    res = env.cmd('FT.EXPLAIN', 'idx', '@tag:{*abc\\*yxv:*}')
    env.assertEqual(res, 'TAG:@tag {\n  INFIX{*abc*yxv:*}\n}\n')

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{*@mail.*}=>{$weight:3.4}',
                  'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:5'])

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{*$param*}=>{$weight:3.4}',
                  'PARAMS', '2', 'param', '@mail.', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:5'])

    # if '$' is escaped, it is treated as a regular character, and the parameter
    # is not replaced
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{*\$param*}=>{$weight:3.4}',
                  'PARAMS', '2', 'param', '@mail.', 'NOCONTENT')
    env.assertEqual(res, [0])

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{*\$literal*}',
                  'PARAMS', '2', 'literal', '@mail.', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:13'])

    # Test wildcard
    res = env.cmd('FT.EXPLAIN', 'idx', "@tag:{w'-@??'}")
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{-@??}\n}\n")

    res = env.cmd('FT.EXPLAIN', 'idx', "@tag1:{w'$param'}",
                  'PARAMS', '2', 'param', 'hello world')
    env.assertEqual(res, "TAG:@tag1 {\n  WILDCARD{hello world}\n}\n")

    res = env.cmd('FT.EXPLAIN', 'idx',
                  "@tag1:{w'foo*:-;bar?'}=>{$weight:3.4; $inorder: true;}",
                  'PARAMS', '2', 'param', 'hello world')
    env.assertEqual(res, "TAG:@tag1 {\n  WILDCARD{foo*:-;bar?}\n} => { $weight: 3.4; $inorder: true; }\n")

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{w'*:1?xyz:*'}=>{$weight:3.4;}",
                  'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:4', '{doc}:7'])

    # wildcard including single quote
    res = env.cmd('FT.EXPLAIN', 'idx', "@tag:{w'a\\'bc'}")
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{a'bc}\n}\n")

    # wildcard with leading and trailing spaces are valid, spaces are ignored
    res = env.cmd('FT.EXPLAIN', 'idx', "@tag:{w'?*1'}")
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{?*1}\n}\n")
    
    res2 = env.cmd('FT.EXPLAIN', 'idx', "@tag:{  w'?*1'}")
    env.assertEqual(res, res2)

    res2 = env.cmd('FT.EXPLAIN', 'idx', "@tag:{w'?*1'  }")
    env.assertEqual(res, res2)

    res2 = env.cmd('FT.EXPLAIN', 'idx', "@tag:{     w'?*1'  }")
    env.assertEqual(res, res2)

    # Test escaped wildcards which become tags
    res = env.cmd('FT.EXPLAIN', 'idx', "@tag:{\\w'?*1'}")
    env.assertEqual(res, "TAG:@tag {\n  w'?*1'\n}\n")
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{\\w'?*1'}", 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:21'])

    res = env.cmd('FT.EXPLAIN', 'idx', "(@tag:{w'-abc})")
    env.assertEqual(res, "TAG:@tag {\n  w'-abc\n}\n")

    res = env.cmd('FT.EXPLAIN', 'idx', "@tag:{w\'???1a}")
    env.assertEqual(res, "TAG:@tag {\n  w'???1a\n}\n")

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{w'?'}", 'SORTBY', 'id', 'ASC',
                  'NOCONTENT')
    env.assertEqual(res, [2, '{doc}:17', '{doc}:18'])

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{w''}")
    env.assertEqual(res, [1, '{doc}:20', ['tag', "w''", 'id', '20']])

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{w'}")
    env.assertEqual(res, [1, '{doc}:19', ['tag', "w'", 'id', '19']])

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{w'?'} -@tag:{w'w'}")
    env.assertEqual(res, [1, '{doc}:17', ['tag', 'x', 'id', '17']])

    # Test tags with leading and trailing spaces
    expected_result = [1, '{doc}:14', ['tag', '  with: space  ', 'id', '14']]

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{  with: space  }")
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{*with: space*}")
    env.assertEqual(res, expected_result)

    # leading spaces of the prefix are ignored
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{              with: space*}")
    env.assertEqual(res, expected_result)

    # invalid because the '*' is not the last character, then it is treated as a
    # regular character and should be escaped
    env.expect('FT.SEARCH', 'idx', "@tag:{   with: space* }").error()\
        .contains('Syntax error')

    # trailing spaces of the suffix are ignored
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{*with: space              }")
    env.assertEqual(res, expected_result)

    # invalid because the '*' is not the first character, then it is treated as a
    # regular character and should be escaped
    env.expect('FT.SEARCH', 'idx', "@tag:{   *with: space}").error()\
        .contains('Syntax error')

    # This returns 0 because the query is looking for a tag with a leading
    # space but the leading space was removed upon data ingestion
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{* with: space}")
    env.assertEqual(res, [0])
    res = env.cmd('FT.EXPLAINCLI', 'idx', "@tag:{* with: space}")
    env.assertEqual(res, ['TAG:@tag {', '  SUFFIX{* with: space}', '}', ''])

    # This returns 0 because the query is looking for a tag with a trailing
    # space but the trailing space was removed upon data ingestion
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{with: space *}")
    env.assertEqual(res, [0])
    res = env.cmd('FT.EXPLAINCLI', 'idx', "@tag:{with: space *}")
    env.assertEqual(res, ['TAG:@tag {', '  PREFIX{with: space *}', '}', ''])

    # This returns 0 because the query is looking for a tag with leading and
    # trailing spaces but the spaces were removed upon data ingestion
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{* with: space *}")
    env.assertEqual(res, [0])
    res = env.cmd('FT.EXPLAINCLI', 'idx', "@tag:{* with: space *}")
    env.assertEqual(res, ['TAG:@tag {', '  INFIX{* with: space *}', '}', ''])

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{$param}",
                  'PARAMS', '2', 'param', 'with: space')
    env.assertEqual(res, expected_result)

    # Test tags with leading spaces
    expected_result = [1, '{doc}:15', ['tag', '  leading:space', 'id', '15']]

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{  leading*}")
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{  leading:space}")
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{*eading:space}")
    env.assertEqual(res, expected_result)

    # This returns 0 because the query is looking for a tag with a leading
    # space but the leading space was removed upon data ingestion
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{* leading:space}")
    env.assertEqual(res, [0])

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{$param}",
                  'PARAMS', '2', 'param', 'leading:space')
    env.assertEqual(res, expected_result)

    # Test tags with trailing spaces
    expected_result = [1, '{doc}:16', ['tag', 'trailing:space  ', 'id', '16']]

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{trailing*}")
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{trailing:spac*}")
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', 'idx', "@tag:{trailing:space  }")
    env.assertEqual(res, expected_result)

    # This returns 0 because the query is looking for a tag with a trailing
    # space but the trailing space was removed upon data ingestion
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{trailing:space *}")
    env.assertEqual(res, [0])

def testDialect5InvalidSyntax(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 5')

    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', '{doc}:',
               'SCHEMA', 'tag', 'TAG', 'SORTABLE', 'id',
               'NUMERIC', 'SORTABLE').ok()

    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.EXPLAIN', 'idx', "@tag:{w\\'?*1'}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', '@tag:{abc:1\\}')

    # wildcard and prefix
    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', "@tag:{w'1?'*}")

    # wildcard with trailing spaces and prefix
    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', "@tag:{w'1?'   *}")

    # suffix and wildcard
    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', "@tag:{*w'1?'}")

    # wildcard and contains
    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', "@tag:{*w'1?'*}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', "@tag:{*\\w'abc'\\*}")
    
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @tag:{w'-abc*}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', "(@tag:{\\w'-abc*})")
         
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx '@tag:{*w'-abc*}'")

    # escaping an invalid wildcard
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx '@tag:{\\w-:abc}")
    
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx '@tag:{\\w'-:abc}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @t1:(%)")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @t1:(|)")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @t1:({)")

    # test punct character
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.explaincli idx @t1:>")

    # test cntrl character
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.explaincli idx @t1:\10")

def testTagUNF(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 5')

    # Create index without UNF
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', '{doc}:',
               'SCHEMA', 'tag', 'TAG', 'SORTABLE').ok()
    
    # Create index with UNF
    env.expect('FT.CREATE', 'idx_unf', 'ON', 'HASH', 'PREFIX', '1', '{doc}:',
               'SCHEMA', 'tag', 'TAG', 'SORTABLE', 'UNF').ok()

    # Create sample data
    env.cmd('HSET', '{doc}:1', 'tag', 'america')
    env.cmd('HSET', '{doc}:2', 'tag', 'aMerica')
    env.cmd('HSET', '{doc}:3', 'tag', 'America')

    # Without UNF, the tags are normalized and the results are sorted by key
    res = env.cmd('FT.SEARCH', 'idx', '@tag:{america}', 'NOCONTENT',
                  'SORTBY', 'tag', 'ASC', 'WITHCOUNT')
    env.assertEqual(res, [3, '{doc}:1', '{doc}:2', '{doc}:3'])

    # Without UNF, the results are normalized and are grouped
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@tag',
                  'REDUCE', 'COUNT', '0')
    env.assertEqual(res[0], 1)

    # With UNF (un-normalized form), the normalization is disabled and the tags
    # are sorted by its original form
    res = env.cmd('FT.SEARCH', 'idx_unf', '@tag:{america}', 'NOCONTENT',
                  'SORTBY', 'tag', 'WITHCOUNT')
    env.assertEqual(res, [3, '{doc}:3', '{doc}:2', '{doc}:1'])

    # With UNF, the results are not normalized and are not grouped
    res = env.cmd('FT.AGGREGATE', 'idx_unf', '*', 'GROUPBY', '1', '@tag',
                  'REDUCE', 'COUNT', '0')
    env.assertEqual(res[0], 3)
