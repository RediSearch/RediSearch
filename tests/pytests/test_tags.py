# -*- coding: utf-8 -*-

from RLTest import Env
from includes import *
from common import *

def testTagIndex(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH','schema', 'title', 'text', 'tags', 'tag').ok()
    N = 10
    con = env.getClusterConnectionIfNeeded()
    for n in range(N):
        env.assertOk(con.execute_command('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                         'title', 'hello world term%d' % n, 'tags', 'foo bar,xxx,tag %d' % n))
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
                'ft.search', 'idx', r'@tags:{tag\ %d}' % n, 'nocontent')
            env.assertEqual(1, res[0])

            res = env.cmd(
                'ft.search', 'idx', r'hello world @tags:{tag\ %d|tag %d}' % (n, n + 1), 'nocontent')
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

    env.assertOk(env.getClusterConnectionIfNeeded().execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'tags', 'x:hello world: fooz bar:foo,bar:BOO FAR'))
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for q in ('@tags:{hello world}', '@tags:{fooz bar}', r'@tags:{foo\,bar}', r'@tags:{boo\ far}', '@tags:{x}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(1, res[0])

@skip(cluster=True)
def testTagPrefix(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'tags', 'tag', 'separator', ',').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields', 'title', 'hello world',
               'tags', 'hello world,hello-world,hell,jell').ok()
    env.expect(debug_cmd(), 'dump_tagidx', 'idx', 'tags')    \
        .equal([['hell', [1]], ['hello world', [1]], ['hello-world', [1]], ['jell', [1]]])

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for q in ('@tags:{hello world}', '@tags:{hel*}', r'@tags:{hello\-*}', '@tags:{he*}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(res[0], 1)

def testTagFieldCase(env):
    dialect = env.cmd(config_cmd(), 'GET', 'DEFAULT_DIALECT')[0][1]
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'TAgs', 'tag').ok()

    env.expect(env.getClusterConnectionIfNeeded().execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'TAgs', 'HELLO WORLD,FOO BAR'))
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        env.assertEqual([1, 'doc1'], env.cmd(
            'FT.SEARCH', 'idx', '@TAgs:{HELLO WORLD}', 'NOCONTENT'))
        env.assertEqual([1, 'doc1'], env.cmd(
            'FT.SEARCH', 'idx', '@TAgs:{foo bar}', 'NOCONTENT'))

        # Bad queries
        if dialect == '1':
            env.assertEqual([0], env.cmd(
                'FT.SEARCH', 'idx', '@tags:{HELLO WORLD}'))
            env.assertEqual([0], env.cmd(
                'FT.SEARCH', 'idx', '@TAGS:{foo bar}', 'NOCONTENT'))
        else:
            env.expect('FT.SEARCH', 'idx', '@tags:{HELLO WORLD}').error().contains('Unknown field')
            env.expect('FT.SEARCH', 'idx', '@TAGS:{foo bar}').error().contains('Unknown field')

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
    con = env.getClusterConnectionIfNeeded()

    N = 100
    alltags = set()
    for n in range(N):
        tags = (f'foo {n}', f'bar {n}', 'x')
        alltags.add(tags[0])
        alltags.add(tags[1])
        alltags.add(tags[2])

        env.assertOk(con.execute_command('ft.add', 'idx', f'doc{n}', 1.0, 'fields',
                   'tags', ','.join(tags), 'othertags', f'baz {int(n // 2)}'))
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
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(con.execute_command('FT.ADD', 'myIdx', 'doc2', '1.0', 'FIELDS', 'title', '"work"'))
    with env.assertResponseError():
        con.execute_command('FT.ADD', 'myIdx', 'doc2', '1.0', 'FIELDS', 'title', '"hello"')
    env.assertOk(con.execute_command('FT.ADD', 'myIdx', 'doc3', '1.0', 'FIELDS', 'title', '"hello"'))
    env.assertOk(con.execute_command('FT.ADD', 'myIdx', 'doc1', '1.0', 'FIELDS', 'title', '"hello,work"'))
    expectedRes = {'doc2': ['0', ['title', '"work"']], 'doc3' : ['0', ['title', '"hello"']],
                   'doc1' : ['0', ['title', '"hello,work"']]}
    res = env.cmd('ft.search', 'myIdx', '~@title:{wor} ~@title:{hell}', 'WITHSCORES')[1:]
    res = {res[i]:res[i + 1: i + 3] for i in range(0, len(res), 3)}
    env.assertEqual(res, expectedRes)

@skip(cluster=True)
def testTagIndex_OnReopen(env:Env): # issue MOD-8011
    n_docs_per_tag_block = 1000
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG').ok()
    # Add a first tag
    env.cmd('HSET', 'first', 't', 'bar')
    # Add 2 blocks of documents with the same tag
    for i in range(n_docs_per_tag_block * 2):
        env.cmd('HSET', f'doc{i}', 't', 'foo')

    # Search for both tags, read first + more than 1 block of the second
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '@t:{bar|foo}', 'LOAD', '*', 'WITHCURSOR', 'COUNT', int(1 + n_docs_per_tag_block * 1.5))
    env.assertEqual(res[1], ['t', 'bar']) # First tag
    env.assertNotEqual(cursor, 0) # Not done, we have more results to read from the second block of the second tag

    # Delete the first tag + first block of the second tag
    env.expect('DEL', 'first').equal(1)
    for i in range(n_docs_per_tag_block):
        env.expect('DEL', f'doc{i}').equal(1)
    forceInvokeGC(env) # Trigger GC to remove the inverted index of `bar` and the first block of `foo`

    # Read from the cursor, should not crash
    env.expect('FT.CURSOR', 'READ', 'idx', cursor).noError().equal([ANY, 0]) # cursor is done

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
        conn.execute_command(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
        env.expect(debug_cmd(), 'dump_tagidx', 'idx1', 't').equal([['foo', [1, 2, 3]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx2', 't').equal([['FOO', [1, 2]], ['foo', [1, 3]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx3', 't').equal([['foo', [2, 3]], ['foo,foo', [1]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx4', 't').equal([['FOO', [2]], ['foo', [3]], ['foo,FOO', [1]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx5', 't').equal([['FOO', [2]], ['foo', [3]], ['foo,FOO', [1]]])

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

        env.expect(debug_cmd(), 'dump_tagidx', 'idx1', 't').equal([['f o', [4, 5, 6]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx2', 't').equal([['F O', [4, 5]], ['f o', [4, 6]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx3', 't').equal([['f o', [5, 6]], ['f o,f o', [4]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx4', 't').equal([['F O', [5]], ['f o', [6]], ['f o,F O', [4]]])
        env.expect(debug_cmd(), 'dump_tagidx', 'idx5', 't').equal([['F O', [5]], ['f o', [6]], ['f o,F O', [4]]])

    # not casesensitive
    env.expect('FT.SEARCH', 'idx1', r'@t:{F\ O}')         \
        .equal([3, 'doc1', ['t', 'f o,F O'], 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])
    env.expect('FT.SEARCH', 'idx1', r'@t:{f\ o}')         \
        .equal([3, 'doc1', ['t', 'f o,F O'], 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])

    # casesensitive
    env.expect('FT.SEARCH', 'idx2', r'@t:{F\ O}')         \
        .equal([2, 'doc1', ['t', 'f o,F O'], 'doc2', ['t', 'F O']])
    env.expect('FT.SEARCH', 'idx2', r'@t:{f\ o}')         \
        .equal([2, 'doc1', ['t', 'f o,F O'], 'doc3', ['t', 'f o']])

    # not casesensitive
    env.expect('FT.SEARCH', 'idx3', r'@t:{f\ o\,f\ o}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx3', r'@t:{f\ o\,F\ O}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx3', r'@t:{F\ O\,F\ O}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx3', r'@t:{F\ O}')         \
        .equal([2, 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])
    env.expect('FT.SEARCH', 'idx3', r'@t:{f\ o}')         \
        .equal([2, 'doc2', ['t', 'F O'], 'doc3', ['t', 'f o']])

    # casesensitive
    env.expect('FT.SEARCH', 'idx4', r'@t:{f\ o\,f\ o}')         \
        .equal([0])
    env.expect('FT.SEARCH', 'idx4', r'@t:{f\ o\,F\ O}')         \
        .equal([1, 'doc1', ['t', 'f o,F O']])
    env.expect('FT.SEARCH', 'idx4', r'@t:{F\ O\,F\ O}')         \
        .equal([0])
    env.expect('FT.SEARCH', 'idx4', r'@t:{F\ O}')         \
        .equal([1, 'doc2', ['t', 'F O']])
    env.expect('FT.SEARCH', 'idx4', r'@t:{f\ o}')         \
        .equal([1, 'doc3', ['t', 'f o']])

@skip(cluster=True)
def testTagGCClearEmpty(env):

    conn = getConnectionByEnv(env)
    conn.execute_command(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'bar')
    conn.execute_command('HSET', 'doc3', 't', 'baz')
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([['bar', [2]], ['baz', [3]], ['foo', [1]]])
    env.expect('FT.SEARCH', 'idx', '@t:{foo}').equal([1, 'doc1', ['t', 'foo']])

    # delete two tags
    conn.execute_command('DEL', 'doc1')
    conn.execute_command('DEL', 'doc2')
    forceInvokeGC(env, 'idx')
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([['baz', [3]]])
    env.expect('FT.SEARCH', 'idx', '@t:{foo}').equal([0])

    # delete last tag
    conn.execute_command('DEL', 'doc3')
    forceInvokeGC(env, 'idx')
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([])

    # check term can be used after being empty
    conn.execute_command('HSET', 'doc4', 't', 'foo')
    conn.execute_command('HSET', 'doc5', 't', 'foo')
    env.expect('FT.SEARCH', 'idx', '@t:{foo}')  \
        .equal([2, 'doc4', ['t', 'foo'], 'doc5', ['t', 'foo']])

@skip(cluster=True)
def testTagGCClearEmptyWithCursor(env):

    conn = getConnectionByEnv(env)
    conn.execute_command(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'foo')
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([['foo', [1, 2]]])

    n_docs = 2
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '@t:{foo}', 'WITHCURSOR', 'COUNT', '1')
    env.assertEqual(res, [n_docs, []])

    # delete both documents and run the GC to clean 'foo' inverted index
    env.expect('DEL', 'doc1').equal(1)
    env.expect('DEL', 'doc2').equal(1)

    forceInvokeGC(env, 'idx')

    # make sure the inverted index was cleaned
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([])

    # read from the cursor
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
    env.assertEqual(res, [n_docs])
    env.assertEqual(cursor, 0)

@skip(cluster=True)
def testTagGCClearEmptyWithCursorAndMoreData(env):

    conn = getConnectionByEnv(env)
    conn.execute_command(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'foo')
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([['foo', [1, 2]]])

    n_docs = 2
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '@t:{foo}', 'WITHCURSOR', 'COUNT', '1')
    env.assertEqual(res, [n_docs, []])

    # delete both documents and run the GC to clean 'foo' inverted index
    env.expect('DEL', 'doc1').equal(1)
    env.expect('DEL', 'doc2').equal(1)

    forceInvokeGC(env, 'idx')

    # make sure the inverted index was cleaned
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([])

    # add data
    conn.execute_command('HSET', 'doc3', 't', 'foo')
    conn.execute_command('HSET', 'doc4', 't', 'foo')
    n_docs = 2
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([['foo', [3, 4]]])

    # read from the cursor
    res, cursor = conn.execute_command('FT.CURSOR', 'READ', 'idx', cursor)
    env.assertEqual(res, [n_docs])
    env.assertEqual(cursor, 0)

    # ensure later documents with same tag are read
    res = conn.execute_command('FT.AGGREGATE', 'idx', '@t:{foo}')
    env.assertEqual(res, [n_docs, [], []])

@skip(cluster=True)
def testEmptyTagLeak(env):

    cycles = 1
    tags = 30

    conn = getConnectionByEnv(env)
    env.cmd(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    pl = conn.pipeline()

    for i in range(cycles):
        for j in range(tags):
            x = j + i * tags
            pl.execute_command('HSET', f'doc{x}', 't', f'tag{x}')
        pl.execute()
        for j in range(tags):
            pl.execute_command('DEL', f'doc{j + i * tags}')
        pl.execute()
    forceInvokeGC(env, 'idx')
    env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([])

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

def _testDialect2TagExact(env, idx):
    """Test exact match on tags with dialect 2."""

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
    env.cmd('HSET', '{doc}:12', 'tag', r'_@12\345', 'id', '12')
    env.cmd('HSET', '{doc}:13', 'tag', '$literal', 'id', '13')
    env.cmd('HSET', '{doc}:14', 'tag', '*literal', 'id', '14')
    # tags with leading and trailing spaces
    env.cmd('HSET', '{doc}:15', 'tag', '  with: space  ', 'id', '15')
    env.cmd('HSET', '{doc}:16', 'tag', '  leading:space', 'id', '16')
    env.cmd('HSET', '{doc}:17', 'tag', 'trailing:space  ', 'id', '17')
    # short tags
    env.cmd('HSET', '{doc}:18', 'tag', 'x', 'id', '18')
    env.cmd('HSET', '{doc}:19', 'tag', 'w', 'id', '19')
    env.cmd('HSET', '{doc}:20', 'tag', "w'", 'id', '20')
    env.cmd('HSET', '{doc}:21', 'tag', "w''", 'id', '21')
    # tag matching wildcard format
    env.cmd('HSET', '{doc}:22', 'tag', "w'?*1'", 'id', '22')
    # tag without special characters
    env.cmd('HSET', '{doc}:23', 'tag', "hello world", 'id', '23')
    env.cmd('HSET', '{doc}:24', 'tag', "hello", 'id', '24')

    # Test exact match
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1"}', 'NOCONTENT',
                'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:1', '{doc}:3'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1|xyz:2"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:7'])

    # Test exact match with escaped '$' and '*' characters
    res = env.cmd('FT.SEARCH', idx, '@tag:{"$literal"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:13'])

    res = env.cmd('FT.SEARCH', idx, r'@tag:{\*literal}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:14'])

    # with dialect < 5, the pipe is an OR operator
    expected_result = [3, '{doc}:1', '{doc}:2', '{doc}:3']
    for dialect in [1, 2, 3, 4]:
        res = env.cmd('FT.SEARCH', idx, r'@tag:{abc\:1|xyz\:2}', 'NOCONTENT',
                    'SORTBY', 'id', 'ASC', 'DIALECT', dialect)
        env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', idx, '@tag:{"_12@"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:8'])

    # escape character (backslash '\')
    res = env.cmd('FT.SEARCH', idx, r'@tag:{"_@12\345"}')
    env.assertEqual(res, [1, '{doc}:12', ['tag', r'_@12\345', 'id', '12']])

    res = env.cmd('FT.SEARCH', idx, '@tag:{"ab(12)"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:10'])

    # Test tag with '-'
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1-xyz:2"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:4'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{-99999}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:9'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{"-99999"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:9'])

    # Test tag with '|' and ' '
    res = env.cmd('FT.SEARCH', idx, '@tag:{"a|b-c d"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:11'])

    # AND Operator (INTERSECT queries)
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1"} @tag:{"xyz:2"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:3'])

    # Negation Queries (using dash "-")
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1"} -@tag:{"xyz:2"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:1'])

    # OR Operator (UNION queries)
    expected = [2, '{doc}:4', '{doc}:5']
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1-xyz:2"} | @tag:{"joe@mail.com"}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1-xyz:2"|"joe@mail.com"}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1-xyz:2" | "joe@mail.com"}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)

    expected = [3, '{doc}:2', '{doc}:3', '{doc}:23']
    res = env.cmd('FT.SEARCH', idx, '@tag:{"xyz:2" | hello world}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', idx, '@tag:{hello world | "xyz:2"}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)

    expected = [3, '{doc}:2', '{doc}:3', '{doc}:24']
    res = env.cmd('FT.SEARCH', idx, '@tag:{"xyz:2" | hello}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', idx, '@tag:{hello | "xyz:2"}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)

    expected = [4, '{doc}:2', '{doc}:3', '{doc}:23', '{doc}:24']
    res = env.cmd('FT.SEARCH', idx, '@tag:{"xyz:2" | hello | hello world}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', idx, '@tag:{hello | "xyz:2" | hello world}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, expected)

    # Optional Queries (using tilde "~")
    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:1"} ~@tag:{"xyz:2"}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:1', '{doc}:3'])

    # Test exact match with brackets
    res = env.cmd('FT.SEARCH', idx, '@tag:{"tag with {brackets}"}',
                'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:6'])

    # Search with attributes
    res = env.cmd('FT.SEARCH', idx, '@tag:{"xyz:2"}=>{$weight:5.0}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:2', '{doc}:3'])

    res = env.cmd('FT.SEARCH', idx,
                '(@tag:{"xyz:2"} | @tag:{"abc:1"}) => { $weight: 5.0; }',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [3, '{doc}:1', '{doc}:2', '{doc}:3'])

    res = env.cmd('FT.SEARCH', idx,
                '(@tag:{"xyz:2"}  @tag:{"abc:1"}) => { $weight:0.2 }',
                'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:3'])

    # Test prefix
    res = env.cmd('FT.EXPLAIN', idx, '@tag:{"a-b-c"*}')
    env.assertEqual(res, "TAG:@tag {\n  PREFIX{a-b-c*}\n}\n")

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{"a-b-c*"}')
    env.assertEqual(res, 'TAG:@tag {\n  a-b-c*\n}\n')

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{"abc*yxv"*}')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{abc*yxv*}\n}\n')

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{"abc:?*yxv"*}=>{$weight:3.4}',
                'PARAMS', '2', 'abc', 'hello')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{abc:?*yxv*}\n} => { $weight: 3.4; }\n')

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{"abc:?"*}=>{$weight:3.4}',
                'PARAMS', '2', 'abc', 'hello')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{abc:?*}\n} => { $weight: 3.4; }\n')

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{$abc*}=>{$weight:3.4}',
                'PARAMS', '2', 'abc', 'hello')
    env.assertEqual(res, 'TAG:@tag {\n  PREFIX{hello*}\n} => { $weight: 3.4; }\n')

    res = env.cmd('FT.SEARCH', idx, '@tag:{"abc:"*}=>{$weight:3.4}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [4, '{doc}:1', '{doc}:3', '{doc}:4', '{doc}:7'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{"*liter"*}', 'NOCONTENT',)
    env.assertEqual(res, [1, '{doc}:14'])

    # Test suffix
    res = env.cmd('FT.EXPLAIN', idx, '@tag:{*"a-b-c"}')
    env.assertEqual(res, "TAG:@tag {\n  SUFFIX{*a-b-c}\n}\n")

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{"*a-b-c"}')
    env.assertEqual(res, 'TAG:@tag {\n  *a-b-c\n}\n')

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{*"abc*yxv"}')
    env.assertEqual(res, 'TAG:@tag {\n  SUFFIX{*abc*yxv}\n}\n')

    res = env.cmd('FT.SEARCH', idx, '@tag:{*"xyz:2"}=>{$weight:3.4}',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [4, '{doc}:2', '{doc}:3', '{doc}:4', '{doc}:7'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{*"*literal"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:14'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{*$param}',
                'PARAMS', '2', 'param', 'xyz:2',
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [4, '{doc}:2', '{doc}:3', '{doc}:4', '{doc}:7'])

    # Test infix
    res = env.cmd('FT.EXPLAIN', idx, '@tag:{*"a-b-c"*}')
    env.assertEqual(res, "TAG:@tag {\n  INFIX{*a-b-c*}\n}\n")

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{"*a-b-c*"}')
    env.assertEqual(res, 'TAG:@tag {\n  *a-b-c*\n}\n')

    res = env.cmd('FT.EXPLAIN', idx, '@tag:{*"abc*yxv:"*}')
    env.assertEqual(res, 'TAG:@tag {\n  INFIX{*abc*yxv:*}\n}\n')

    res = env.cmd('FT.SEARCH', idx, '@tag:{*"@mail."*}=>{$weight:3.4}',
                'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:5'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{*"*literal"*}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:14'])

    res = env.cmd('FT.SEARCH', idx, '@tag:{*$param*}=>{$weight:3.4}',
                'PARAMS', '2', 'param', '@mail.', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:5'])

    # if '$' is escaped, it is treated as a regular character, and the parameter
    # is not replaced
    res = env.cmd('FT.SEARCH', idx, r'@tag:{*\$param*}=>{$weight:3.4}',
                'PARAMS', '2', 'param', '@mail.', 'NOCONTENT')
    env.assertEqual(res, [0])

    res = env.cmd('FT.SEARCH', idx, r'@tag:{*\$literal*}',
                'PARAMS', '2', 'literal', '@mail.', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:13'])

    # Test wildcard
    res = env.cmd('FT.EXPLAIN', idx, "@tag:{w'-@??'}")
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{-@??}\n}\n")

    res = env.cmd('FT.EXPLAIN', idx, "@tag:{w'$param'}",
                'PARAMS', '2', 'param', 'hello world')
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{hello world}\n}\n")

    res = env.cmd('FT.EXPLAIN', idx,
                "@tag:{w'foo*:-;bar?'}=>{$weight:3.4; $inorder: true;}",
                'PARAMS', '2', 'param', 'hello world')
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{foo*:-;bar?}\n} => { $weight: 3.4; $inorder: true; }\n")

    res = env.cmd('FT.SEARCH', idx, "@tag:{w'*:1?xyz:*'}=>{$weight:3.4;}",
                'NOCONTENT', 'SORTBY', 'id', 'ASC')
    env.assertEqual(res, [2, '{doc}:4', '{doc}:7'])

    # wildcard including single quote
    res = env.cmd('FT.EXPLAIN', idx, r"@tag:{w'a\'bc'}")
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{a'bc}\n}\n")

    # wildcard with leading and trailing spaces are valid, spaces are ignored
    res = env.cmd('FT.EXPLAIN', idx, "@tag:{w'?*1'}")
    env.assertEqual(res, "TAG:@tag {\n  WILDCARD{?*1}\n}\n")

    res2 = env.cmd('FT.EXPLAIN', idx, "@tag:{  w'?*1'}")
    env.assertEqual(res, res2)

    res2 = env.cmd('FT.EXPLAIN', idx, "@tag:{w'?*1'  }")
    env.assertEqual(res, res2)

    res2 = env.cmd('FT.EXPLAIN', idx, "@tag:{     w'?*1'  }")
    env.assertEqual(res, res2)

    # Test escaped wildcards which become tags
    res = env.cmd('FT.EXPLAIN', idx, r'@tag:{"w\'?*1\'"}')
    env.assertEqual(res, "TAG:@tag {\n  w'?*1'\n}\n")
    res = env.cmd('FT.SEARCH', idx, r'@tag:{"w\'?*1\'"}', 'NOCONTENT')
    env.assertEqual(res, [1, '{doc}:22'])

    res = env.cmd('FT.EXPLAIN', idx, r'(@tag:{"w\'-abc"})')
    env.assertEqual(res, "TAG:@tag {\n  w'-abc\n}\n")

    res = env.cmd('FT.EXPLAIN', idx, r'@tag:{"w\'???1a"}')
    env.assertEqual(res, "TAG:@tag {\n  w'???1a\n}\n")

    res = env.cmd('FT.SEARCH', idx, "@tag:{w'?'}", 'SORTBY', 'id', 'ASC',
                'NOCONTENT')
    env.assertEqual(res, [2, '{doc}:18', '{doc}:19'])

    # This is a tag, not a wildcard, because there is no text enclosed
    # in the quotes
    res = env.cmd('FT.SEARCH', idx, r'@tag:{"w\'\'"}')
    env.assertEqual(res, [1, '{doc}:21', ['tag', "w''", 'id', '21']])

    res = env.cmd('FT.SEARCH', idx, r'@tag:{"w\'"}')
    env.assertEqual(res, [1, '{doc}:20', ['tag', "w'", 'id', '20']])

    res = env.cmd('FT.SEARCH', idx, "@tag:{w'?'} -@tag:{w'w'}")
    env.assertEqual(res, [1, '{doc}:18', ['tag', 'x', 'id', '18']])

    # Test tags with leading and trailing spaces
    expected_result = [1, '{doc}:15', ['tag', '  with: space  ', 'id', '15']]

    res = env.cmd('FT.SEARCH', idx, '@tag:{  "with: space"  }')
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', idx, '@tag:{*"with: space"*}')
    env.assertEqual(res, expected_result)

    # leading spaces of the prefix are ignored
    res = env.cmd('FT.SEARCH', idx, '@tag:{              "with: space"*}')
    env.assertEqual(res, expected_result)

    # valid, characters before the quotes and after the star are ignored
    res = env.cmd('FT.SEARCH', idx, '@tag:{   "with: space"* }')
    env.assertEqual(res, expected_result)

    # trailing spaces of the suffix are ignored
    res = env.cmd('FT.SEARCH', idx, '@tag:{*"with: space"              }')
    env.assertEqual(res, expected_result)

    # valid, characters before the star are ignored
    res = env.cmd('FT.SEARCH', idx, '@tag:{   *"with: space"}')
    env.assertEqual(res, expected_result)

    # This returns 0 because the query is looking for a tag with a leading
    # space but the leading space was removed upon data ingestion
    res = env.cmd('FT.SEARCH', idx, '@tag:{*" with: space"}')
    env.assertEqual(res, [0])
    res = env.cmd('FT.EXPLAINCLI', idx, '@tag:{*" with: space"}')
    env.assertEqual(res, ['TAG:@tag {', '  SUFFIX{* with: space}', '}', ''])

    # This returns 0 because the query is looking for a tag with a trailing
    # space but the trailing space was removed upon data ingestion
    res = env.cmd('FT.SEARCH', idx, '@tag:{"with: space "*}')
    env.assertEqual(res, [0])
    res = env.cmd('FT.EXPLAINCLI', idx, '@tag:{"with: space "*}')
    env.assertEqual(res, ['TAG:@tag {', '  PREFIX{with: space *}', '}', ''])

    # This returns 0 because the query is looking for a tag with leading and
    # trailing spaces but the spaces were removed upon data ingestion
    res = env.cmd('FT.SEARCH', idx, '@tag:{*" with: space "*}')
    env.assertEqual(res, [0])
    res = env.cmd('FT.EXPLAINCLI', idx, '@tag:{*" with: space "*}')
    env.assertEqual(res, ['TAG:@tag {', '  INFIX{* with: space *}', '}', ''])

    res = env.cmd('FT.SEARCH', idx, "@tag:{$param}",
                'PARAMS', '2', 'param', 'with: space')
    env.assertEqual(res, expected_result)

    # Test tags with leading spaces
    expected_result = [1, '{doc}:16', ['tag', '  leading:space', 'id', '16']]

    res = env.cmd('FT.SEARCH', idx, "@tag:{  leading*}")
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', idx, '@tag:{"leading:space"}')
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', idx, '@tag:{*"eading:space"}')
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', idx, "@tag:{$param}",
                'PARAMS', '2', 'param', 'leading:space')
    env.assertEqual(res, expected_result)

    # Test tags with trailing spaces
    expected_result = [1, '{doc}:17', ['tag', 'trailing:space  ', 'id', '17']]

    res = env.cmd('FT.SEARCH', idx, "@tag:{trailing*}")
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', idx, '@tag:{"trailing:spac"*}')
    env.assertEqual(res, expected_result)

    res = env.cmd('FT.SEARCH', idx, '@tag:{"trailing:space"}')
    env.assertEqual(res, expected_result)

def testDialect2TagExactOptimized():
    """Test exact match with dialect 2 using the existing-index optimization."""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    # Create another index with the optimization of using the existing-index ON
    env.expect('FT.CREATE', 'idxOptimized', 'INDEXALL', 'ENABLE', 'ON', 'HASH', 'PREFIX', '1', '{doc}:',
               'SCHEMA', 'tag', 'TAG', 'id', 'NUMERIC', 'SORTABLE').ok()
    _testDialect2TagExact(env, 'idxOptimized')

def testDialect2TagExact():
    """Test exact match with dialect 2."""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', '{doc}:',
               'SCHEMA', 'tag', 'TAG', 'id', 'NUMERIC', 'SORTABLE').ok()
    _testDialect2TagExact(env, 'idx')

def testDialect2InvalidSyntax():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')

    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', '{doc}:',
               'SCHEMA', 'tag', 'TAG', 'SORTABLE', 't1', 'TEXT', 'id',
               'NUMERIC', 'SORTABLE').ok()

    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.EXPLAIN', 'idx', r"@tag:{w\'?*1'}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', r'@tag:{abc:1\}')

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
        env.cmd('FT.SEARCH', 'idx', r"@tag:{*\w'abc'\*}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @tag:{w'-abc*}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd('FT.SEARCH', 'idx', r"(@tag:{\w'-abc*})")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx '@tag:{*w'-abc*}'")

    # escaping an invalid wildcard
    with env.assertResponseError(contained='Syntax error'):
        env.cmd(r"FT.SEARCH idx '@tag:{\w-:abc}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd(r"FT.SEARCH idx '@tag:{\w'-:abc}")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @t1:(%)")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @t1:(|)")

    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.SEARCH idx @t1:({)")

    # test punct character
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.EXPLAIN idx @t1:>")

    # test cntrl character
    with env.assertResponseError(contained='Syntax error'):
        env.cmd("FT.EXPLAIN idx @t1:\10")

def testDialect2SpecialChars():
    """Test search with punct characters with dialect 2."""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    # punct = [!-/:-@[-‘{-~]
    punct_1 = list(range(ord('!'), ord('/') + 1))  # Characters from '!' to '/'
    punct_2 = list(range(ord(':'), ord('@') + 1))  # Characters from ':' to '@'
    punct_3 = list(range(ord('['), ord('`') + 1))  # Characters from '[' to '`'
    punct_4 = list(range(ord('{'), ord('~') + 1))  # Characters from '{' to '~'
    punct = punct_1 + punct_2 + punct_3 + punct_4

    # Create docs with a text containing the punct characters
    for c in punct:
        conn.execute_command("HSET", f"doc{ord(chr(c))}", "text",
                             f"single\\{chr(c)}term")

    # Create docs without special characters
    conn.execute_command("HSET", "doc0", "text", "hanoi")
    conn.execute_command("HSET", "doc999", "text", "two words")

    # Query for a single term, where the punct character is escaped
    for c in punct:
        expected = [1, f"doc{ord(chr(c))}"]

        # Test exact match
        cmd = f"FT.SEARCH idx @text:(single\\{chr(c)}term) NOCONTENT"
        res = env.execute_command(cmd)
        env.assertEqual(res, expected)

        # Test prefix
        if chr(c) != '\\':
            cmd = f"FT.SEARCH idx @text:(single\\{chr(c)}*) NOCONTENT"
        else:
            # TODO: why do I need to use the 't' after the backslash?
            cmd = f"FT.SEARCH idx @text:(single\\\\t*) NOCONTENT"
        res = env.execute_command(cmd)
        env.assertEqual(res, expected)

        # Test suffix
        cmd = f"FT.SEARCH idx @text:(*\\{chr(c)}term) NOCONTENT"
        res = env.execute_command(cmd)
        env.assertEqual(res, expected)

        # Test infix
        cmd = f"FT.SEARCH idx @text:(*le\\{chr(c)}te*) NOCONTENT"
        res = env.execute_command(cmd)
        env.assertEqual(res, expected)

        # Test INTERSECTION operator
        res = env.execute_command("FT.SEARCH", "idx",
                      f"@text:(single\\{chr(c)}term single* *term)", "NOCONTENT")
        env.assertEqual(res, expected)

        # Test UNION operator
        res = conn.execute_command("FT.SEARCH", "idx",
                      f"@text:(single\\{chr(c)}term | hanoi)", "NOCONTENT",
                      "SORTBY", "text", "ASC")
        env.assertEqual(res, [2, "doc0", f"doc{ord(chr(c))}"])

    # Test queries where the punct character is NOT escaped
    expected = [1, 'doc999', ['text', 'two words']]
    expected_explain = [
        '@text:INTERSECT {',
        '  @text:UNION {',
        '    @text:two',
        '    @text:+two(expanded)',
        '  }',
        '  @text:UNION {',
        '    @text:words',
        '    @text:+word(expanded)',
        '    @text:word(expanded)',
        '  }',
        '}',
        ''
    ]
    for c in punct:
        char = chr(c)
        # skip characters which are not consumed by the lexer
        if char in {'"', '$', '%', '(', ')', '-', ':', ';', '@', '[', ']',
                    '_', '{', '}', '~', '|', '*'}:
            continue

        res = env.execute_command(
            "FT.SEARCH", "idx", f"@text:(two{char}words)")
        env.assertEqual(res, expected)

        res = env.execute_command(
            "FT.EXPLAINCLI", "idx", f"@text:(two{char}words)")
        env.assertEqual(res, expected_explain)

    # Test control characters
    for cntrl in range(1,32):
        res = env.execute_command(
            "FT.SEARCH", "idx", f"@text:(two{chr(cntrl)}words)")
        env.assertEqual(res, expected)

        res = env.execute_command(
            "FT.EXPLAINCLI", "idx", f"@text:(two{chr(cntrl)}words)")
        env.assertEqual(res, expected_explain)

def testTagUNF():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")

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
