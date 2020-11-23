from includes import *

def search(env, r, *args):
    return r.execute_command('ft.search', *args)

def testTagIndex(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag'))
    N = 10
    for n in range(N):

        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                       'title', 'hello world term%d' % n, 'tags', 'foo bar,xxx,tag %d' % n))
    for _ in r.retry_with_rdb_reload():
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
            env.assertEqual('doc%d' % n, res[2])
            env.assertEqual('doc%d' % (n + 1), res[1])

            res = env.cmd(
                'ft.search', 'idx', 'term%d @tags:{tag %d}' % (n, n), 'nocontent')
            env.assertEqual(1, res[0])
            env.assertEqual('doc%d' % n, res[1])

def testSeparator(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', ':'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'tags', 'x:hello world: fooz bar:foo,bar:BOO FAR'))
    for _ in r.retry_with_rdb_reload():

        for q in ('@tags:{hello world}', '@tags:{fooz bar}', '@tags:{foo\\,bar}', '@tags:{boo\\ far}', '@tags:{x}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(1, res[0])

def testTagPrefix(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', ','))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'tags', 'hello world,hello-world,hell,jell'))
    for _ in r.retry_with_rdb_reload():

        for q in ('@tags:{hello world}', '@tags:{hel*}', '@tags:{hello\\-*}', '@tags:{he*}'):
            res = env.cmd('ft.search', 'idx', q)
            env.assertEqual(res[0], 1)

def testTagFieldCase(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'TAgs', 'tag'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                   'title', 'hello world', 'TAgs', 'HELLO WORLD,FOO BAR'))
    for _ in r.retry_with_rdb_reload():

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
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator')
    with env.assertResponseError():
        r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', "foo")
    with env.assertResponseError():
        r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', "")

def testTagVals(env):
    r = env
    r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'othertags', 'tag')

    N = 100
    alltags = set()
    for n in range(N):
        tags = ('foo %d' % n, 'bar %d' % n, 'x')
        alltags.add(tags[0])
        alltags.add(tags[1])
        alltags.add(tags[2])

        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                       'tags', ','.join(tags), 'othertags', 'baz %d' % int(n // 2)))
    for _ in r.retry_with_rdb_reload():
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
    env.expect('FT.CREATE idx SCHEMA t TAG SORTABLE').ok()
    env.expect('FT.SEARCH idx @t:{val}').equal([0])

def testIssue1305(env):
    env.expect('FT.CREATE myIdx SCHEMA title TAG').ok()
    env.expect('FT.ADD myIdx doc2 1.0 FIELDS title "work"').ok()
    env.expect('FT.ADD myIdx doc2 1.0 FIELDS title "hello"').error()
    env.expect('FT.ADD myIdx doc3 1.0 FIELDS title "hello"').ok()
    env.expect('FT.ADD myIdx doc1 1.0 FIELDS title "hello,work"').ok()
    res_expected = [3L, 'doc1', 'inf', ['title', '"hello,work"'], 'doc3', 'inf', ['title', '"hello"'], 'doc2', 'inf', ['title', '"work"']]
    res_actual = env.cmd('ft.search', 'myIdx', '~@title:{wor} ~@title:{hell}', 'WITHSCORES')
    env.assertListEqual(sorted(res_actual), sorted(res_expected))

def test_1667(env):
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'text', 'TEXT')
  env.expect('ft.search idx @tag:{a}').equal([0L])
  env.expect('ft.search idx @tag:{b}').equal([0L])

  env.expect('FT.ADD idx doc 1.0 FIELDS tag a,b').ok()
  env.expect('FT.ADD idx doc1 1.0 FIELDS tag abc').ok()

  # test single stopword
  env.expect('ft.search idx @tag:{a}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{b}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c}').equal([0L])

  # test stopword in list
  env.expect('ft.search idx @tag:{a|c}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c|a}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c|a|c}').equal([1L, 'doc', ['tag', 'a,b']])

  # test stopword with prefix
  env.expect('ft.search idx @tag:{ab*}').equal([1L, 'doc1', ['tag', 'abc']])
  env.expect('ft.search idx @tag:{abc*}').equal([1L, 'doc1', ['tag', 'abc']])
  
  # ensure regular text field
  env.expect('FT.ADD idx doc_a 1.0 FIELDS text a').ok()
  env.expect('FT.ADD idx doc_b 1.0 FIELDS text b').ok()

  env.expect('ft.search idx a').equal([0L])
  env.expect('ft.search idx b').equal([1L, 'doc_b', ['text', 'b']])
