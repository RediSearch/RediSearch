from includes import *
from common import *
import csv
from RLTest import Env

def testWITHSUFFIXTRIEParamText(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'schema', 't', 'TEXT', 'SORTABLE', 'WITHSUFFIXTRIE').error()
    env.expect('ft.create', 'idx', 'schema', 't', 'TEXT', 'SORTABLE', 'NOSTEM').error() # sortable must be last

    # without sortable
    env.expect('ft.create', 'idx', 'schema', 't', 'TEXT', 'WITHSUFFIXTRIE').ok()
    res_info = [['identifier', 't', 'attribute', 't', 'type', 'TEXT', 'WEIGHT', '1', 'WITHSUFFIXTRIE']]
    assertInfoField(env, 'idx', 'attributes', res_info)

    # with sortable
    env.expect('ft.create', 'idx_sortable', 'schema', 't', 'TEXT', 'WITHSUFFIXTRIE', 'SORTABLE').ok()
    res_info = [['identifier', 't', 'attribute', 't', 'type', 'TEXT', 'WEIGHT', '1', 'SORTABLE', 'WITHSUFFIXTRIE']]
    assertInfoField(env, 'idx_sortable', 'attributes', res_info)

    # nostem 1st
    env.expect('ft.create', 'idx_nostem1', 'schema', 't', 'TEXT', 'WITHSUFFIXTRIE', 'NOSTEM').ok()
    res_info = [['identifier', 't', 'attribute', 't', 'type', 'TEXT', 'WEIGHT', '1', 'NOSTEM', 'WITHSUFFIXTRIE']]
    assertInfoField(env, 'idx_nostem1', 'attributes', res_info)

    # nostem 2nd
    env.expect('ft.create', 'idx_nostem2', 'schema', 't', 'TEXT', 'NOSTEM', 'WITHSUFFIXTRIE').ok()
    assertInfoField(env, 'idx_nostem2', 'attributes', res_info)

def testWITHSUFFIXTRIEParamTag(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'schema', 't', 'TAG', 'SORTABLE', 'WITHSUFFIXTRIE').error()
    env.expect('ft.create', 'idx', 'schema', 't', 'TAG', 'SORTABLE', 'CASESENSITIVE').error() # sortable must be last

    # without sortable
    env.expect('ft.create', 'idx', 'schema', 't', 'TAG', 'WITHSUFFIXTRIE').ok()
    res_info = [['identifier', 't', 'attribute', 't', 'type', 'TAG', 'SEPARATOR', ',', 'WITHSUFFIXTRIE']]
    assertInfoField(env, 'idx', 'attributes', res_info)

    # with sortable
    env.expect('ft.create', 'idx_sortable', 'schema', 't', 'TAG', 'WITHSUFFIXTRIE', 'SORTABLE').ok()
    res_info = [['identifier', 't', 'attribute', 't', 'type', 'TAG', 'SEPARATOR', ',', 'SORTABLE', 'WITHSUFFIXTRIE']]
    assertInfoField(env, 'idx_sortable', 'attributes', res_info)

    # with casesensitive - automatically set to UNF as well
    env.expect('ft.create', 'idx_casesensitive', 'schema', 't', 'TAG', 'WITHSUFFIXTRIE', 'CASESENSITIVE', 'SORTABLE').ok()
    res_info = [['identifier', 't', 'attribute', 't', 'type', 'TAG', 'SEPARATOR', ',', 'CASESENSITIVE', 'SORTABLE', 'UNF', 'WITHSUFFIXTRIE']]
    assertInfoField(env, 'idx_casesensitive', 'attributes', res_info)

def testBasicContains(env):
    conn = getConnectionByEnv(env)

    env.expect('ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text').ok()
    conn.execute_command('HSET', 'doc1', 'title', 'hello world', 'body', 'this is a test')

    # prefix
    res = env.cmd('ft.search', 'idx', 'worl*')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

    # suffix
    res = env.cmd('ft.search', 'idx', '*orld')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))
    env.expect('ft.search', 'idx', '*orl').equal([0])

    # contains
    res = env.cmd('ft.search', 'idx', '*orl*')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

@skip(cluster=True)
def testSanity(env: Env):
    env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
    env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 10000000).ok()
    item_qty = 1000

    index_list = ['idx_bf', 'idx_suffix']
    env.cmd('ft.create', 'idx_bf', 'SCHEMA', 't', 'TEXT')
    env.cmd('ft.create', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

    conn = getConnectionByEnv(env)

    pl = conn.pipeline()
    for i in range(item_qty):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()

    for i in range(2):
        #prefix
        env.expect('ft.search', index_list[i], 'f*', 'LIMIT', 0, 0).equal([4000])
        env.expect('ft.search', index_list[i], 'foo*', 'LIMIT', 0, 0).equal([4000])
        env.expect('ft.search', index_list[i], 'foo1*', 'LIMIT', 0, 0).equal([111])
        env.expect('ft.search', index_list[i], '*ooo1*', 'LIMIT', 0, 0).equal([222])

        # contains
        env.expect('ft.search', index_list[i], '*oo*', 'LIMIT', 0, 0).equal([4000])
        # 55x & x55 - 555
        env.expect('ft.search', index_list[i], '*55*', 'LIMIT', 0, 0).equal([76])
        # 555
        env.expect('ft.search', index_list[i], '*555*', 'LIMIT', 0, 0).equal([4])
        env.expect('ft.search', index_list[i], '*o55*', 'LIMIT', 0, 0).equal([44])
        env.expect('ft.search', index_list[i], '*oo55*', 'LIMIT', 0, 0).equal([33])
        env.expect('ft.search', index_list[i], '*oo555*', 'LIMIT', 0, 0).equal([3])

        # 23x & x23
        env.expect('ft.search', index_list[i], '*23*', 'LIMIT', 0, 0).equal([80])
        # 234
        env.expect('ft.search', index_list[i], '*234*', 'LIMIT', 0, 0).equal([4])
        env.expect('ft.search', index_list[i], '*o23*', 'LIMIT', 0, 0).equal([44])
        env.expect('ft.search', index_list[i], '*oo23*', 'LIMIT', 0, 0).equal([33])
        env.expect('ft.search', index_list[i], '*oo234*', 'LIMIT', 0, 0).equal([3])

        # suffix
        env.expect('ft.search', index_list[i], '*oo234', 'LIMIT', 0, 0).equal([3])
        env.expect('ft.search', index_list[i], '*234', 'LIMIT', 0, 0).equal([4])
        env.expect('ft.search', index_list[i], '*13', 'LIMIT', 0, 0).equal([40])

    # test timeout
    for i in range(item_qty, item_qty * 5):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()

    env.expect(config_cmd(), 'set', 'TIMEOUT', 1).ok()
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'FAIL').ok()
    env.expect('ft.search', index_list[0], 'foo*', 'LIMIT', 0, 0).error() \
      .contains('SEARCH_TIMEOUT Timeout limit was reached')
    env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0, 0).error() \
      .contains('SEARCH_TIMEOUT Timeout limit was reached')

@skip(cluster=True)
def testSanityTags(env):
    env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
    env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 10000000).ok()
    item_qty = 1000

    index_list = ['idx_bf', 'idx_suffix']
    env.cmd('ft.create', index_list[0], 'SCHEMA', 't', 'TAG')
    env.cmd('ft.create', index_list[1], 'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE')

    conn = getConnectionByEnv(env)

    pl = conn.pipeline()
    for i in range(item_qty):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()

    for i in range(len(index_list)):
        #prefix
        env.expect('ft.search', index_list[i], '@t:{f*}', 'LIMIT', 0, 0).equal([4000])
        env.expect('ft.search', index_list[i], '@t:{foo*}', 'LIMIT', 0, 0).equal([4000])
        env.expect('ft.search', index_list[i], '@t:{foo1*}', 'LIMIT', 0, 0).equal([111])
        env.expect('ft.search', index_list[i], '@t:{*ooo1*}', 'LIMIT', 0, 0).equal([222])

        # contains
        env.expect('ft.search', index_list[i], '@t:{*oo*}', 'LIMIT', 0, 0).equal([4000])
        # 55x & x55 - 555
        env.expect('ft.search', index_list[i], '@t:{*55*}', 'LIMIT', 0, 0).equal([76])
        # 555
        env.expect('ft.search', index_list[i], '@t:{*555*}', 'LIMIT', 0, 0).equal([4])
        env.expect('ft.search', index_list[i], '@t:{*o55*}', 'LIMIT', 0, 0).equal([44])
        env.expect('ft.search', index_list[i], '@t:{*oo55*}', 'LIMIT', 0, 0).equal([33])
        env.expect('ft.search', index_list[i], '@t:{*oo555*}', 'LIMIT', 0, 0).equal([3])

        # 23x & x23
        env.expect('ft.search', index_list[i], '@t:{*23*}', 'LIMIT', 0, 0).equal([80])
        # 234
        env.expect('ft.search', index_list[i], '@t:{*234*}', 'LIMIT', 0, 0).equal([4])
        env.expect('ft.search', index_list[i], '@t:{*o23*}', 'LIMIT', 0, 0).equal([44])
        env.expect('ft.search', index_list[i], '@t:{*oo23*}', 'LIMIT', 0, 0).equal([33])
        env.expect('ft.search', index_list[i], '@t:{*oo234*}', 'LIMIT', 0, 0).equal([3])

        # suffix
        env.expect('ft.search', index_list[i], '@t:{*oo234}', 'LIMIT', 0, 0).equal([3])
        env.expect('ft.search', index_list[i], '@t:{*234}', 'LIMIT', 0, 0).equal([4])
        env.expect('ft.search', index_list[i], '@t:{*13}', 'LIMIT', 0, 0).equal([40])

    # test timeout
    for i in range(item_qty, item_qty * 5):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()

    env.expect(config_cmd(), 'set', 'TIMEOUT', 1).ok()
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'FAIL').ok()
    env.expect('ft.search', index_list[0], '@t:{foo*}', 'LIMIT', 0, 0).error() \
      .contains('SEARCH_TIMEOUT Timeout limit was reached')
    env.expect('ft.search', index_list[1], '@t:{foo*}', 'LIMIT', 0, 0).error() \
      .contains('SEARCH_TIMEOUT Timeout limit was reached')

def testEscape(env):
  # this test check that `\*` is escaped correctly on contains queries
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')
  conn.execute_command('HSET', 'doc1', 't', '1foo1')
  conn.execute_command('HSET', 'doc2', 't', r'\*foo2')
  conn.execute_command('HSET', 'doc3', 't', r'3\*foo3')
  conn.execute_command('HSET', 'doc4', 't', r'4foo\*')
  conn.execute_command('HSET', 'doc5', 't', r'5foo\*5')
  all_docs = [5, 'doc1', ['t', '1foo1'], 'doc2', ['t', r'\*foo2'], 'doc3', ['t', r'3\*foo3'],
                 'doc4', ['t', r'4foo\*'], 'doc5', ['t', r'5foo\*5']]
  # contains
  res = env.cmd('ft.search', 'idx', '*foo*')
  env.assertEqual(toSortedFlatList(res), toSortedFlatList(all_docs))

  # prefix only
  env.expect('ft.search', 'idx', r'\*foo*').equal([1, 'doc2', ['t', r'\*foo2']])
  # suffix only
  env.expect('ft.search', 'idx', r'*foo\*').equal([1, 'doc4', ['t', r'4foo\*']])
  # none
  env.expect('ft.search', 'idx', r'\*foo\*').equal([0])

  env.expect('ft.search', 'idx', r'*\*foo\**').equal([0])


def test_misc1(env):
  # this test check that `\*` is escaped correctly on contains queries
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE', 'SORTABLE')

  conn.execute_command('HSET', 'doc1', 't', 'world')
  conn.execute_command('HSET', 'doc2', 't', 'keyword')
  conn.execute_command('HSET', 'doc3', 't', 'doctorless')
  conn.execute_command('HSET', 'doc4', 't', 'anteriorly')
  conn.execute_command('HSET', 'doc5', 't', 'colorlessness')
  conn.execute_command('HSET', 'doc6', 't', 'floorless')

  # prefix
  res = env.cmd('ft.search', 'idx', 'worl*')
  env.assertEqual(res[0:2], [1, 'doc1'])
  env.assertEqual(set(res[2]), set(['world', 't']))

  # contains
  res = env.cmd('ft.search', 'idx', '*orld*')
  env.assertEqual(res, [1, 'doc1', ['t', 'world']])

  res = env.cmd('ft.search', 'idx', '*orl*')
  actual_res = [5, 'doc1', ['t', 'world'], 'doc3', ['t', 'doctorless'],
                'doc4', ['t', 'anteriorly'], 'doc5', ['t', 'colorlessness'], 'doc6', ['t', 'floorless']]
  env.assertEqual(toSortedFlatList(res), toSortedFlatList(actual_res))

  res = env.cmd('ft.search', 'idx', '*or*')
  actual_res = [6, 'doc1', ['t', 'world'], 'doc2', ['t', 'keyword'], 'doc3', ['t', 'doctorless'],
                'doc4', ['t', 'anteriorly'], 'doc5', ['t', 'colorlessness'], 'doc6', ['t', 'floorless']]
  env.assertEqual(toSortedFlatList(res), toSortedFlatList(actual_res))

  # suffix
  res = env.cmd('ft.search', 'idx', '*orld')
  env.assertEqual(res, [1, 'doc1', ['t', 'world']])

  res = env.cmd('ft.search', 'idx', '*ess')
  actual_res = [3, 'doc3', ['t', 'doctorless'], 'doc5', ['t', 'colorlessness'], 'doc6', ['t', 'floorless']]
  env.assertEqual(toSortedFlatList(res), toSortedFlatList(actual_res))

  res = env.cmd('ft.search', 'idx', '*less')
  actual_res = [2, 'doc3', ['t', 'doctorless'], 'doc6', ['t', 'floorless']]
  env.assertEqual(toSortedFlatList(res), toSortedFlatList(actual_res))

@skip(cluster=True)
def testContainsGC(env):
  env.expect(config_cmd() + ' set FORK_GC_CLEAN_THRESHOLD 0').ok()

  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE', 'SORTABLE')

  conn.execute_command('HSET', 'doc1', 't', 'hello')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx')),
                  sorted(['ello', 'hello', 'llo', 'lo', 'o']))
  conn.execute_command('HSET', 'doc1', 't', 'world')

  forceInvokeGC(env, 'idx')

  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx')),
                  sorted(['d', 'ld', 'orld', 'rld', 'world']))

  conn.execute_command('HSET', 'doc2', 't', 'bold')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx')),
                  sorted(['bold', 'd', 'ld', 'old', 'orld', 'rld', 'world']))
  conn.execute_command('DEL', 'doc2')

  forceInvokeGC(env, 'idx')

  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx')),
                  sorted(['d', 'ld', 'orld', 'rld', 'world']))

@skip(cluster=True)
def testContainsGCTag(env):
  env.expect(config_cmd() + ' set FORK_GC_CLEAN_THRESHOLD 0').ok()

  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE', 'SORTABLE')

  conn.execute_command('HSET', 'doc1', 't', 'hello')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't')),
                  sorted(['ello', 'hello', 'llo', 'lo', 'o']))
  conn.execute_command('HSET', 'doc1', 't', 'world')

  forceInvokeGC(env, 'idx')

  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't')),
                  sorted(['d', 'ld', 'orld', 'rld', 'world']))

  conn.execute_command('HSET', 'doc2', 't', 'bold')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't')),
                  sorted(['bold', 'd', 'ld', 'old', 'orld', 'rld', 'world']))
  conn.execute_command('DEL', 'doc2')

  forceInvokeGC(env, 'idx')

  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't')),
                  sorted(['d', 'ld', 'orld', 'rld', 'world']))

@skip(cluster=True)
def testContainsDebugCommand(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT', 'WITHSUFFIXTRIE')
  env.expect(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 'field').error()  \
    .contains('Could not find given field in index spec')

  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 'text', 'TEXT')
  env.expect(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx_no').error()  \
    .contains('Index does not have suffix trie')

  conn.execute_command('FT.CREATE', 'idx_tag', 'SCHEMA', 'tag', 'TAG', 'WITHSUFFIXTRIE')
  env.expect(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 'tag_no').error() \
    .contains('Could not find given field in index spec')
  env.expect(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 'tag_no', 'tag_yes').error(). \
    contains('wrong number of arguments')

@skip(cluster=True)
def testContainsMixedWithSuffix(env):

  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'WITHSUFFIXTRIE', 't2', 'TEXT')
  conn.execute_command('HSET', 'doc1', 't1', 'hello', 't2', 'hello')
  conn.execute_command('HSET', 'doc1', 't1', 'hello', 't2', 'hello')

  env.expect('ft.search', 'idx', '@t1:*ell*', 'NOCONTENT').equal([1, 'doc1'])
  env.expect('ft.search', 'idx', '@t2:*ell*', 'NOCONTENT').error()  \
    .contains('Contains query on fields without WITHSUFFIXTRIE support')

@skip(cluster=True)
def testSuffixTrieIncludesShortTagValue(env):
  # A 1-char tag value is stored in the suffix triemap and removed cleanly
  # after the document is deleted and the GC runs.
  env.expect(config_cmd() + ' set FORK_GC_CLEAN_THRESHOLD 0').ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE')

  conn.execute_command('HSET', 'doc1', 't', 'a')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't')),
                  sorted(['a']))

  conn.execute_command('DEL', 'doc1')
  forceInvokeGC(env, 'idx')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't')),
                  sorted([]))

@skip(cluster=True)
def testSuffixTrieIncludesSingleRuneMultiByteText(env):
  # A single-rune CJK term is stored in the suffix trie even though its byte
  # length differs from its rune length.
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

  conn.execute_command('HSET', 'doc1', 't', '中')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx')),
                  sorted(['中']))

@skip(cluster=True)
def testSuffixTrieIncludesLengthOneSubSuffix(env):
  # For a multi-char term, the length-1 sub-suffix (the term's last char) is
  # stored alongside the longer sub-suffixes. This is what lets `*x*` and `*x`
  # queries hit the suffix DS directly without a fallback.
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

  conn.execute_command('HSET', 'doc1', 't', 'banana')
  env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx')),
                  sorted(['a', 'ana', 'anana', 'banana', 'na', 'nana']))

@skip(cluster=True)
def testSuffixTrieFindsShortAsciiTag(env):
  # With MINPREFIX=1, a 1-char suffix query finds tag values that end with
  # that character, with and without WITHSUFFIXTRIE.
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TAG')

  conn.execute_command('HSET', 'doc:1', 't', 'banana')

  env.expect('FT.SEARCH', 'idx_w',  '@t:{*a}', 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', '@t:{*a}', 'NOCONTENT').equal([1, 'doc:1'])

@skip(cluster=True)
def testSuffixTrieFindsShortAsciiText(env):
  # With MINPREFIX=1, a 1-char suffix query finds text terms that end with
  # that character, with and without WITHSUFFIXTRIE.
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TEXT')

  conn.execute_command('HSET', 'doc:1', 't', 'banana')

  env.expect('FT.SEARCH', 'idx_w',  '*a', 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', '*a', 'NOCONTENT').equal([1, 'doc:1'])

@skip(cluster=True)
def testSuffixTrieFindsMultiByteRuneText(env):
  # A single-rune CJK suffix query finds text terms that end with that rune,
  # with and without WITHSUFFIXTRIE. At default MINPREFIX=2, "中" is 3 bytes
  # so the query passes MINPREFIX, but it's only 1 rune.
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TEXT')

  conn.execute_command('HSET', 'doc:1', 't', 'ba中')

  env.expect('FT.SEARCH', 'idx_w',  '*中', 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', '*中', 'NOCONTENT').equal([1, 'doc:1'])

@skip(cluster=True)
def testSuffixTrieFindsShortAsciiContainsText(env):
  # With MINPREFIX=1, a 1-char contains query finds text terms containing
  # that character, with and without WITHSUFFIXTRIE.
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TEXT')

  conn.execute_command('HSET', 'doc:1', 't', 'banana')

  env.expect('FT.SEARCH', 'idx_w',  '*a*', 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', '*a*', 'NOCONTENT').equal([1, 'doc:1'])

@skip(cluster=True)
def testSuffixTrieFindsShortAsciiContainsTag(env):
  # With MINPREFIX=1, a 1-char contains query finds tag values containing
  # that character, with and without WITHSUFFIXTRIE.
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TAG')

  conn.execute_command('HSET', 'doc:1', 't', 'banana')

  env.expect('FT.SEARCH', 'idx_w',  '@t:{*a*}', 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', '@t:{*a*}', 'NOCONTENT').equal([1, 'doc:1'])

@skip(cluster=True)
def testSuffixTrieFindsMultiByteRuneContainsText(env):
  # A single-rune CJK contains query finds text terms containing that rune,
  # with and without WITHSUFFIXTRIE.
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TEXT')

  conn.execute_command('HSET', 'doc:1', 't', 'ba中')

  env.expect('FT.SEARCH', 'idx_w',  '*中*', 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', '*中*', 'NOCONTENT').equal([1, 'doc:1'])

@skip(cluster=True)
def testSuffixTrieMixedSchemaShortToken(env):
  # In a mixed schema (one TEXT field with WITHSUFFIXTRIE, one without), a
  # short-token contains/suffix query is served exclusively from the suffix
  # DS — which is built only from suffix-enabled fields. Explicitly targeting
  # the non-suffix field is rejected; all-mask queries never surface terms
  # that live only in the non-suffix field.
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                       't1', 'TEXT', 'WITHSUFFIXTRIE',
                       't2', 'TEXT')

  # 'apple' lives in t1 (in the suffix DS); 'orange' lives only in t2.
  conn.execute_command('HSET', 'doc:1', 't1', 'apple', 't2', 'orange')

  # @t1 short-token queries: served by the suffix DS.
  env.expect('FT.SEARCH', 'idx', '@t1:*e',  'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx', '@t1:*p*', 'NOCONTENT').equal([1, 'doc:1'])

  # @t2 (no WITHSUFFIXTRIE) is rejected for contains/suffix queries.
  env.expect('FT.SEARCH', 'idx', '@t2:*e',  'NOCONTENT').error() \
    .contains('Contains query on fields without WITHSUFFIXTRIE support')
  env.expect('FT.SEARCH', 'idx', '@t2:*o*', 'NOCONTENT').error() \
    .contains('Contains query on fields without WITHSUFFIXTRIE support')

  # All-mask: finds via t1 (apple ends in 'e' / contains 'p'), but does not
  # surface 'orange' even though it contains 'o' — orange is not in the suffix DS.
  env.expect('FT.SEARCH', 'idx', '*e',  'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx', '*p*', 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx', '*o*', 'NOCONTENT').equal([0])

@skip(cluster=True)
def testSuffixTrieMultipleSuffixFieldsShortToken(env):
  # Two TEXT fields both with WITHSUFFIXTRIE share `spec->suffix`. Field-scoped
  # short-token queries narrow correctly to each field's terms; all-mask
  # queries find terms contributed by either field.
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                       't1', 'TEXT', 'WITHSUFFIXTRIE',
                       't2', 'TEXT', 'WITHSUFFIXTRIE')

  conn.execute_command('HSET', 'doc:1', 't1', 'alpha', 't2', 'bravo')

  # Field-scoped: only the term in that field is reachable.
  env.expect('FT.SEARCH', 'idx', '@t1:*a',  'NOCONTENT').equal([1, 'doc:1'])  # alpha
  env.expect('FT.SEARCH', 'idx', '@t1:*o',  'NOCONTENT').equal([0])
  env.expect('FT.SEARCH', 'idx', '@t2:*o',  'NOCONTENT').equal([1, 'doc:1'])  # bravo
  env.expect('FT.SEARCH', 'idx', '@t2:*a',  'NOCONTENT').equal([0])

  # All-mask: both terms are in the shared suffix DS, so either rune matches.
  env.expect('FT.SEARCH', 'idx', '*a',  'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx', '*o',  'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx', '*v*', 'NOCONTENT').equal([1, 'doc:1'])      # bravo contains 'v'
  env.expect('FT.SEARCH', 'idx', '*l*', 'NOCONTENT').equal([1, 'doc:1'])      # alpha contains 'l'

@skip(cluster=True)
def testSuffixTrieFindsMultiTokenShortPatternText(env):
  # Verbatim wildcards (`w'...'`, dialect 2+) with multiple length-1 tokens
  # between `*`s used to return 0 results on WITHSUFFIXTRIE fields:
  # Suffix_ChooseToken filtered every candidate token and the suffix DS bailed
  # out. With the floor gone, the chooser picks a token and the iteration
  # finds the matching term — same recall as a plain field.
  env.expect(config_cmd(), 'set', 'DEFAULT_DIALECT', 2).ok()
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TEXT')

  conn.execute_command('HSET', 'doc:1', 't', 'rabbit')  # has 'a' followed by 'b'
  conn.execute_command('HSET', 'doc:2', 't', 'lemon')   # no 'a', no 'b'

  env.expect('FT.SEARCH', 'idx_w',  "w'*a*b*'", 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', "w'*a*b*'", 'NOCONTENT').equal([1, 'doc:1'])

  # No term contains both 'x' and 'y'.
  env.expect('FT.SEARCH', 'idx_w',  "w'*x*y*'", 'NOCONTENT').equal([0])
  env.expect('FT.SEARCH', 'idx_no', "w'*x*y*'", 'NOCONTENT').equal([0])

@skip(cluster=True)
def testSuffixTrieFindsMultiTokenShortPatternTag(env):
  # TAG analogue of testSuffixTrieFindsMultiTokenShortPatternText.
  env.expect(config_cmd(), 'set', 'DEFAULT_DIALECT', 2).ok()
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA', 't', 'TAG')

  conn.execute_command('HSET', 'doc:1', 't', 'rabbit')
  conn.execute_command('HSET', 'doc:2', 't', 'lemon')

  env.expect('FT.SEARCH', 'idx_w',  "@t:{w'*a*b*'}", 'NOCONTENT').equal([1, 'doc:1'])
  env.expect('FT.SEARCH', 'idx_no', "@t:{w'*a*b*'}", 'NOCONTENT').equal([1, 'doc:1'])

  env.expect('FT.SEARCH', 'idx_w',  "@t:{w'*x*y*'}", 'NOCONTENT').equal([0])
  env.expect('FT.SEARCH', 'idx_no', "@t:{w'*x*y*'}", 'NOCONTENT').equal([0])

@skip(cluster=True)
def testSuffixTrieNoEmptyDriftUnderIndexEmpty(env):
  # Regression for the INDEXEMPTY drift concern on PR #9945: the suffix DS gates
  # adds on `len == 0`, so an empty value is never stored in the suffix trie.
  # This must NOT change recall relative to an INDEXEMPTY field *without*
  # WITHSUFFIXTRIE: empty values are reachable only via exact match (which
  # bypasses the suffix DS), and `?`/`*` wildcards must treat the empty value
  # identically on both. We compare a suffix-trie field against a plain field,
  # both INDEXEMPTY, for exact-empty match and for `*`, `?*`, `*?*`, `*?`.
  env.expect(config_cmd(), 'set', 'DEFAULT_DIALECT', 2).ok()
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx_w',  'SCHEMA',
                       't',  'TAG',  'INDEXEMPTY', 'WITHSUFFIXTRIE',
                       'tx', 'TEXT', 'INDEXEMPTY', 'WITHSUFFIXTRIE')
  conn.execute_command('FT.CREATE', 'idx_no', 'SCHEMA',
                       't',  'TAG',  'INDEXEMPTY',
                       'tx', 'TEXT', 'INDEXEMPTY')

  conn.execute_command('HSET', 'doc:empty', 't', '',       'tx', '')
  conn.execute_command('HSET', 'doc:val',   't', 'banana', 'tx', 'banana')

  def search(idx, query):
    res = conn.execute_command('FT.SEARCH', idx, query, 'NOCONTENT')
    return [res[0], *sorted(res[1:])]

  # (query, expected) pairs. The empty value is matched only by exact-empty and
  # by the bare `*` / TAG `w'*'` match-all; every `?`-bearing wildcard requires
  # at least one character and so must exclude doc:empty.
  cases = [
    ('@t:{""}',      [1, 'doc:empty']),               # TAG exact empty
    ("@t:{w'*'}",    [2, 'doc:empty', 'doc:val']),    # TAG match-all incl. empty
    ("@t:{w'?*'}",   [1, 'doc:val']),                 # >=1 char, excludes empty
    ("@t:{w'*?*'}",  [1, 'doc:val']),
    ("@t:{w'*?'}",   [1, 'doc:val']),
    ('@tx:""',       [1, 'doc:empty']),               # TEXT exact empty
    ("@tx:w'?*'",    [1, 'doc:val']),
    ("@tx:w'*?*'",   [1, 'doc:val']),
    ("@tx:w'*?'",    [1, 'doc:val']),
  ]
  for query, expected in cases:
    res_w  = search('idx_w',  query)
    res_no = search('idx_no', query)
    # No drift: suffix-trie field agrees with the plain INDEXEMPTY field ...
    env.assertEqual(res_w, res_no, message=f'drift for {query}')
    # ... and both match the expected recall.
    env.assertEqual(res_w, expected, message=f'recall for {query}')

def test_params(env):
  env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
  # this test check that `\*` is escaped correctly on contains queries
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')

  conn.execute_command('HSET', 'doc1', 't', 'world')

  env.expect('ft.search', 'idx', '$prefix*', 'PARAMS', 2, 'prefix', 'wor').equal([1, 'doc1', ['t', 'world']])
  env.expect('ft.search', 'idx', '*$contains*', 'PARAMS', 2, 'contains', 'orl').equal([1, 'doc1', ['t', 'world']])
  env.expect('ft.search', 'idx', '*$suffix', 'PARAMS', 2, 'suffix', 'rld').equal([1, 'doc1', ['t', 'world']])

@skip(cluster=True)
def test_issue_3124(env):
  # test prefix query on field with suffix trie
  env.cmd('ft.create', 'idx_txt', 'SCHEMA', 't', 'TEXT')
  env.cmd('ft.create', 'idx_txt_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')
  env.cmd('ft.create', 'idx_tag', 'SCHEMA', 't', 'TAG')
  env.cmd('ft.create', 'idx_tag_suffix', 'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE')
  conn = getConnectionByEnv(env)

  # insert a single document with value 'hello' in field 't'
  conn.execute_command('HSET', 'doc', 't', 'hello')

  # test prefix query on field with existing prefix query
  res_exist1 = env.cmd('ft.search', 'idx_txt', '@t:hell*')
  res_exist2 = env.cmd('ft.search', 'idx_txt_suffix', '@t:hell*')
  env.assertEqual(res_exist1, res_exist2)

  # test prefix query on field with non-existing prefix query
  res_not_exist1 = env.cmd('ft.search', 'idx_txt', '@t:ell*')
  res_not_exist2 = env.cmd('ft.search', 'idx_txt_suffix', '@t:ell*')
  env.assertEqual(res_not_exist1, res_not_exist2)

@skip(cluster=True)
def testTextSuffixTrieMaxPrefixExpansions():
    """Contains query on TEXT WITHSUFFIXTRIE field hits max prefix expansion limit."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=3)
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT', 'WITHSUFFIXTRIE').ok()

    # Create many distinct values sharing a common substring
    for i in range(20):
        conn.execute_command('HSET', f'{{doc}}:{i}', 't', f'val{i}common')

    # Set max expansions very low
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Contains query (*common*) uses the suffix trie charIterCb path
    res = env.cmd('FT.SEARCH', 'idx', '*common*', 'LIMIT', '0', '0')
    env.assertContains('Max prefix expansions limit was reached', res['warning'])

    # Restore default
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '200')
