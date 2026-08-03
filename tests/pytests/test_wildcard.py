from includes import *
from common import *
from RLTest import Env
import time

@skip(cluster=True)
def testSanity_dialect_2(env):
  dotestSanity(env, 2)

@skip(cluster=True)
def testSanity_dialect_3(env):
  dotestSanity(env, 3)

def dotestSanity(env, dialect):
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  env.expect(config_cmd(), 'set', 'DEFAULT_DIALECT', dialect).ok()
  env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 10000000).ok()
  item_qty = 1000

  index_list = ['idx_bf', 'idx_suffix']
  env.cmd('FT.CREATE', 'idx_bf', 'SCHEMA', 't', 'TEXT')
  env.cmd('FT.CREATE', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

  conn = getConnectionByEnv(env)

  pl = conn.pipeline()
  for i in range(item_qty):
    pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
    pl.execute()

  for index in index_list:
    #prefix
    env.expect('ft.search', index, "w'f*'", 'LIMIT', 0 , 0).equal([4000])
    env.expect('ft.search', index, "w'foo*'", 'LIMIT', 0 , 0).equal([4000])
    env.expect('ft.search', index, "w'foo1*'", 'LIMIT', 0 , 0).equal([111])
    env.expect('ft.search', index, "w'*ooo1*'", 'LIMIT', 0 , 0).equal([222])

    # contains
    env.expect('ft.search', index, "w'*oo*'", 'LIMIT', 0 , 0).equal([4000])
    # 55x & x55 - 555
    env.expect('ft.search', index, "w'*55*'", 'LIMIT', 0 , 0).equal([76])
    # 555
    env.expect('ft.search', index, "w'*555*'", 'LIMIT', 0 , 0).equal([4])
    env.expect('ft.search', index, "w'*o55*'", 'LIMIT', 0 , 0).equal([44])
    env.expect('ft.search', index, "w'*oo55*'", 'LIMIT', 0 , 0).equal([33])
    env.expect('ft.search', index, "w'*oo555*'", 'LIMIT', 0 , 0).equal([3])

    # 23x & x23
    env.expect('ft.search', index, '*23*', 'LIMIT', 0 , 0).equal([80])
    # 234
    env.expect('ft.search', index, '*234*', 'LIMIT', 0 , 0).equal([4])
    env.expect('ft.search', index, '*o23*', 'LIMIT', 0 , 0).equal([44])
    env.expect('ft.search', index, '*oo23*', 'LIMIT', 0 , 0).equal([33])
    env.expect('ft.search', index, '*oo234*', 'LIMIT', 0 , 0).equal([3])

    # suffix
    env.expect('ft.search', index, "w'*oo234'", 'LIMIT', 0 , 0).equal([3])
    env.expect('ft.search', index, "w'*234'", 'LIMIT', 0 , 0).equal([4])
    env.expect('ft.search', index, "w'*13'", 'LIMIT', 0 , 0).equal([40])

    # all
    env.expect('ft.search', index, r"@t:(w'*')", 'LIMIT', 0 , 0).equal([4*item_qty])

  # test timeout
  for i in range(item_qty, item_qty * 5):
      pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
      pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
      pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
      pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
      pl.execute()

  env.expect(config_cmd(), 'set', 'TIMEOUT', 1).ok()
  env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'FAIL').ok()
  env.expect('ft.search', index_list[0], "w'foo*'", 'LIMIT', 0 , 0).error() \
    .contains('Timeout limit was reached')

@skip(cluster=True)
def testSanityTag_dialect_2(env):
  dotestSanityTag(env, 2)

@skip(cluster=True)
def testSanityTag_dialect_3(env):
  dotestSanityTag(env, 3)

def dotestSanityTag(env, dialect):
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  env.expect(config_cmd(), 'set', 'DEFAULT_DIALECT', dialect).ok()
  env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 10000000).ok()
  item_qty = 1000

  index_list = ['idx_bf', 'idx_suffix']
  env.cmd('FT.CREATE', 'idx_bf', 'SCHEMA', 't', 'TAG', 'SORTABLE')
  env.cmd('FT.CREATE', 'idx_suffix', 'SCHEMA', 't', 'TAG', 'WITHSUFFIXTRIE')

  conn = getConnectionByEnv(env)

  pl = conn.pipeline()
  for i in range(item_qty):
    pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
    pl.execute()

  for index in index_list:
    #prefix
    env.expect('ft.search', index, "@t:{w'f*'}", 'LIMIT', 0 , 0).equal([4000])
    env.expect('ft.search', index, "@t:{w'foo*'}", 'LIMIT', 0 , 0).equal([4000])
    env.expect('ft.search', index, "@t:{w'foo1*'}", 'LIMIT', 0 , 0).equal([111])
    env.expect('ft.search', index, "@t:{w'*ooo1*'}", 'LIMIT', 0 , 0).equal([222])

    # contains
    env.expect('ft.search', index, "@t:{w'*oo*'}", 'LIMIT', 0 , 0).equal([4000])
    # 55x & x55 - 555
    env.expect('ft.search', index, "@t:{w'*55*'}", 'LIMIT', 0 , 0).equal([76])
    # 555
    env.expect('ft.search', index, "@t:{w'*555*'}", 'LIMIT', 0 , 0).equal([4])
    env.expect('ft.search', index, "@t:{w'*o55*'}", 'LIMIT', 0 , 0).equal([44])
    env.expect('ft.search', index, "@t:{w'*oo55*'}", 'LIMIT', 0 , 0).equal([33])
    env.expect('ft.search', index, "@t:{w'*oo555*'}", 'LIMIT', 0 , 0).equal([3])

    # 23x & x23
    env.expect('ft.search', index, "@t:{w'*23*'}", 'LIMIT', 0 , 0).equal([80])
    # 234x & x234
    env.expect('ft.search', index, "@t:{w'*234*'}", 'LIMIT', 0 , 0).equal([4])
    env.expect('ft.search', index, "@t:{w'*o23*'}", 'LIMIT', 0 , 0).equal([44])
    env.expect('ft.search', index, "@t:{w'*oo23*'}", 'LIMIT', 0 , 0).equal([33])
    env.expect('ft.search', index, "@t:{w'*oo234*'}", 'LIMIT', 0 , 0).equal([3])

    # suffix
    env.expect('ft.search', index, "@t:{w'*oo234'}", 'LIMIT', 0 , 0).equal([3])
    env.expect('ft.search', index, "@t:{w'*234'}", 'LIMIT', 0 , 0).equal([4])
    env.expect('ft.search', index, "@t:{w'*13'}", 'LIMIT', 0 , 0).equal([40])
    env.expect('ft.search', index, "@t:{w'*oo23?'}", 'LIMIT', 0 , 0).equal([30])
    env.expect('ft.search', index, "@t:{w'*23?'}", 'LIMIT', 0 , 0).equal([40])
    env.expect('ft.search', index, "@t:{w'*13?'}", 'LIMIT', 0 , 0).equal([40])
    env.expect('ft.search', index, "@t:{w'*oo2?4'}", 'LIMIT', 0 , 0).equal([30])
    env.expect('ft.search', index, "@t:{w'*2?4'}", 'LIMIT', 0 , 0).equal([40])
    env.expect('ft.search', index, "@t:{w'*1?3'}", 'LIMIT', 0 , 0).equal([40])

    # all
    env.expect('ft.search', index, r"@t:{w'*'}", 'LIMIT', 0 , 0).equal([4*item_qty])

  # test timeout
  for i in range(item_qty, item_qty * 5):
      pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
      pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
      pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
      pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
      pl.execute()

  env.expect(config_cmd(), 'set', 'TIMEOUT', 1).ok()
  env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'FAIL').ok()
  env.expect('ft.search', index_list[0], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
    .contains('Timeout limit was reached')
  env.expect('ft.search', index_list[1], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
    .contains('Timeout limit was reached')

@skip()
def testBenchmark(env):
  env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
  env.expect(config_cmd(), 'set', 'DEFAULT_DIALECT', 2).ok()
  env.expect(config_cmd(), 'set', 'TIMEOUT', 100000).ok()
  env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
  item_qty = 1000000

  index_list = ['idx_bf']
  env.cmd('FT.CREATE', 'idx_bf', 'SCHEMA', 't', 'TEXT')
  #env.cmd('FT.CREATE', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

  conn = getConnectionByEnv(env)

  start = time.time()
  pl = conn.pipeline()
  for i in range(item_qty):
    pl.execute_command('HSET', 'doc%d' % i, 't', 'foo321%dbar312' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo321%dbar311' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo312%dbar312' % i)
    pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo31%dbar312' % i)
    pl.execute()

  print('----*ooo1*----')

  for i in range(1):
    #prefix
    start_time = time.time()
    env.expect('ft.search', index_list[i], "*ooo3*", 'LIMIT', 0 , 0).equal([2222])
    print(time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', index_list[i], "w'*o**o3*'", 'LIMIT', 0 , 0).equal([2222])
    print(time.time() - start_time)
    start_time = time.time()
    print('----*ooo1*----')

    env.expect('ft.search', index_list[i], "*555*", 'LIMIT', 0 , 0).equal([76])
    print(time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', index_list[i], "w'*55*5*'", 'LIMIT', 0 , 0).equal([76])
    print(time.time() - start_time)
    start_time = time.time()
    print('----*555*----')

    # suffix
    env.expect('ft.search', index_list[i], '*oo2*34', 'LIMIT', 0 , 0).equal([3])
    print(time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', index_list[i], "w'*oo2*34'", 'LIMIT', 0 , 0).equal([3])
    print(time.time() - start_time)
    start_time = time.time()
    print('----*oo234----')

@skip(cluster=True)
def testEscape(env):
  conn = getConnectionByEnv(env)

  env.expect(config_cmd(), 'SET', 'MINPREFIX', 1).ok()
  env.expect(config_cmd(), 'SET', 'DEFAULT_DIALECT', 2).ok()
  env.expect(config_cmd(), 'SET', 'TIMEOUT', 100000).ok()
  env.expect(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false').ok()

  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'NOSTEM')

  conn.execute_command('HSET', 'doc1', 't', 'hello')
  conn.execute_command('HSET', 'doc2', 't', 'jello')
  conn.execute_command('HSET', 'doc3', 't', 'hallelujah')
  conn.execute_command('HSET', 'doc4', 't', 'help')
  conn.execute_command('HSET', 'doc5', 't', 'mellow')
  conn.execute_command('HSET', 'doc6', 't', 'jellyfish')
  conn.execute_command('HSET', 'doc7', 't', 'hello\\\'world')
  conn.execute_command('HSET', 'doc8', 't', 'hello\\\\world')
  conn.execute_command('HSET', 'doc9', 't', '\\\'hello')
  conn.execute_command('HSET', 'doc10', 't', '\\\\hello')
  conn.execute_command('HSET', 'doc11', 't', 'hello\\\'')
  conn.execute_command('HSET', 'doc12', 't', 'hello\\\\')
  conn.execute_command('HSET', 'doc13', 't', 'halloween')

  env.expect(debug_cmd(), 'dump_terms', 'idx').equal(
      ["'hello", '\\hello', 'hallelujah', 'halloween', 'hello', "hello'", "hello'world",
       'hello\\', 'hello\\world', 'help', 'jello', 'jellyfish', 'mellow'])

  env.expect('FT.SEARCH', 'idx', "w'*ell*'", 'LIMIT', 0 , 0).equal([10])
  env.expect('FT.SEARCH', 'idx', "w'*ello'", 'LIMIT', 0 , 0).equal([4])

  env.expect('FT.SEARCH', 'idx', "w'?????'").equal([2, 'doc1', ['t', 'hello'],
                                                       'doc2', ['t', 'jello']])
  env.expect('FT.SEARCH', 'idx', "w'?ello'").equal([2, 'doc1', ['t', 'hello'],
                                                       'doc2', ['t', 'jello']])
  env.expect('FT.SEARCH', 'idx', "w'?ello?'").equal([3, 'doc5', ['t', 'mellow'],
                                                        'doc11', ['t', "hello\\'"],
                                                        'doc12', ['t', 'hello\\\\']])
  # hallelujah
  env.expect('FT.SEARCH', 'idx', "w'hallelujah'", 'LIMIT', 0 , 0).equal([1])
  env.expect('FT.SEARCH', 'idx', "w'ha?l*jah'", 'LIMIT', 0 , 0).equal([1])
  env.expect('FT.SEARCH', 'idx', "w'ha*???lujah'", 'LIMIT', 0 , 0).equal([1])
  env.expect('FT.SEARCH', 'idx', "w'?al?*?h'", 'LIMIT', 0 , 0).equal([1])

  # escape \'
  env.expect('FT.SEARCH', 'idx', "w'*\\'*'").equal([3, 'doc7', ['t', "hello\\'world"], # *'*
                                                        'doc9', ['t', "\\'hello"],
                                                        'doc11', ['t', "hello\\'"]])
  env.expect('FT.SEARCH', 'idx', "w'*o\\\'w*'").equal([1, 'doc7', ['t', "hello\\'world"]]) # *o'w*

  # escape \\
  env.expect('FT.SEARCH', 'idx', "w'*\\\\*'").equal([3, 'doc8', ['t', 'hello\\\\world'], # *\*
                                                      'doc10', ['t', '\\\\hello'],
                                                      'doc12', ['t', 'hello\\\\']])
  env.expect('FT.SEARCH', 'idx', "w'*o\\\\w*'").equal([1, 'doc8', ['t', "hello\\\\world"]]) # *o\w*


  # test with PARAMS
  # escape \'
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*\\\'*").equal([3, 'doc7', ['t', "hello\\'world"], # *'*
                                                        'doc9', ['t', "\\'hello"],
                                                        'doc11', ['t', "hello\\'"]])
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*o\\\'w*").equal([1, 'doc7', ['t', "hello\\'world"]]) # *o'w*

  # escape \\
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*\\\\*").equal([3, 'doc8', ['t', 'hello\\\\world'], # *\*
                                                      'doc10', ['t', '\\\\hello'],
                                                      'doc12', ['t', 'hello\\\\']])
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*o\\\\w*").equal([1, 'doc8', ['t', "hello\\\\world"]]) # *o\w*

  query_type = lambda res: res[1][1][0][3][3]

  # add more documents so the wildcard queries are not optimized to a single term
  conn.execute_command('HSET', 'more_doc1', 't', 'heplo') # codespell:ignore heplo
  conn.execute_command('HSET', 'more_doc7', 't', 'hello\\\'werld') # codespell:ignore werld
  conn.execute_command('HSET', 'more_doc8', 't', 'hello\\\\werld') # codespell:ignore werld
  conn.execute_command('HSET', 'more_doc9', 't', '\\\'helno') # codespell:ignore helno
  conn.execute_command('HSET', 'more_doc10', 't', '\\\\helno') # codespell:ignore helno

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'he?lo'")
  env.assertEqual(query_type(res), "WILDCARD - he?lo")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'h*?*o'")
  env.assertEqual(query_type(res), "WILDCARD - h*?*o")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'h\\*?*o'")
  env.assertEqual(query_type(res), "WILDCARD - h*?*o")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'\\h*?*o'")
  env.assertEqual(query_type(res), "WILDCARD - h*?*o")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'\\'h*?*o'")
  env.assertEqual(query_type(res), "WILDCARD - 'h*?*o")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'\\\\h*?*o'")
  env.assertEqual(query_type(res), r"WILDCARD - \h*?*o")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'*o\\\\w*'")
  env.assertEqual(query_type(res), "WILDCARD - *o\\w*")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'*o\\'w*'")
  env.assertEqual(query_type(res), "WILDCARD - *o'w*")

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', "w'*o\\\'w*'")
  env.assertEqual(query_type(res), "WILDCARD - *o'w*")

@skip(cluster=True)
def testLowerUpperCase(env):
  conn = getConnectionByEnv(env)

  env.expect(config_cmd(), 'SET', 'MINPREFIX', 1).ok()
  env.expect(config_cmd(), 'SET', 'DEFAULT_DIALECT', 2).ok()
  env.expect(config_cmd(), 'SET', 'TIMEOUT', 100000).ok()
  env.expect(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false').ok()

  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'NOSTEM')

  conn.execute_command('HSET', 'doc1', 't', 'hello')
  conn.execute_command('HSET', 'doc2', 't', 'HELLO')
  conn.execute_command('HSET', 'doc3', 't', 'help')
  conn.execute_command('HSET', 'doc4', 't', 'HELP')

  env.expect('FT.SEARCH', 'idx', "w'*el*'", 'NOCONTENT').equal([4, 'doc1', 'doc2', 'doc3', 'doc4'])
  env.expect('FT.SEARCH', 'idx', "w'*EL*'", 'NOCONTENT').equal([4, 'doc1', 'doc2', 'doc3', 'doc4'])


def testBasic():
  env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
  conn = getConnectionByEnv(env)

  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')

  conn.execute_command('HSET', 'doc1', 't', 'hello')
  conn.execute_command('HSET', 'doc2', 't', 'hell')
  conn.execute_command('HSET', 'doc3', 't', 'helen')
  conn.execute_command('HSET', 'doc4', 't', 'help')
  conn.execute_command('HSET', 'doc5', 't', 'olah')
  conn.execute_command('HSET', 'doc6', 't', 'heal')
  conn.execute_command('HSET', 'doc7', 't', 'hall')
  conn.execute_command('HSET', 'doc8', 't', 'hallo')

  q_params = ('NOCONTENT', 'SCORER', 'TFIDF')
  env.expect('FT.SEARCH', 'idx', "w'*el*'", *q_params).equal([4, 'doc1', 'doc2', 'doc3', 'doc4'])
  env.expect('FT.SEARCH', 'idx', "w'*ll*'", *q_params).equal([4, 'doc1', 'doc2', 'doc7', 'doc8'])
  env.expect('FT.SEARCH', 'idx', "w'*llo'", *q_params).equal([2, 'doc1', 'doc8'])
  env.expect('FT.SEARCH', 'idx', "w'he*'", *q_params).equal([5, 'doc1', 'doc2', 'doc3', 'doc4', 'doc6'])

  env.expect('FT.AGGREGATE', 'idx', "w'*el*'", 'LOAD', 1, '@t', 'SORTBY', 1, '@t')    \
        .equal([4, ['t', 'helen'], ['t', 'hell'], ['t', 'hello'], ['t', 'help']])

  env.expect('FT.AGGREGATE', 'idx', "w'*ll*'", 'LOAD', 1, '@t', 'SORTBY', 1, '@t')    \
        .equal([4, ['t', 'hall'], ['t', 'hallo'], ['t', 'hell'], ['t', 'hello']])

  env.expect('FT.AGGREGATE', 'idx', "w'*llo'", 'LOAD', 1, '@t', 'SORTBY', 1, '@t')    \
        .equal([2, ['t', 'hallo'], ['t', 'hello']])

  env.expect('FT.AGGREGATE', 'idx', "w'he*'", 'LOAD', 1, '@t', 'SORTBY', 1, '@t')     \
        .equal([5, ['t', 'heal'], ['t', 'helen'], ['t', 'hell'], ['t', 'hello'], ['t', 'help']])

def testSuffixCleanup(env):
  conn = getConnectionByEnv(env)
  env.expect(config_cmd() + ' SET FORK_GC_CLEAN_THRESHOLD 0').ok()

  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'WITHSUFFIXTRIE', 't2', 'TEXT')
  conn.execute_command('HSET', 'doc', 't1', 'foo', 't2', 'bar')
  conn.execute_command('DEL', 'doc')

  forceInvokeGC(env, 'idx')

def testMOD7453():
  """Tests that we don't enter an infinite loop when we match a wildcard to a
    wildcard in the matched term"""

  env = DialectEnv()
  conn = getConnectionByEnv(env)

  # Create an index with a TEXT and TAG field
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'text', 'TEXT')

  # Populate the db
  conn.execute_command('HSET', 'doc1', 'tag', 'ba*cl', 'text', 'ba*cl')

  # Search via "problematic" wildcard
  MAX_DIALECT = set_max_dialect(env)
  for dialect in range(2, MAX_DIALECT + 1):
    env.set_dialect(dialect)
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{w'*a*'} @text:w'*a*'")
    env.assertEqual(res, [1, 'doc1', ['tag', 'ba*cl', 'text', 'ba*cl']])

    # TODO: Bug - this should work for intersection as well, but doesn't since
    # the text wildcard doesn't match the result correctly.
    res = env.cmd('FT.SEARCH', 'idx', "@tag:{w'*a*?'} | @text:w'*a*?'")
    env.assertEqual(res, [1, 'doc1', ['tag', 'ba*cl', 'text', 'ba*cl']])

@skip(cluster=True)
def testWildcardOnFieldWithoutSuffixTrie():
    """Wildcard query on a TEXT field without WITHSUFFIXTRIE errors when spec has a suffix trie."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # t1 has WITHSUFFIXTRIE, t2 does not
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't1', 'TEXT', 'WITHSUFFIXTRIE',
               't2', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't1', 'hello', 't2', 'world')

    # Wildcard on t2 should error: spec->suffix exists (from t1) but t2 is not in suffixMask
    env.expect('FT.SEARCH', 'idx', "@t2:w'hel*o'").error() \
        .contains('WITHSUFFIXTRIE')

_MAX_EXPANSIONS_WARNING = 'Max prefix expansions limit was reached'

# A cap above 1, so a truncated expansion is distinguishable from an empty one,
# and below the size of one document family, so it is always reached inside the
# first one.
_EXPANSION_CAP = 3
# Large enough that the terms trie's lexicographic order and the suffix trie's
# insertion order disagree within the first `_EXPANSION_CAP` terms, which is what
# lets the two tests below tell the two walks apart by their result.
_EXPANSION_DOCS = 12
assert _EXPANSION_CAP < _EXPANSION_DOCS
assert _EXPANSION_DOCS >= 12

def _setupExpansionIndex(env, *schemaArgs):
    """Index two families of `_EXPANSION_DOCS` documents, one distinct term
    each: `val<i>common` under `doc:<i>`, then `xal<i>common` under `xdoc:<i>`.
    Every pattern the max-expansion tests use matches both families."""
    conn = getConnectionByEnv(env)
    # NOSTEM so the terms trie holds exactly the terms indexed below: a stemmed
    # form is stored as a separate '+'-prefixed term, which could match the
    # patterns below and make the expected expansion depend on the stemmer.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'NOSTEM', *schemaArgs).ok()

    # The suffix trie records terms in indexing order, which
    # testWildcardSuffixTrieMaxPrefixExpansions depends on. Documents must
    # therefore be written after FT.CREATE, one at a time, in this order —
    # writing them first and letting the background scan index them, or reloading
    # the index, would rebuild that order from a keyspace scan instead.
    for letter, keyPrefix in (('v', 'doc'), ('x', 'xdoc')):
        for i in range(_EXPANSION_DOCS):
            conn.execute_command('HSET', f'{keyPrefix}:{i}', 't', f'{letter}al{i}common')

def _capExpansions(env):
    """Lower MAXPREFIXEXPANSIONS to `_EXPANSION_CAP`, returning the value it
    held before, which the caller must restore."""
    # Capture rather than hardcode the default, so this keeps restoring the right
    # value if it ever changes.
    reply = env.cmd(config_cmd(), 'GET', 'MAXPREFIXEXPANSIONS')
    previous = reply['MAXPREFIXEXPANSIONS'] if isinstance(reply, dict) else reply[0][1]
    env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', _EXPANSION_CAP).ok()
    return previous

def _assertMaxExpansionsWarning(env, res):
    """Assert `res` carries the max-expansion warning and nothing else."""
    # Matching the whole list rather than searching it, so an extra unrelated
    # warning does not pass unnoticed either.
    env.assertEqual(res['warning'], [_MAX_EXPANSIONS_WARNING], message=res)

@skip(cluster=True)
def testWildcardMaxPrefixExpansions():
    """A wildcard query brute-forcing the terms trie stops expanding at
    MAXPREFIXEXPANSIONS terms, and warns."""
    # protocol=3 so the warning is a named reply field rather than positional;
    # DEFAULT_DIALECT 2 because w'...' syntax needs dialect 2 or above.
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=3)
    # No WITHSUFFIXTRIE, so the expansion brute-forces the terms trie.
    _setupExpansionIndex(env)
    query = "w'*al*'"

    # Negative control: uncapped, the query expands to every indexed term. So the
    # capped result below is the cap truncating this expansion, not the pattern
    # matching three documents for some unrelated reason.
    res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT')
    env.assertEqual(res['total_results'], 2 * _EXPANSION_DOCS, message=res)
    env.assertEqual(res['warning'], [], message=res)

    previous = _capExpansions(env)
    try:
        # Asserting the ids, not just the count: every term holds exactly one
        # document, so a count alone cannot tell a correctly truncated expansion
        # from one that opened three unrelated readers. The terms trie is walked
        # lexicographically, so the first three terms are 'val0common',
        # 'val10common', 'val11common' — digits sort ahead of the 'c' of
        # 'val1common'.
        #
        # Compared as a set: which terms the walk keeps is the assertion, while
        # the order they come back in is the scorer's business — every document
        # here holds one term and so scores equally.
        res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT')
        env.assertEqual(res['total_results'], _EXPANSION_CAP, message=res)
        env.assertEqual(sorted(doc['id'] for doc in res['results']),
                        sorted(['doc:0', 'doc:10', 'doc:11']), message=res)
        _assertMaxExpansionsWarning(env, res)
    finally:
        env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', previous).ok()

@skip(cluster=True)
def testWildcardSuffixTrieMaxPrefixExpansions():
    """A wildcard query expanding through the suffix trie stops expanding at
    MAXPREFIXEXPANSIONS terms, and warns."""
    # See testWildcardMaxPrefixExpansions for why this Env is customised.
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=3)
    _setupExpansionIndex(env, 'WITHSUFFIXTRIE')

    # Anchored on 'common', which the suffix trie can look up, so the expansion
    # runs through the suffix trie rather than the terms trie — a walk the rest
    # of this file never takes under a cap.
    query = "w'*common'"

    # Negative control, as in testWildcardMaxPrefixExpansions.
    res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT')
    env.assertEqual(res['total_results'], 2 * _EXPANSION_DOCS, message=res)
    env.assertEqual(res['warning'], [], message=res)

    previous = _capExpansions(env)
    try:
        # The ids are what pin the walk to the suffix trie: it lists the terms
        # carrying 'common' in indexing order, so the cap truncates that list to
        # 'val0common', 'val1common', 'val2common'. Should the suffix trie ever
        # decline this pattern, the fallback brute-force walk warns identically
        # and returns the same number of documents — but the lexicographic set
        # testWildcardMaxPrefixExpansions asserts, not this one.
        res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT')
        env.assertEqual(res['total_results'], _EXPANSION_CAP, message=res)
        env.assertEqual(sorted(doc['id'] for doc in res['results']),
                        sorted(['doc:0', 'doc:1', 'doc:2']), message=res)
        _assertMaxExpansionsWarning(env, res)
    finally:
        env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', previous).ok()

@skip(cluster=True)
def testWildcardQuestionMarkMultibyteWithoutSuffixTrie():
    """Without WITHSUFFIXTRIE, a wildcard query is evaluated by brute force over
    the rune terms trie (Wildcard_MatchRune), where `?` consumes one codepoint —
    so w'entr?' matches 'entré' ('é' is two UTF-8 bytes but one codepoint)."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'entré')
    conn.execute_command('HSET', 'doc2', 't', 'entrx')

    res = env.cmd('FT.SEARCH', 'idx', "w'entr?'", 'NOCONTENT')
    env.assertEqual(res, [2, 'doc1', 'doc2'])

@skip(cluster=True)
def testWildcardQuestionMarkMultibyteWithSuffixTrie():
    """With WITHSUFFIXTRIE, the candidate terms found via the suffix trie are
    re-filtered rune-wise (Suffix_CB_Wildcard -> Wildcard_MatchRune), where `?`
    consumes one codepoint — so w'entr?' matches 'entré', the same result the
    brute-force path produces without the suffix trie."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE').ok()
    conn.execute_command('HSET', 'doc1', 't', 'entré')
    conn.execute_command('HSET', 'doc2', 't', 'entrx')

    res = env.cmd('FT.SEARCH', 'idx', "w'entr?'", 'NOCONTENT')
    env.assertEqual(res, [2, 'doc1', 'doc2'])

@skip(cluster=True)
def testWildcardStarredNonFinalAnchorWithSuffixTrie():
    """On the suffix-trie path, a pattern whose best anchor token is starred
    and non-final (in w'verylongtoken*a', 'verylongtoken' out-scores the tail
    token 'a' despite the starred-anchor penalty) must return the same result
    as the brute-force path: the anchor is NUL-terminated in place inside the
    pattern rune buffer before the candidate re-filter runs, and that filter
    must still see the full pattern."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE').ok()
    conn.execute_command('HSET', 'doc1', 't', 'verylongtokenxa')
    conn.execute_command('HSET', 'doc2', 't', 'verylongtokenxb')

    res = env.cmd('FT.SEARCH', 'idx', "w'verylongtoken*a'", 'NOCONTENT')
    env.assertEqual(res, [1, 'doc1'])
