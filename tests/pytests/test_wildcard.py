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
  env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'RETURN').ok()
  env.expect('ft.search', index_list[0], "w'foo*'", 'LIMIT', 0 , 0).error() \
    .contains('Timeout limit was reached')
  #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
  #  .contains('Timeout limit was reached')

  env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'FAIL').ok()
  env.expect('ft.search', index_list[0], "w'foo*'", 'LIMIT', 0 , 0).error() \
    .contains('Timeout limit was reached')
  #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
  #  .contains('Timeout limit was reached')

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
  env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'RETURN').ok()
  env.expect('ft.search', index_list[0], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
    .contains('Timeout limit was reached')
  env.expect('ft.search', index_list[1], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
    .contains('Timeout limit was reached')

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

  env.expect('FT.SEARCH', 'idx', "w'*el*'", 'NOCONTENT').equal([4, 'doc1', 'doc2', 'doc3', 'doc4'])
  env.expect('FT.SEARCH', 'idx', "w'*ll*'", 'NOCONTENT').equal([4, 'doc1', 'doc2', 'doc7', 'doc8'])
  env.expect('FT.SEARCH', 'idx', "w'*llo'", 'NOCONTENT').equal([2, 'doc1', 'doc8'])
  env.expect('FT.SEARCH', 'idx', "w'he*'", 'NOCONTENT').equal([5, 'doc1', 'doc2', 'doc3', 'doc4', 'doc6'])

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
