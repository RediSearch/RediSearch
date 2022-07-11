from includes import *
from common import *
from RLTest import Env
import time

def testSanity(env):
    env.skipOnCluster()
    env.expect('FT.CONFIG', 'set', 'MINPREFIX', 1).ok()
    env.expect('FT.CONFIG', 'set', 'DEFAULT_DIALECT', 2).ok()
    env.expect('FT.CONFIG', 'set', 'TIMEOUT', 100000).ok()
    env.expect('FT.CONFIG', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
    item_qty = 10000

    index_list = ['idx_bf']
    env.cmd('FT.CREATE', 'idx_bf', 'SCHEMA', 't', 'TEXT')
    #env.cmd('FT.CREATE', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

    conn = getConnectionByEnv(env)

    start = time.time()
    pl = conn.pipeline()
    for i in range(item_qty):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()

    for i in range(1):
        #prefix
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'f*'", 'LIMIT', 0 , 0).equal([40000])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'foo*'", 'LIMIT', 0 , 0).equal([40000])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'foo1*'", 'LIMIT', 0 , 0).equal([1111])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*ooo1*'", 'LIMIT', 0 , 0).equal([2222])
        print(time.time() - start_time)
        start_time = time.time()

        # contains
        env.expect('ft.search', index_list[i], "w'*oo*'", 'LIMIT', 0 , 0).equal([40000])
        print(time.time() - start_time)
        start_time = time.time()
        # 55xx & x55x & xx55 - 555x - x555 
        env.expect('ft.search', index_list[i], "w'*55*'", 'LIMIT', 0 , 0).equal([1120])
        print(time.time() - start_time)
        start_time = time.time()
        # 555x & x555 - 5555
        env.expect('ft.search', index_list[i], "w'*555*'", 'LIMIT', 0 , 0).equal([76])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*o55*'", 'LIMIT', 0 , 0).equal([444])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*oo55*'", 'LIMIT', 0 , 0).equal([333])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*oo555*'", 'LIMIT', 0 , 0).equal([33])
        print(time.time() - start_time)
        start_time = time.time()

        # # 23xx & x23x & xx23 - 2323
        # env.expect('ft.search', index_list[i], '*23*', 'LIMIT', 0 , 0).equal([1196])
        # # 234x & x234
        # start = time.time()
        # env.expect('ft.search', index_list[i], '*234*', 'LIMIT', 0 , 0).equal([80])
        # print (time.time() - start)
        # start = time.time()
        # env.expect('ft.search', index_list[i], '*o23*', 'LIMIT', 0 , 0).equal([444])
        # print (time.time() - start)
        # env.expect('ft.search', index_list[i], '*oo23*', 'LIMIT', 0 , 0).equal([333])
        # env.expect('ft.search', index_list[i], '*oo234*', 'LIMIT', 0 , 0).equal([33])

        # suffix
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*oo234'", 'LIMIT', 0 , 0).equal([3])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*234'", 'LIMIT', 0 , 0).equal([40])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*13'", 'LIMIT', 0 , 0).equal([400])
        print(time.time() - start_time)
        start_time = time.time()

    # test timeout
    env.expect('FT.CONFIG', 'set', 'TIMEOUT', 1).ok()
    env.expect('FT.CONFIG', 'set', 'ON_TIMEOUT', 'RETURN').ok()
    env.expect('ft.search', index_list[0], "w'foo*'", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

    env.expect('FT.CONFIG', 'set', 'ON_TIMEOUT', 'FAIL').ok()
    env.expect('ft.search', index_list[0], "w'foo*'", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

def testSanityTag(env):
    env.skipOnCluster()
    env.expect('FT.CONFIG', 'set', 'MINPREFIX', 1).ok()
    env.expect('FT.CONFIG', 'set', 'DEFAULT_DIALECT', 2).ok()
    env.expect('FT.CONFIG', 'set', 'TIMEOUT', 100000).ok()
    env.expect('FT.CONFIG', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
    item_qty = 10000

    index_list = ['idx_bf']
    env.cmd('FT.CREATE', 'idx_bf', 'SCHEMA', 't', 'TAG', 'SORTABLE')
    #env.cmd('FT.CREATE', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

    conn = getConnectionByEnv(env)

    start = time.time()
    pl = conn.pipeline()
    for i in range(item_qty):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()

    for i in range(1):
        #prefix
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'f*'}", 'LIMIT', 0 , 0).equal([40000])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'foo*'}", 'LIMIT', 0 , 0).equal([40000])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'foo1*'}", 'LIMIT', 0 , 0).equal([1111])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'*ooo1*'}", 'LIMIT', 0 , 0).equal([2222])
        print(time.time() - start_time)
        start_time = time.time()

        # contains
        env.expect('ft.search', index_list[i], "@t:{w'*oo*'}", 'LIMIT', 0 , 0).equal([40000])
        print(time.time() - start_time)
        start_time = time.time()
        # 55xx & x55x & xx55 - 555x - x555 
        env.expect('ft.search', index_list[i], "@t:{w'*55*'}", 'LIMIT', 0 , 0).equal([1120])
        print(time.time() - start_time)
        start_time = time.time()
        # 555x & x555 - 5555
        env.expect('ft.search', index_list[i], "@t:{w'*555*'}", 'LIMIT', 0 , 0).equal([76])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'*o55*'}", 'LIMIT', 0 , 0).equal([444])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'*oo55*'}", 'LIMIT', 0 , 0).equal([333])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'*oo555*'}", 'LIMIT', 0 , 0).equal([33])
        print(time.time() - start_time)
        start_time = time.time()

        # # 23xx & x23x & xx23 - 2323
        # env.expect('ft.search', index_list[i], '*23*', 'LIMIT', 0 , 0).equal([1196])
        # # 234x & x234
        # start = time.time()
        # env.expect('ft.search', index_list[i], '*234*', 'LIMIT', 0 , 0).equal([80])
        # print (time.time() - start)
        # start = time.time()
        # env.expect('ft.search', index_list[i], '*o23*', 'LIMIT', 0 , 0).equal([444])
        # print (time.time() - start)
        # env.expect('ft.search', index_list[i], '*oo23*', 'LIMIT', 0 , 0).equal([333])
        # env.expect('ft.search', index_list[i], '*oo234*', 'LIMIT', 0 , 0).equal([33])

        # suffix
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'*oo234'}", 'LIMIT', 0 , 0).equal([3])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'*234'}", 'LIMIT', 0 , 0).equal([40])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "@t:{w'*13'}", 'LIMIT', 0 , 0).equal([400])
        print(time.time() - start_time)
        start_time = time.time()

    # test timeout
    env.expect('FT.CONFIG', 'set', 'TIMEOUT', 1).ok()
    env.expect('FT.CONFIG', 'set', 'ON_TIMEOUT', 'RETURN').ok()
    env.expect('ft.search', index_list[0], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

    env.expect('FT.CONFIG', 'set', 'ON_TIMEOUT', 'FAIL').ok()
    env.expect('ft.search', index_list[0], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

def testBenchmark(env):
  env.skip()
  env.skipOnCluster()
  env.expect('FT.CONFIG', 'set', 'MINPREFIX', 1).ok()
  env.expect('FT.CONFIG', 'set', 'DEFAULT_DIALECT', 2).ok()
  env.expect('FT.CONFIG', 'set', 'TIMEOUT', 100000).ok()
  env.expect('FT.CONFIG', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
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


def testBasic(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)

  env.expect('FT.CONFIG', 'set', 'MINPREFIX', 1).ok()
  env.expect('FT.CONFIG', 'set', 'DEFAULT_DIALECT', 2).ok()
  env.expect('FT.CONFIG', 'set', 'TIMEOUT', 100000).ok()

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

  env.expect('FT.DEBUG', 'dump_terms', 'idx').equal(
      ['hello', "hello'", "hello'world", 'hello\\', 'hello\\world', 'help', 'hallelujah',
       'halloween', 'jello','jellyfish', 'mellow', "'hello", '\\hello'])

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
  env.expect('FT.SEARCH', 'idx', "w'*\\*'").equal([3, 'doc8', ['t', 'hello\\\\world'], # *\*
                                                      'doc10', ['t', '\\\\hello'],
                                                      'doc12', ['t', 'hello\\\\']])
  env.expect('FT.SEARCH', 'idx', "w'*o\\w*'").equal([1, 'doc8', ['t', "hello\\\\world"]]) # *o\w*


  # test with PARAMS
  # escape \'
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*\\\'*").equal([3, 'doc7', ['t', "hello\\'world"], # *'*
                                                        'doc9', ['t', "\\'hello"],
                                                        'doc11', ['t', "hello\\'"]])
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*o\\\'w*").equal([1, 'doc7', ['t', "hello\\'world"]]) # *o'w*

  # escape \\
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*\\*").equal([3, 'doc8', ['t', 'hello\\\\world'], # *\*
                                                      'doc10', ['t', '\\\\hello'],
                                                      'doc12', ['t', 'hello\\\\']])
  env.expect('FT.SEARCH', 'idx', "w'$wcq'", 'PARAMS', '2', 'wcq', "*o\w*").equal([1, 'doc8', ['t', "hello\\\\world"]]) # *o\w*
  ''''''
