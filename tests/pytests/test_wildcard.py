from includes import *
from common import *
from RLTest import Env
import time

def testSanity(env):
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MINPREFIX', 1).ok()
    env.expect('ft.config', 'set', 'DEFAULT_DIALECT', 2).ok()
    env.expect('ft.config', 'set', 'TIMEOUT', 100000).ok()
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
    item_qty = 10000

    index_list = ['idx_bf']
    env.cmd('ft.create', 'idx_bf', 'SCHEMA', 't', 'TEXT')
    #env.cmd('ft.create', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

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
    env.expect('ft.config', 'set', 'TIMEOUT', 1).ok()
    env.expect('ft.config', 'set', 'ON_TIMEOUT', 'RETURN').ok()
    env.expect('ft.search', index_list[0], "w'foo*'", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

    env.expect('ft.config', 'set', 'ON_TIMEOUT', 'FAIL').ok()
    env.expect('ft.search', index_list[0], "w'foo*'", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

def testSanityTag(env):
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MINPREFIX', 1).ok()
    env.expect('ft.config', 'set', 'DEFAULT_DIALECT', 2).ok()
    env.expect('ft.config', 'set', 'TIMEOUT', 100000).ok()
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
    item_qty = 10000

    index_list = ['idx_bf']
    env.cmd('ft.create', 'idx_bf', 'SCHEMA', 't', 'TAG', 'SORTABLE')
    #env.cmd('ft.create', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

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
    env.expect('ft.config', 'set', 'TIMEOUT', 1).ok()
    env.expect('ft.config', 'set', 'ON_TIMEOUT', 'RETURN').ok()
    env.expect('ft.search', index_list[0], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

    env.expect('ft.config', 'set', 'ON_TIMEOUT', 'FAIL').ok()
    env.expect('ft.search', index_list[0], "@t:{w'foo*'}", 'LIMIT', 0 , 0).error() \
      .contains('Timeout limit was reached')
    #env.expect('ft.search', index_list[1], 'foo*', 'LIMIT', 0 , 0).error() \
    #  .contains('Timeout limit was reached')

def testBenchmark(env):
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MINPREFIX', 1).ok()
    env.expect('ft.config', 'set', 'DEFAULT_DIALECT', 2).ok()
    env.expect('ft.config', 'set', 'TIMEOUT', 100000).ok()
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
    item_qty = 1000000

    index_list = ['idx_bf']
    env.cmd('ft.create', 'idx_bf', 'SCHEMA', 't', 'TEXT')
    #env.cmd('ft.create', 'idx_suffix', 'SCHEMA', 't', 'TEXT', 'WITHSUFFIXTRIE')

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
        env.expect('ft.search', index_list[i], "*ooo1*", 'LIMIT', 0 , 0).equal([2222])
        print(time.time() - start_time)
        start_time = time.time()
        env.expect('ft.search', index_list[i], "w'*o**o1*'", 'LIMIT', 0 , 0).equal([2222])
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
