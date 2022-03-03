from includes import *
from common import *
import os
import csv



def testBasicContains(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.expect('HSET', 'doc1', 'title', 'hello world', 'body', 'this is a test') \
                .equal(2L)

    # prefix
    res = r.execute_command('ft.search', 'idx', 'worl*')
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

    # suffix
    res = r.execute_command('ft.search', 'idx', '*orld')
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))
    r.expect('ft.search', 'idx', '*orl').equal([0L])

    # contains
    res = r.execute_command('ft.search', 'idx', '*orl*')
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

def testSanity(env):
    item_qty = 1000000
    query_qty = 1

    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT')
    pl = conn.pipeline()

    start = time.time()
    for i in range(item_qty):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()
    print (time.time() - start)
    #raw_input('stop')
    #env.expect('ft.search', 'idx', '*').equal(item_qty)

    for _ in range(1):
        start = time.time()
        for i in range(query_qty):
            # res = env.execute_command('ft.search', 'idx', '555*', 'LIMIT', 0 , 1000)
            # env.assertEqual(res, 1000)

            #res = env.execute_command('ft.profile', 'idx', 'search', 'limited', 'query', '555*', 'LIMIT', 0 , 0)
            #res = env.execute_command('ft.profile', 'idx', 'search', 'limited', 'query', '23*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, 1000)
            #res = env.execute_command('ft.profile', 'idx', 'search', 'limited', 'query', 'foo55*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, 1000)
            #res = env.execute_command('ft.profile', 'idx', 'search', 'limited', 'query', 'foo555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, 1000)
            #res = env.execute_command('ft.profile', 'idx', 'search', 'limited', 'query', 'oo555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, 1000)
            #res = env.execute_command('ft.profile', 'idx', 'search', 'limited', 'query', 'o555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, 1000)

            #res = env.execute_command('ft.search', 'idx', '*555*', 'LIMIT', 0 , 100, 'NOCONTENT')
            #env.assertEqual(res, 1000)
            #res = env.execute_command('ft.search', 'idx', 'foo55*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, [111L])
            #res = env.execute_command('ft.search', 'idx', 'foo555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, [11L])
            #res = env.execute_command('ft.search', 'idx', '*oooo555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, [11L])
            #res = env.execute_command('ft.search', 'idx', '*ooo555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, [22L])
            #res = env.execute_command('ft.search', 'idx', '*oo555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, [44L])
            #res = env.execute_command('ft.search', 'idx', '*o555*', 'LIMIT', 0 , 0)
            #env.assertEqual(res, [44L])
            res = env.execute_command('ft.search', 'idx', '*23*', 'LIMIT', 0 , 0)
            res = env.execute_command('ft.search', 'idx', '*23*', 'LIMIT', 0 , 0, 'timeout', 10000)
            res = env.execute_command('ft.profile', 'idx', 'SEARCH', 'LIMITED', 'QUERY', '*23*', 'LIMIT', 0 , 0, 'timeout', 10000)
            env.assertEqual(res, [1196L])
#
        print (time.time() - start)
        raw_input('pause')

def testBible(env):
    env.skip()
    reader = csv.reader(open('/home/ariel/redis/RediSearch/bible.txt','rb'))
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT')

    i = 0
    start = time.time()    
    for line in reader:
        #print(line)
        i += 1
        conn.execute_command('HSET', 'doc%d' % i, 't', " ".join(line))
    print (time.time() - start)

    start = time.time()    
    for _ in range(1):
        env.expect('ft.search', 'idx', 'thy*', 'LIMIT', 0 , 0).equal('OK')
        env.expect('ft.search', 'idx', 'mos*', 'LIMIT', 0 , 0).equal('OK')
        env.expect('ft.search', 'idx', 'alt*', 'LIMIT', 0 , 0).equal('OK')
        env.expect('ft.search', 'idx', 'ret*', 'LIMIT', 0 , 0).equal('OK')
        #env.expect('ft.search', 'idx', 'mo*', 'LIMIT', 0 , 0).equal('OK')
        #env.expect('ft.search', 'idx', 'go*', 'LIMIT', 0 , 0).equal('OK')
        #env.expect('ft.search', 'idx', 'll*', 'LIMIT', 0 , 0).equal('OK')
        #env.expect('ft.search', 'idx', 'oo*', 'LIMIT', 0 , 0).equal('OK')
        #env.expect('ft.search', 'idx', 'r*', 'LIMIT', 0 , 0).equal('OK')

        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', 'thy*', 'LIMIT', 0 , 0).equal('OK')
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', 'mos*', 'LIMIT', 0 , 0).equal('OK')
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', 'alt*', 'LIMIT', 0 , 0).equal('OK')
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', 'ret*', 'LIMIT', 0 , 0).equal('OK')
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', 'mo*', 'LIMIT', 0 , 0).equal('OK')
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', 'go*', 'LIMIT', 0 , 0).equal('OK')

    env.expect('ft.profile', 'idx', 'search', 'query', 'thy*').equal('OK')
    env.expect('ft.info', 'idx').equal('OK')
    print (time.time() - start)
    #input('stop')
