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
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MINPREFIX', 1).ok()
    item_qty = 1000
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

    env.expect('ft.search', 'idx', '*555*', 'LIMIT', 0 , 0).equal([4L])
    env.expect('ft.search', 'idx', '*55*', 'LIMIT', 0 , 0).equal([76L])
    env.expect('ft.search', 'idx', '*23*', 'LIMIT', 0 , 0).equal([80])
    env.expect('ft.search', 'idx', '*oo55*', 'LIMIT', 0 , 0).equal([33L])
    env.expect('ft.search', 'idx', '*oo555*', 'LIMIT', 0 , 0).equal([3L])
    
    # we get up to 200 results since MAXEXPANSIONS is set to 200
    env.expect('ft.search', 'idx', '*oo*', 'LIMIT', 0 , 0).equal([200L])
    env.expect('ft.search', 'idx', '*o*', 'LIMIT', 0 , 0).equal([200L])
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000).equal('OK')
    env.expect('ft.search', 'idx', '*oo*', 'LIMIT', 0 , 0).equal([4000L])
    env.expect('ft.search', 'idx', '*o*', 'LIMIT', 0 , 0).equal([4000L])
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 200).equal('OK')
            
    env.expect('ft.search', 'idx', '555*', 'LIMIT', 0 , 0).equal([0L])
    env.expect('ft.search', 'idx', 'foo55*', 'LIMIT', 0 , 0).equal([11])
    env.expect('ft.search', 'idx', 'foo23*', 'LIMIT', 0 , 0).equal([11])

def testBible(env):
    env.skip()
    # env.expect('ft.config', 'set', 'MINPREFIX', 1).ok()
    # env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000).equal('OK')
    # https://www.gutenberg.org/cache/epub/10/pg10.txt
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
    conn.execute_command('SAVE')
    start = time.time()    
    for _ in range(1):
        # prefix
        env.expect('ft.search', 'idx', 'thy*', 'LIMIT', 0 , 0).equal([4071L])
        env.expect('ft.search', 'idx', 'mos*', 'LIMIT', 0 , 0).equal([992L])
        env.expect('ft.search', 'idx', 'alt*', 'LIMIT', 0 , 0).equal([478L])
        env.expect('ft.search', 'idx', 'ret*', 'LIMIT', 0 , 0).equal([471L])
        env.expect('ft.search', 'idx', 'mo*', 'LIMIT', 0 , 0).equal([4526L])
        env.expect('ft.search', 'idx', 'go*', 'LIMIT', 0 , 0).equal([7987L])
        env.expect('ft.search', 'idx', 'll*', 'LIMIT', 0 , 0).equal([0L])
        env.expect('ft.search', 'idx', 'oo*', 'LIMIT', 0 , 0).equal([0L])
        env.expect('ft.search', 'idx', 'r*', 'LIMIT', 0 , 0).equal([2572L])
        # contains
        env.expect('ft.search', 'idx', '*thy*', 'LIMIT', 0 , 0).equal([4173L])
        env.expect('ft.search', 'idx', '*mos*', 'LIMIT', 0 , 0).equal([1087L])
        env.expect('ft.search', 'idx', '*alt*', 'LIMIT', 0 , 0).equal([2233L])
        env.expect('ft.search', 'idx', '*ret*', 'LIMIT', 0 , 0).equal([1967])
        env.expect('ft.search', 'idx', '*mo*', 'LIMIT', 0 , 0).equal([4250L])
        env.expect('ft.search', 'idx', '*go*', 'LIMIT', 0 , 0).equal([8246L])
        env.expect('ft.search', 'idx', '*ll*', 'LIMIT', 0 , 0).equal([7712L])
        env.expect('ft.search', 'idx', '*oo*', 'LIMIT', 0 , 0).equal([4530L])
        env.expect('ft.search', 'idx', '*r*', 'LIMIT', 0 , 0).equal([3999L])
        # suffix
        env.expect('ft.search', 'idx', '*thy', 'LIMIT', 0 , 0).equal([3980L])
        env.expect('ft.search', 'idx', '*mos', 'LIMIT', 0 , 0).equal([14L])
        env.expect('ft.search', 'idx', '*alt', 'LIMIT', 0 , 0).equal([1672L])
        env.expect('ft.search', 'idx', '*ret', 'LIMIT', 0 , 0).equal([200L])
        env.expect('ft.search', 'idx', '*mo', 'LIMIT', 0 , 0).equal([14L])
        env.expect('ft.search', 'idx', '*go', 'LIMIT', 0 , 0).equal([1606L])
        env.expect('ft.search', 'idx', '*ll', 'LIMIT', 0 , 0).equal([16520L])
        env.expect('ft.search', 'idx', '*oo', 'LIMIT', 0 , 0).equal([52L])
        env.expect('ft.search', 'idx', '*r', 'LIMIT', 0 , 0).equal([7201L])

    #env.expect('ft.profile', 'idx', 'search', 'query', 'thy*').equal('OK')
    #env.expect('ft.info', 'idx').equal('OK')
    print (time.time() - start)
    input('stop')
