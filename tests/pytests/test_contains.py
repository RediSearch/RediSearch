from includes import *
from common import *
import os
import csv


def testBasicContains(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.expect('HSET', 'doc1', 'title', 'hello world', 'body', 'this is a test') \
                .equal(2)

    # prefix
    res = r.execute_command('ft.search', 'idx', 'worl*')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

    # suffix
    res = r.execute_command('ft.search', 'idx', '*orld')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))
    r.expect('ft.search', 'idx', '*orl').equal([0])

    # contains
    res = r.execute_command('ft.search', 'idx', '*orl*')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

def testSanity(env):
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MINPREFIX', 1).ok()
    env.expect('ft.config', 'set', 'TIMEOUT', 100000).ok()
    item_qty = 1000000
    query_qty = 1

    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'withcontains')
    # , 'WITHCONTAINS'
    pl = conn.pipeline()
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')

    start = time.time()
    for i in range(item_qty):
        pl.execute_command('HSET', 'doc%d' % i, 't', 'foo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty), 't', 'fooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 2), 't', 'foooo%d' % i)
        pl.execute_command('HSET', 'doc%d' % (i + item_qty * 3), 't', 'foofo%d' % i)
        pl.execute()
    start_time = time.time()
    env.expect('ft.search', 'idx', 'f*', 'LIMIT', 0 , 0).equal([4])
    print (time.time() - start_time)
    start_time = time.time()
    env.expect('ft.profile', 'idx', 'search', 'limited', 'query', 'f*', 'LIMIT', 0 , 0, 'TIMEOUT', 1).equal([4])
    print (time.time() - start_time)
    start_time = time.time()
    env.expect('ft.profile', 'idx', 'search', 'lRedisModuleInfoFuncited', 'query', '*o3*', 'LIMIT', 0 , 0, 'TIMEOUT', 1).equal([4])
    print (time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', 'idx', '*555*', 'LIMIT', 0 , 0).equal([4])
    print (time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', 'idx', '*55*', 'LIMIT', 0 , 0).equal([76])
    print (time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', 'idx', '*23*', 'LIMIT', 0 , 0).equal([80])
    print (time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', 'idx', '*oo55*', 'LIMIT', 0 , 0).equal([33])
    print (time.time() - start_time)
    start_time = time.time()
    env.expect('ft.search', 'idx', '*oo555*', 'LIMIT', 0 , 0).equal([3])
    
    # we get up to 200 results since MAXEXPANSIONS is set to 200
    print (time.time() - start_time)
    start_time = time.time()
    # env.expect('ft.search', 'idx', '*oo*', 'LIMIT', 0 , 0).equal([200])
    # print (time.time() - start_time)
    # start_time = time.time()
    # env.expect('ft.search', 'idx', '*o*', 'LIMIT', 0 , 0).equal([200])
    # #env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000).equal('OK')
    # print (time.time() - start_time)
    # start_time = time.time()
    # env.expect('ft.search', 'idx', '*oo*', 'LIMIT', 0 , 0).equal([4000])
    # print (time.time() - start_time)
    # start_time = time.time()
    # env.expect('ft.search', 'idx', '*o*', 'LIMIT', 0 , 0).equal([4000])
    # #env.expect('ft.config', 'set', 'MAXEXPANSIONS', 200).equal('OK')
    # print (time.time() - start_time)
    # start_time = time.time()
            
    env.expect('ft.search', 'idx', '555*', 'LIMIT', 0 , 0).equal([0])
    print (time.time() - start_time)
    start_time = time.time()
    # env.expect('ft.search', 'idx', 'foo55*', 'LIMIT', 0 , 0).equal([11])
    # print (time.time() - start_time)
    # start_time = time.time()
    # env.expect('ft.search', 'idx', 'foo23*', 'LIMIT', 0 , 0).equal([11])
    # print (time.time() - start_time)
    # start_time = time.time()

def testBible(env):
    # env.skip()
    # env.expect('ft.config', 'set', 'MINPREFIX', 1).ok()
    # env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000).equal('OK')
    # https://www.gutenberg.org/cache/epub/10/pg10.txt
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 10000000).equal('OK')
    env.expect('ft.config', 'set', 'UNION_ITERATOR_HEAP', 10000000).equal('OK')

    reader = csv.reader(open('/home/ariel/redis/RediSearch/bible.txt','r'))
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
        # env.expect('ft.search', 'idx', 'thy*', 'LIMIT', 0 , 0).equal([4071])
        # env.expect('ft.search', 'idx', 'mos*', 'LIMIT', 0 , 0).equal([992])
        # env.expect('ft.search', 'idx', 'alt*', 'LIMIT', 0 , 0).equal([478])
        # env.expect('ft.search', 'idx', 'ret*', 'LIMIT', 0 , 0).equal([471])
        # env.expect('ft.search', 'idx', 'mo*', 'LIMIT', 0 , 0).equal([4526])
        # env.expect('ft.search', 'idx', 'go*', 'LIMIT', 0 , 0).equal([7987])
        # env.expect('ft.search', 'idx', 'll*', 'LIMIT', 0 , 0).equal([0])
        # env.expect('ft.search', 'idx', 'oo*', 'LIMIT', 0 , 0).equal([0])
        # env.expect('ft.search', 'idx', 'r*', 'LIMIT', 0 , 0).equal([2572])
        # contains
        env.expect('ft.search', 'idx', '*thy*', 'LIMIT', 0 , 0).equal([4173])
        env.expect('ft.search', 'idx', '*mos*', 'LIMIT', 0 , 0).equal([1087])
        env.expect('ft.search', 'idx', '*alt*', 'LIMIT', 0 , 0).equal([2233])
        env.expect('ft.search', 'idx', '*ret*', 'LIMIT', 0 , 0).equal([1967])
        env.expect('ft.search', 'idx', '*mo*', 'LIMIT', 0 , 0).equal([4250])
        env.expect('ft.search', 'idx', '*go*', 'LIMIT', 0 , 0).equal([8246])
        env.expect('ft.search', 'idx', '*ll*', 'LIMIT', 0 , 0).equal([7712])
        env.expect('ft.search', 'idx', '*oo*', 'LIMIT', 0 , 0).equal([4530])
        env.expect('ft.search', 'idx', '*r*', 'LIMIT', 0 , 0).equal([3999])
        # contains profile
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', '*thy*', 'LIMIT', 0 , 0).equal([4173])
        env.expect('ft.profile', 'idx', 'search', 'query', '*mos*', 'LIMIT', 0 , 0).equal([1087])
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', '*alt*', 'LIMIT', 0 , 0).equal([2233])
        env.expect('ft.profile', 'idx', 'search', 'query', '*retu*', 'LIMIT', 0 , 0).equal([1967])
        env.expect('ft.profile', 'idx', 'search', 'query', '*reta*', 'LIMIT', 0 , 0).equal([1967])
        env.expect('ft.profile', 'idx', 'search', 'limited', 'query', '*mo*', 'LIMIT', 0 , 0).equal([4250])
        env.expect('ft.profile', 'idx', 'search', 'limited', 'query', '*go*', 'LIMIT', 0 , 0).equal([8246])
        env.expect('ft.profile', 'idx', 'search', 'limited', 'query', '*ll*', 'LIMIT', 0 , 0).equal([7712])
        env.expect('ft.profile', 'idx', 'search', 'limited', 'query', '*oo*', 'LIMIT', 0 , 0).equal([4530])
        # env.expect('ft.profile', 'idx', 'search', 'limited', 'query', '*r*', 'LIMIT', 0 , 0).equal([3999])
        # # suffix
        # env.expect('ft.search', 'idx', '*thy', 'LIMIT', 0 , 0).equal([3980])
        # env.expect('ft.search', 'idx', '*mos', 'LIMIT', 0 , 0).equal([14])
        # env.expect('ft.search', 'idx', '*alt', 'LIMIT', 0 , 0).equal([1672])
        # env.expect('ft.search', 'idx', '*ret', 'LIMIT', 0 , 0).equal([200])
        # env.expect('ft.search', 'idx', '*mo', 'LIMIT', 0 , 0).equal([14])
        # env.expect('ft.search', 'idx', '*go', 'LIMIT', 0 , 0).equal([1606])
        # env.expect('ft.search', 'idx', '*ll', 'LIMIT', 0 , 0).equal([16520])
        # env.expect('ft.search', 'idx', '*oo', 'LIMIT', 0 , 0).equal([52])
        # env.expect('ft.search', 'idx', '*r', 'LIMIT', 0 , 0).equal([7201])

    #env.expect('ft.profile', 'idx', 'search', 'query', 'thy*').equal('OK')
    #env.expect('ft.info', 'idx').equal('OK')
    print (time.time() - start)
    conn.execute_command('SAVE')
    input('stop')

def testEscape(env):
  # this test check that `\*` is escaped correctly on contains queries
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')    
  conn.execute_command('HSET', 'doc1', 't', '1foo1')
  conn.execute_command('HSET', 'doc2', 't', '\*foo2')
  conn.execute_command('HSET', 'doc3', 't', '3\*foo3')
  conn.execute_command('HSET', 'doc4', 't', '4foo\*')
  conn.execute_command('HSET', 'doc5', 't', '5foo\*5')
  all_docs = [5, 'doc1', ['t', '1foo1'], 'doc2', ['t', '\\*foo2'], 'doc3', ['t', '3\\*foo3'],
                 'doc4', ['t', '4foo\\*'], 'doc5', ['t', '5foo\\*5']]
  # contains
  env.expect('ft.search', 'idx', '*foo*').equal(all_docs)
  # prefix only
  env.expect('ft.search', 'idx', '\*foo*').equal([1, 'doc2', ['t', '\\*foo2']])
  # suffix only
  env.expect('ft.search', 'idx', '*foo\*').equal([1, 'doc4', ['t', '4foo\\*']])
  # none
  env.expect('ft.search', 'idx', '\*foo\*').equal([0])

def testWithContains(env):
  # this test check that `\*` is escaped correctly on contains queries
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'WITHCONTAINS', 'SORTABLE')


  env.expect('HSET', 'doc1', 't', 'world').equal(1)
  env.expect('HSET', 'doc2', 't', 'keyword').equal(1)
  env.expect('HSET', 'doc3', 't', 'doctorless').equal(1)
  env.expect('HSET', 'doc4', 't', 'anteriorly').equal(1)
  env.expect('HSET', 'doc5', 't', 'colorlessness').equal(1)
  env.expect('HSET', 'doc6', 't', 'floorless').equal(1)

  # prefix
  # res = env.execute_command('ft.search', 'idx', 'worl*')
  # env.assertEqual(res[0:2], [1, 'doc1'])
  # env.assertEqual(set(res[2]), set(['hello world', 't']))

  # contains
  res = env.execute_command('ft.search', 'idx', '*orld*')
  env.assertEqual(res, [1, 'doc1'])
  res = env.execute_command('ft.search', 'idx', '*orl*')
  env.assertEqual(res, [1, 'doc1'])
  res = env.execute_command('ft.search', 'idx', '*or*')
  env.assertEqual(res, [1, 'doc1'])
  #env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))
  

  # suffix
  res = env.execute_command('ft.search', 'idx', '*orld')
  env.assertEqual(res[0:2], [1, 'doc1'])
  res = env.execute_command('ft.search', 'idx', '*orld')
  env.assertEqual(res[0:2], [1, 'doc1'])
  res = env.execute_command('ft.search', 'idx', '*orld')
  env.assertEqual(res[0:2], [1, 'doc1'])
  #env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))
  #env.expect('ft.search', 'idx', '*orl').equal([0])