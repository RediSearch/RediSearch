from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList


def testBasicSynonymsUseCase(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child'), 'OK')

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

def testTermOnTwoSynonymsGroup(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child'), 'OK')
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id2', 'boy', 'offspring'), 'OK')

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')

    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

    res = r.execute_command('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

def testSynonymGroupWithThreeSynonyms(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1L, 'doc1',])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))
    res = r.execute_command('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

def testSynonymWithMultipleDocs(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'she is a girl',
                                    'body', 'the child sister'))

    res = r.execute_command('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0], 2L)
    env.assertEqual(res[1], 'doc1')
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))
    env.assertEqual(res[3], 'doc2')
    env.assertEqual(set(res[4]), set(['title', 'she is a girl', 'body', 'the child sister']))
    

def testSynonymUpdate(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'SKIPINITIALSCAN', 'boy', 'child', 'offspring'), 'OK')
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a baby',
                                    'body', 'this is a test'))

    env.assertOk(r.execute_command('ft.synupdate', 'idx', 'id1', 'SKIPINITIALSCAN', 'baby'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'he is another baby',
                                    'body', 'another test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    # synonyms are applied from the moment they were added, previuse docs are not reindexed
    env.assertEqual(res[0:2], [1L, 'doc2'])
    env.assertEqual(set(res[2]), set(['title', 'he is another baby', 'body', 'another test']))

def testSynonymDump(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id2', 'baby', 'child'), 'OK')
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id3', 'tree', 'wood'), 'OK')
    res = r.execute_command('ft.syndump', 'idx')
    res = {res[i] : res[i + 1] for i in range(0,len(res),2)}
    env.assertEqual(res, {'boy': ['id1'], 'tree': ['id3'], 'wood': ['id3'], 'child': ['id1', 'id2'], 'baby': ['id2'], 'offspring': ['id1']})

def testSynonymUpdateWorngArity(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child')
    with env.assertResponseError(contained='wrong number of arguments'):
        r.execute_command('ft.synupdate', 'idx', 'id1')

def testSynonymUpdateUnknownIndex(env):
    env.expect('ft.synupdate', 'idx', '0', 'child').error().contains('Unknown index name')

def testSynonymDumpWorngArity(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child')

    env.expect('ft.syndump').error().contains('wrong number of arguments')
    env.expect('ft.syndump idx foo').error().contains('wrong number of arguments')

def testSynonymUnknownIndex(env):
    r = env
    exceptionStr = None
    try:
        r.execute_command('ft.syndump', 'idx')
    except Exception as e:
        exceptionStr = str(e)
    env.assertEqual(exceptionStr, 'Unknown index name')

def testSynonymsRdb(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')
    for _ in env.reloading_iterator():
        waitForIndex(env, 'idx')
        res = r.execute_command('ft.syndump', 'idx')
        res = {res[i] : res[i + 1] for i in range(0,len(res),2)}
        env.assertEqual(res, {'boy': ['id1'], 'offspring': ['id1'], 'child': ['id1']})

def testTwoSynonymsSearch(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy child boy',
                                    'body', 'another test'))

    res = r.execute_command('ft.search', 'idx', 'offspring offspring', 'EXPANDER', 'SYNONYM')
    # synonyms are applied from the moment they were added, previuse docs are not reindexed
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy child boy', 'body', 'another test']))

def testSynonymsIntensiveLoad(env):
    iterations = 1000
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text'))
    for i in range(iterations):
        env.assertEqual(r.execute_command('ft.synupdate', 'idx', 'id%d' % i, 'boy%d' % i, 'child%d' % i, 'offspring%d' % i), 'OK')
    for i in range(iterations):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'title', 'he is a boy%d' % i,
                                        'body', 'this is a test'))
    for _ in env.reloading_iterator():
        waitForIndex(r, 'idx')
        for i in range(iterations):
            res = r.execute_command('ft.search', 'idx', 'child%d' % i, 'EXPANDER', 'SYNONYM')
            env.assertEqual(res[0:2], [1L, 'doc%d' % i])
            env.assertEqual(set(res[2]), set(['title', 'he is a boy%d' % i, 'body', 'this is a test']))

def testSynonymsLowerCase(env):
    env.expect('FT.CREATE lowcase ON HASH SCHEMA foo text').ok()
    env.expect('FT.SYNUPDATE lowcase id1 HELLO SHALOM AHALAN').ok()
    dump = env.cmd('FT.SYNDUMP lowcase')
    env.assertEqual(toSortedFlatList(dump), toSortedFlatList((['ahalan', ['id1'], 'shalom', ['id1'], 'hello', ['id1']])))
    env.expect('FT.ADD lowcase doc1 1 FIELDS foo hello').ok()
    env.expect('FT.ADD lowcase doc2 1 FIELDS foo HELLO').ok()
    res = [2L, 'doc1', ['foo', 'hello'], 'doc2', ['foo', 'HELLO']]
    env.expect('FT.SEARCH lowcase SHALOM').equal(res)
    env.expect('FT.SEARCH lowcase shalom').equal(res)

def testSkipInitialIndex(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE idx1 SCHEMA foo text').ok()
    env.expect('FT.CREATE idx2 SCHEMA foo text').ok()
    conn.execute_command('HSET', 'doc1', 'foo', 'bar')

    env.expect('FT.SEARCH idx1 @foo:xyz').equal([0L])
    env.expect('FT.SEARCH idx2 @foo:xyz').equal([0L])

    env.expect('FT.SYNUPDATE idx1 g1 bar xyz').ok()
    env.expect('FT.SYNUPDATE idx2 g2 SKIPINITIALSCAN bar xyz').ok()

    waitForIndex(env, 'idx1')

    env.expect('FT.SEARCH idx1 @foo:xyz').equal([1L, 'doc1', ['foo', 'bar']])
    env.expect('FT.SEARCH idx2 @foo:xyz').equal([0L])
