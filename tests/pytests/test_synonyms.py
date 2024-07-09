from includes import *
from common import getConnectionByEnv, waitForIndex, toSortedFlatList


def testBasicSynonymsUseCase(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child'), 'OK')

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test').ok()

    res = env.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

def testTermOnTwoSynonymsGroup(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child'), 'OK')
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id2', 'boy', 'offspring'), 'OK')

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
               'title', 'he is a boy', 'body', 'this is a test').ok()

    res = env.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')

    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

    res = env.cmd('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

def testSynonymGroupWithThreeSynonyms(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
               'title', 'he is a boy', 'body', 'this is a test').ok()

    res = env.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1, 'doc1',])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))
    res = env.cmd('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))

def testSynonymWithMultipleDocs(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
               'title', 'he is a boy', 'body', 'this is a test').ok()

    env.expect('ft.add', 'idx', 'doc2', 1.0, 'fields',
               'title', 'she is a girl', 'body', 'the child sister').ok()

    res = env.cmd('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res[0], 2)
    env.assertEqual(res[1], 'doc1')
    env.assertEqual(set(res[2]), set(['title', 'he is a boy', 'body', 'this is a test']))
    env.assertEqual(res[3], 'doc2')
    env.assertEqual(set(res[4]), set(['title', 'she is a girl', 'body', 'the child sister']))


def testSynonymUpdate(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'SKIPINITIALSCAN', 'boy', 'child', 'offspring'), 'OK')
    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
               'title', 'he is a baby', 'body', 'this is a test').ok()

    env.expect('ft.synupdate', 'idx', 'id1', 'SKIPINITIALSCAN', 'baby').ok()

    env.expect('ft.add', 'idx', 'doc2', 1.0, 'fields',
               'title', 'he is another baby', 'body', 'another test').ok()

    res = env.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    # synonyms are applied from the moment they were added, previuse docs are not reindexed
    env.assertEqual(res[0:2], [1, 'doc2'])
    env.assertEqual(set(res[2]), set(['title', 'he is another baby', 'body', 'another test']))

def testSynonymDump(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id2', 'baby', 'child'), 'OK')
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id3', 'tree', 'wood'), 'OK')
    res = env.cmd('ft.syndump', 'idx')
    res = {res[i] : res[i + 1] for i in range(0,len(res),2)}
    env.assertEqual(res, {'boy': ['id1'], 'tree': ['id3'], 'wood': ['id3'], 'child': ['id1', 'id2'], 'baby': ['id2'], 'offspring': ['id1']})

def testSynonymUpdateWorngArity(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child')
    with env.assertResponseError(contained='wrong number of arguments'):
        env.cmd('ft.synupdate', 'idx', 'id1')

def testSynonymUpdateUnknownIndex(env):
    env.expect('ft.synupdate', 'idx', '0', 'child').error().contains('Unknown index name')

def testSynonymDumpWorngArity(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child')

    env.expect('ft.syndump').error().contains('wrong number of arguments')
    env.expect('ft.syndump idx foo').error().contains('wrong number of arguments')

def testSynonymUnknownIndex(env):
    env.expect('ft.syndump', 'idx').error().equal('Unknown index name')

def testSynonymsRdb(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('ft.syndump', 'idx')
        res = {res[i] : res[i + 1] for i in range(0,len(res),2)}
        env.assertEqual(res, {'boy': ['id1'], 'offspring': ['id1'], 'child': ['id1']})

def testTwoSynonymsSearch(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')
    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy child boy',
                                    'body', 'another test').ok()

    res = env.cmd('ft.search', 'idx', 'offspring offspring', 'EXPANDER', 'SYNONYM')
    # synonyms are applied from the moment they were added, previous docs are not reindexed
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'he is a boy child boy', 'body', 'another test']))

def testSynonymsIntensiveLoad(env):
    iterations = 1000
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    for i in range(iterations):
        env.assertEqual(env.cmd('ft.synupdate', 'idx', 'id%d' % i, 'boy%d' % i, 'child%d' % i, 'offspring%d' % i), 'OK')
    for i in range(iterations):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                   'title', 'he is a boy%d' % i, 'body', 'this is a test').ok()
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for i in range(iterations):
            res = env.cmd('ft.search', 'idx', 'child%d' % i, 'EXPANDER', 'SYNONYM')
            env.assertEqual(res[0:2], [1, 'doc%d' % i])
            env.assertEqual(set(res[2]), set(['title', 'he is a boy%d' % i, 'body', 'this is a test']))

            # Test using PARAMS
            res = env.cmd('ft.search', 'idx', '$p', 'EXPANDER', 'SYNONYM',
                          'PARAMS', 2, 'p', 'child%d' % i, 'DIALECT', 2)
            env.assertEqual(res[0:2], [1, 'doc%d' % i])
            env.assertEqual(set(res[2]), set(['title', 'he is a boy%d' % i, 'body', 'this is a test']))

def testSynonymsLowerCase(env):
    env.expect('FT.CREATE lowcase ON HASH SCHEMA foo text').ok()
    env.expect('FT.SYNUPDATE lowcase id1 HELLO SHALOM AHALAN').ok()
    dump = env.cmd('FT.SYNDUMP lowcase')
    env.assertEqual(toSortedFlatList(dump), toSortedFlatList((['ahalan', ['id1'], 'shalom', ['id1'], 'hello', ['id1']])))
    env.expect('FT.ADD lowcase doc1 1 FIELDS foo hello').ok()
    env.expect('FT.ADD lowcase doc2 1 FIELDS foo HELLO').ok()
    res = [2, 'doc1', ['foo', 'hello'], 'doc2', ['foo', 'HELLO']]
    env.expect('FT.SEARCH lowcase SHALOM').equal(res)
    env.expect('FT.SEARCH lowcase shalom').equal(res)

def testSkipInitialIndex(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE idx1 SCHEMA foo text').ok()
    env.expect('FT.CREATE idx2 SCHEMA foo text').ok()
    conn.execute_command('HSET', 'doc1', 'foo', 'bar')

    env.expect('FT.SEARCH idx1 @foo:xyz').equal([0])
    env.expect('FT.SEARCH idx2 @foo:xyz').equal([0])

    env.expect('FT.SYNUPDATE idx1 g1 bar xyz').ok()
    env.expect('FT.SYNUPDATE idx2 g2 SKIPINITIALSCAN bar xyz').ok()

    waitForIndex(env, 'idx1')

    env.expect('FT.SEARCH idx1 @foo:xyz').equal([1, 'doc1', ['foo', 'bar']])
    env.expect('FT.SEARCH idx2 @foo:xyz').equal([0])

def testDoubleDefinition(env):
    env.expect('FT.CREATE idx SCHEMA t text').ok()
    # Add the same synonym twice
    env.expect('FT.SYNUPDATE idx foo bar').ok()
    env.expect('FT.SYNUPDATE idx foo bar').ok()
    # Ensure it's only added once
    env.expect('FT.SYNDUMP idx').equal(['bar', ['foo']])
