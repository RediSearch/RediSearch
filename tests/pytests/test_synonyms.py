from includes import *


def testBasicSynonymsUseCase(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child'), 0)

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

def testTermOnTwoSynonymsGroup(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child'), 0)
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'offspring'), 1)

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])
    res = r.execute_command('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

def testSynonymGroupWithThreeSynonyms(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])
    res = r.execute_command('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

def testSynonymWithMultipleDocs(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy',
                                    'body', 'this is a test'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'she is a girl',
                                    'body', 'the child sister'))

    res = r.execute_command('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [2L, 'doc2', ['title', 'she is a girl', 'body', 'the child sister'], 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

def testSynonymUpdate(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a baby',
                                    'body', 'this is a test'))

    env.assertOk(r.execute_command('ft.synupdate', 'idx', '0', 'baby'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'he is another baby',
                                    'body', 'another test'))

    res = r.execute_command('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
    # synonyms are applied from the moment they were added, previuse docs are not reindexed
    env.assertEqual(res, [1L, 'doc2', ['title', 'he is another baby', 'body', 'another test']])

def testSynonymDump(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'baby', 'child'), 1)
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'tree', 'wood'), 2)
    env.assertEqual(r.execute_command('ft.syndump', 'idx'), ['baby', [1L], 'offspring', [0L], 'wood', [2L], 'tree', [2L], 'child', [0L, 1L], 'boy', [0L]])

def testSynonymAddWorngArity(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    exceptionStr = None
    try:
        r.execute_command('ft.synadd', 'idx')
    except Exception as e:
        exceptionStr = str(e)
    env.assertEqual(exceptionStr, 'wrong number of arguments for \'ft.synadd\' command')

def testSynonymAddUnknownIndex(env):
    r = env
    exceptionStr = None
    try:
        r.execute_command('ft.synadd', 'idx', 'boy', 'child')
    except Exception as e:
        exceptionStr = str(e)
    env.assertEqual(exceptionStr, 'Unknown index name')

def testSynonymUpdateWorngArity(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    r.execute_command('ft.synadd', 'idx', 'boy', 'child')
    with env.assertResponseError(contained='wrong number of arguments'):
        r.execute_command('ft.synupdate', 'idx', '0')

def testSynonymUpdateUnknownIndex(env):
    env.expect('ft.synupdate', 'idx', '0', 'child').error().contains('Unknown index name')
    env.expect('ft.synupdate', 'idx', '10000000000', 'child').error().contains('wrong parameters, id out of range')

def testSynonymUpdateNotNumberId(env):
    r = env
    exceptionStr = None
    try:
        r.execute_command('ft.synupdate', 'idx', 'test', 'child')
    except Exception as e:
        exceptionStr = str(e)
    env.assertEqual(exceptionStr, 'wrong parameters, id is not an integer')

def testSynonymUpdateOutOfRangeId(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    r.execute_command('ft.synadd', 'idx', 'boy', 'child')
    exceptionStr = None
    try:
        r.execute_command('ft.synupdate', 'idx', '1', 'child')
    except Exception as e:
        exceptionStr = str(e)
    env.assertEqual(exceptionStr, 'given id does not exists')

def testSynonymDumpWorngArity(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    r.execute_command('ft.synadd', 'idx', 'boy', 'child')
    exceptionStr = None
    try:
        r.execute_command('ft.syndump')
    except Exception as e:
        exceptionStr = str(e)
    env.assertEqual(exceptionStr, 'wrong number of arguments for \'ft.syndump\' command')

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
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)
    for _ in env.reloading_iterator():
        env.assertEqual(r.execute_command('ft.syndump', 'idx'), ['offspring', [0L], 'child', [0L], 'boy', [0L]])

def testTwoSynonymsSearch(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'he is a boy child boy',
                                    'body', 'another test'))

    res = r.execute_command('ft.search', 'idx', 'offspring offspring', 'EXPANDER', 'SYNONYM')
    # synonyms are applied from the moment they were added, previuse docs are not reindexed
    env.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy child boy', 'body', 'another test']])

def testSynonymsIntensiveLoad(env):
    iterations = 1000
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    for i in range(iterations):
        env.assertEqual(r.execute_command('ft.synadd', 'idx', 'boy%d' % i, 'child%d' % i, 'offspring%d' % i), i)
    for i in range(iterations):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'title', 'he is a boy%d' % i,
                                        'body', 'this is a test'))
    for _ in env.reloading_iterator():
        for i in range(iterations):
            res = r.execute_command('ft.search', 'idx', 'child%d' % i, 'EXPANDER', 'SYNONYM')
            env.assertEqual(res, [1L, 'doc%d' % i, ['title', 'he is a boy%d' % i, 'body', 'this is a test']])

def testSynonymsForceUpdate(env):
    env.expect('ft.synforceupdate', 'idx', '0', 'child').error().contains('Unknown index name')
    env.expect('ft.synforceupdate', 'idx', 'olah', 'child').error().contains('wrong parameters, id is not an integer')
    env.expect('ft.synforceupdate', 'idx', '10000000000', 'child').error().contains('wrong parameters, id out of range')