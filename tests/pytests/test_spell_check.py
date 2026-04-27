from common import *

def testDictAdd(env):
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term3').equal(3)
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term4').equal(1)

def testDictAddWrongArity(env):
    env.expect('ft.dictadd', 'dict').error()

def testDictDelete(env):
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term3').equal(3)
    env.expect('ft.dictdel', 'dict', 'term1', 'term2', 'term4').equal(2)
    env.expect('ft.dictdel', 'dict', 'term3').equal(1)
    env.expect('keys', '*').equal([])

def testDictDeleteOnFlush(env):
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term3').equal(3)
    env.expect('FLUSHALL').equal(True)
    env.expect('ft.dictdump', 'dict').equal([])
    env.expect('ft.dictadd', 'dict', 'term4', 'term5', 'term6').equal(3)
    env.expect('ft.dictdump', 'dict').equal(['term4', 'term5', 'term6'])

def testDictDeleteWrongArity(env):
    env.expect('ft.dictdel', 'dict').error()

def testDictDeleteOnNoneExistingKey(env):
    env.expect('ft.dictdel', 'dict', 'term1').equal(0)

def testDictDump(env):
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term3').equal(3)
    env.expect('ft.dictdump', 'dict').equal(['term1', 'term2', 'term3'])

def testDictDumpWrongArity(env):
    env.expect('ft.dictdump').error().contains('wrong number of arguments')

def testDictDumpOnNoneExistingKey(env):
    env.expect('ft.dictdump', 'dict').equal([])

def testBasicSpellCheck(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('hset', 'doc1', 'name', 'name1', 'body', 'body1')
        r.execute_command('hset', 'doc2', 'name', 'name2', 'body', 'body2')
        r.execute_command('hset', 'doc3', 'name', 'name2', 'body', 'name2')
    res = env.cmd('ft.spellcheck', 'idx', 'name')
    exp = [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1']]]]
    compare_lists(env, res, exp)

    if not env.isCluster():
        res = env.cmd('ft.spellcheck', 'idx', '@body:name')
        compare_lists(env, res, [['TERM', 'name', [['0.66666666666666663', 'name2']]]])

def testBasicSpellCheckWithNoResult(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')
    env.expect('ft.spellcheck', 'idx', 'somenotexiststext').equal([['TERM', 'somenotexiststext', []]])

def testSpellCheckOnExistingTerm(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('hset', 'doc1', 'name', 'name', 'body', 'body1')
        r.execute_command('hset', 'doc2', 'name', 'name2', 'body', 'body2')
        r.execute_command('hset', 'doc3', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')
    env.expect('ft.spellcheck', 'idx', 'name').equal([])

def testSpellCheckWithIncludeDict(env):
    env.cmd('ft.dictadd', 'dict', 'name3', 'name4', 'name5')
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')

    res = env.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
    compare_lists(env, res, [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1'],
                                               ['0', 'name3'], ['0', 'name4'], ['0', 'name5']]]])

    res = env.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'include', 'dict')
    compare_lists(env, res, [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1'],
                                               ['0', 'name3'], ['0', 'name4'], ['0', 'name5']]]])

def testSpellCheckWithDuplications(env):
    env.cmd('ft.dictadd', 'dict', 'name1', 'name4', 'name5')
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')

    res = env.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
    compare_lists(env, res, [['TERM', 'name', [['0.66666666666666663', 'name2'],
                                               ['0.33333333333333331', 'name1'],
                                               ['0', 'name4'], ['0', 'name5']]]])

def testSpellCheckExcludeDict(env):
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict').equal([])
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'exclude', 'dict').equal([])

def testSpellCheckNoneExistingIndex(env):
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict').error()

def testSpellCheckWrongArity(env):
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')
    env.expect('ft.spellcheck', 'idx').error()
    env.expect('ft.spellcheck', 'idx').error()

def testSpellCheckBadFormat(env):
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS').error()
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE').error()
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE').error()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE').error()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE', 0).error()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE', -1).error()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE', 5).error()

def testSpellCheckNoneExistingDicts(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict').error()
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict').error()

def testSpellCheckResultsOrder(env):
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'Elior', 'body', 'body1')
        r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'Hila', 'body', 'body2')
    waitForIndex(env, 'idx')
    exp = [
        ['TERM', 'elioh', [['0.5', 'elior']]],
        ['TERM', 'hilh', [['0.5', 'hila']]]
    ]
    env.expect('ft.spellcheck', 'idx', 'Elioh Hilh').equal(exp)

def testSpellCheckDictReleadRDB(env):
    env.expect('FT.DICTADD test 1 2 3').equal(3)
    for _ in env.reloadingIterator():
        env.expect('FT.DICTDUMP test').equal(['1', '2', '3'])

def testSpellCheckIssue437(env):
    env.cmd('ft.create', 'incidents', 'ON', 'HASH', 'SCHEMA', 'report', 'text')
    env.cmd('FT.DICTADD', 'slang', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')
    env.expect('FT.SPELLCHECK', 'incidents',
               'Tooni toque kerfuffle', 'TERMS',
               'EXCLUDE', 'slang', 'TERMS',
               'INCLUDE', 'slang').equal([['TERM', 'tooni', [['0', 'toonie']]]])

def test_spell_check_with_params(env:Env):
    """Test FT.SPELLCHECK with PARAMS support (MOD-10596).
    Covers parameterized queries in dialect 2 and 3, missing params error,
    and fuzzy params."""
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('hset', 'doc1', 'name', 'name1', 'body', 'body1')
        r.execute_command('hset', 'doc2', 'name', 'name2', 'body', 'body2')
        r.execute_command('hset', 'doc3', 'name', 'name2', 'body', 'name2')
        r.execute_command('hset', 'doc4', 'name', 'hello', 'body', 'help')

    # Dialect 2: parameterized query should match non-parameterized baseline
    res1 = env.cmd('ft.spellcheck', 'idx', 'name', 'DIALECT', '2')
    res2 = env.cmd('ft.spellcheck', 'idx', '$query', 'PARAMS', '2', 'query', 'name', 'DIALECT', '2')
    compare_lists(env, res1, res2)

    # Dialect 3: the exact scenario that causes the crash
    res1 = env.cmd('ft.spellcheck', 'idx', 'name', 'DIALECT', '3')
    res2 = env.cmd('ft.spellcheck', 'idx', '$query', 'PARAMS', '2', 'query', 'name', 'DIALECT', '3')
    compare_lists(env, res1, res2)

    # Missing PARAMS: $a in dialect 3 without PARAMS should error, not crash
    env.expect('ft.spellcheck', 'idx', '$a', 'DIALECT', '3').error().contains('No such parameter `a`')

    # Cover the PARAMS parsing error path
    env.expect('ft.spellcheck', 'idx', '$a', 'PARAMS', '3', 'a', 'b', 'c', 'DIALECT', '2').error().contains('Parameters must be specified in PARAM VALUE pairs')

    # Fuzzy params
    res1 = env.cmd('ft.spellcheck', 'idx', '%hell%', 'DIALECT', '2')
    res2 = env.cmd('ft.spellcheck', 'idx', '%$tok%', 'PARAMS', '2', 'tok', 'hell', 'DIALECT', '2')
    compare_lists(env, res1, res2)
