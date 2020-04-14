from includes import *


def testBasicFuzzy(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'hello world',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', '%word%')
    env.assertEqual(res, [1L, 'doc1', ['title', 'hello world', 'body', 'this is a test']])

def testLdLimit(env):
    env.cmd('ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'title', 'hello world')
    env.assertEqual([1L, 'doc1', ['title', 'hello world']], env.cmd('ft.search', 'idx', '%word%'))  # should be ok
    env.assertEqual([0L], env.cmd('ft.search', 'idx', r'%sword%'))  # should return nothing
    env.assertEqual([1L, 'doc1', ['title', 'hello world']], env.cmd('ft.search', 'idx', r'%%sword%%'))

def testStopwords(env):
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text')
    for t in ('iwth', 'ta', 'foo', 'rof', 'whhch', 'witha'):
        env.cmd('ft.add', 'idx', t, 1.0, 'fields', 't1', t)

    r = env.cmd('ft.search', 'idx', '%for%')
    env.assertEqual([1, 'foo', ['t1', 'foo']], r)

    r = env.cmd('ft.search', 'idx', '%%with%%')
    env.assertEqual([2, 'witha', ['t1', 'witha'], 'iwth', ['t1', 'iwth']], r)

    r = env.cmd('ft.search', 'idx', '%with%')
    env.assertEqual([1, 'witha', ['t1', 'witha']], r)

    r = env.cmd('ft.search', 'idx', '%at%')
    env.assertEqual([1, 'ta', ['t1', 'ta']], r)

def testFuzzyMultipleResults(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'hello world',
                                    'body', 'this is a test'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'hello word',
                                    'body', 'this is a test'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'fields',
                                    'title', 'hello ward',
                                    'body', 'this is a test'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc4', 1.0, 'fields',
                                    'title', 'hello wakld',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', '%word%')
    env.assertEqual(res, [3L, 'doc3', ['title', 'hello ward', 'body', 'this is a test'], 'doc2', ['title', 'hello word', 'body', 'this is a test'], 'doc1', ['title', 'hello world', 'body', 'this is a test']])

def testFuzzySyntaxError(env):
    r = env
    unallowChars = ('*', '$', '~', '&', '@', '!')
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'hello world',
                                    'body', 'this is a test'))
    for ch in unallowChars:
        error = None
        try:
            r.execute_command('ft.search', 'idx', '%%wor%sd%%' % ch)
        except Exception as e:
            error = str(e)
        env.assertTrue('Syntax error' in error)

def testFuzzyWithNumbersOnly(env):
    env.expect('ft.create', 'idx', 'schema', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12345').equal('OK')
    env.expect('ft.search', 'idx', '%%21345%%').equal([1, 'doc1', ['test', '12345']])

def testFuzzyWithSeveralFields(env):
    env.cmd('ft.config set UNIONQUICKEXIT')
    env.expect('ft.create', 'idx1', 'schema', 'test1', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx1', 'doc1', '1.0', 'FIELDS', 'test1', 'Admin1 Admin').equal('OK')
    env.expect('ft.search', 'idx1', '%Admin%', 'WITHSCORES', 'HIGHLIGHT').equal([1L, 'doc1', '2', ['test1', '<b>Admin1</b> <b>Admin</b>']])

    env.expect('ft.create', 'idx2', 'schema', 'test1', 'TEXT', 'WEIGHT', '1', 'test2', 'TEXT', 'WEIGHT', '10').equal('OK')
    env.expect('ft.add', 'idx2', 'doc1', '1.0', 'FIELDS', 'test1', 'Admin1', 'test2', 'xyz').equal('OK')
    env.expect('ft.add', 'idx2', 'doc2', '1.0', 'FIELDS', 'test1', 'Admin', 'test2', 'Admin1').equal('OK')
    env.expect('ft.search', 'idx2', '%Admin%', 'WITHSCORES', 'HIGHLIGHT').equal([2L, 
                        'doc2', '1.2', ['test1', '<b>Admin</b>', 'test2', '<b>Admin1</b>'],
                        'doc1', '0.10000000000000001', ['test1', '<b>Admin1</b>', 'test2', 'xyz']])

    env.expect('ft.create', 'idx3', 'schema', 'test1', 'TEXT', 'WEIGHT', '1', 'test2', 'TEXT', 'WEIGHT', '10').equal('OK')
    env.expect('ft.add', 'idx3', 'doc1', '1.0', 'FIELDS', 'test1', 'Admin1', 'test2', 'xyz').equal('OK')
    env.expect('ft.add', 'idx3', 'doc2', '1.0', 'FIELDS', 'test1', 'Admin', 'test2', 'xyz').equal('OK')
    env.expect('ft.add', 'idx3', 'doc3', '1.0', 'FIELDS', 'test1', 'Admin', 'test2', 'Admin1').equal('OK')
    res = env.execute_command('ft.search', 'idx3', '%Admin%', 'WITHSCORES', 'HIGHLIGHT')
    env.assertEqual(res[3][3], '<b>Admin1</b>')
    env.expect('ft.search', 'idx3', '%Admin%', 'WITHSCORES', 'HIGHLIGHT').equal([3L,
                                                'doc3', '1.1000000000000001', ['test1', '<b>Admin</b>', 'test2', '<b>Admin1</b>'],
                                                'doc2', '0.10000000000000001', ['test1', '<b>Admin</b>', 'test2', 'xyz'],
                                                'doc1', '0.10000000000000001', ['test1', '<b>Admin1</b>', 'test2', 'xyz']])

    env.expect('ft.create', 'idx4', 'schema', 'test1', 'TEXT', 'WEIGHT', '100', 'test2', 'TEXT', 'WEIGHT', '10').equal('OK')
    env.expect('ft.add', 'idx4', 'doc1', '1.0', 'FIELDS', 'test1', 'Admin1 admin admid admir admin1 1admin admid', 'test2', 'xyz admid admin1 xyz').equal('OK')
    env.expect('ft.search', 'idx4', '%Admin%', 'WITHSCORES', 'HIGHLIGHT').equal([1L, 'doc1', '1.7142857142857142', 
                ['test1', '<b>Admin1</b> <b>admin</b> <b>admid</b> <b>admir</b> <b>admin1</b> <b>1admin</b> <b>admid</b>',
                 'test2', 'xyz <b>admid</b> <b>admin1</b> xyz']])
