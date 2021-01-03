from includes import *


def testBasicFuzzy(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'hello world',
                                    'body', 'this is a test'))

    res = r.execute_command('ft.search', 'idx', '%word%')
    env.assertEqual(res[0:2], [1L, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

def testLdLimit(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'title', 'hello world')
    env.assertEqual([1L, 'doc1', ['title', 'hello world']], env.cmd('ft.search', 'idx', '%word%'))  # should be ok
    env.assertEqual([0L], env.cmd('ft.search', 'idx', r'%sword%'))  # should return nothing
    env.assertEqual([1L, 'doc1', ['title', 'hello world']], env.cmd('ft.search', 'idx', r'%%sword%%'))

def testStopwords(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 't1', 'text')
    for t in ('iwth', 'ta', 'foo', 'rof', 'whhch', 'witha'):
        env.cmd('ft.add', 'idx', t, 1.0, 'fields', 't1', t)

    r = env.cmd('ft.search', 'idx', '%for%')
    env.assertEqual([1, 'foo', ['t1', 'foo']], r)

    r = env.cmd('ft.search', 'idx', '%%with%%')
    env.assertEqual([2, 'iwth', ['t1', 'iwth'], 'witha', ['t1', 'witha']], r)

    r = env.cmd('ft.search', 'idx', '%with%')
    env.assertEqual([1, 'witha', ['t1', 'witha']], r)

    r = env.cmd('ft.search', 'idx', '%at%')
    env.assertEqual([1, 'ta', ['t1', 'ta']], r)

def testFuzzyMultipleResults(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text'))
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
    env.assertEqual(res[0], 3L)
    for i in range(1,6,2):
        env.assertIn(res[i], ['doc1', 'doc2', 'doc3'])

def testFuzzySyntaxError(env):
    r = env
    unallowChars = ('*', '$', '~', '&', '@', '!')
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text'))
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
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12345').equal('OK')
    env.expect('ft.search', 'idx', '%%21345%%').equal([1, 'doc1', ['test', '12345']])