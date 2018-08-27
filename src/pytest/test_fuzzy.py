

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