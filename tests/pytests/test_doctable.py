from RLTest import Env


# mainly this test adding and removing docs while the doc table size is 100
# and make sure we are not crashing and not leaking memory (when runs with valgrind).
def testDocTable():
    env = Env(moduleArgs='MAXDOCTABLESIZE 100')
    env.assertOk(env.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    # doc table size is 100 so insearting 1000 docs should gives us 10 docs in each bucket
    for i in range(1000):
        env.assertOk(env.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                         'title', 'hello world %d' % (i % 100),
                                         'body', 'lorem ist ipsum'))

    for i in range(100):
        res = env.execute_command('ft.search', 'idx', 'hello world %d' % i)
        env.assertEqual(res[0], 10)

    # deleting the first 100 docs
    for i in range(100):
        env.assertEqual(env.execute_command('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        res = env.execute_command('ft.search', 'idx', 'hello world %d' % i)
        env.assertEqual(res[0], 9)

    env.assertOk(env.execute_command('ft.drop', 'idx'))
