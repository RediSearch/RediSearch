from RLTest import Env
from includes import *


# mainly this test adding and removing docs while the doc table size is 100
# and make sure we are not crashing and not leaking memory (when runs with valgrind).
def testDocTable():
    env = Env(moduleArgs='MAXDOCTABLESIZE 100')
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    # doc table size is 100 so insearting 1000 docs should gives us 10 docs in each bucket
    for i in range(1000):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                         'title', 'hello world %d' % (i % 100),
                                         'body', 'lorem ist ipsum'))

    for i in range(100):
        res = env.cmd('ft.search', 'idx', 'hello world %d' % i)
        env.assertEqual(res[0], 10)

    # deleting the first 100 docs
    for i in range(100):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        res = env.cmd('ft.search', 'idx', 'hello world %d' % i)
        env.assertEqual(res[0], 9)

    env.expect('ft.drop', 'idx').ok()
