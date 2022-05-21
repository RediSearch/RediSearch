import platform
from includes import *


def testWideSchema(env):
    r = env
    schema = []
    FIELDS = 128 if platform.architecture()[0] == '64bit' else 64
    for i in range(FIELDS):
        schema.extend(('field_%d' % i, 'TEXT'))
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', *schema))
    N = 10
    for n in range(N):
        fields = []
        for i in range(FIELDS):
            fields.extend(('field_%d' % i, 'hello token_%d' % i))
        env.assertOk(r.execute_command('ft.add', 'idx',
                                       'doc%d' % n, 1.0, 'fields', *fields))
    for _ in env.reloading_iterator():
        for i in range(FIELDS):

            res = env.cmd('ft.search', 'idx', '@field_%d:token_%d' % (i, i), 'NOCONTENT')
            env.assertEqual(res[0], N)

            res = r.execute_command(
                'ft.explain', 'idx', '@field_%d:token_%d' % (i, i), 'VERBATIM').strip()
            env.assertEqual('@field_%d:token_%d' % (i, i), res)

            res = env.cmd('ft.search', 'idx', 'hello @field_%d:token_%d' % (i, i), 'NOCONTENT')
            env.assertEqual(res[0], N)

        res = env.cmd('ft.search', 'idx', ' '.join(
            ('@field_%d:token_%d' % (i, i) for i in range(FIELDS))))
        env.assertEqual(res[0], N)

        res = env.cmd('ft.search', 'idx', ' '.join(
            ('token_%d' % (i) for i in range(FIELDS))))
        env.assertEqual(res[0], N)
    
    if not env.isCluster():
        # todo: make it less specific to pass on cluster
        res = env.cmd('ft.info', 'idx')
        env.assertEqual(res[3][0], 'MAXTEXTFIELDS')