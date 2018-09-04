import subprocess
import os
import os.path
from RLTest import Env


def testExt():
    extentionPath = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/../tests/ext-example/example.so')

    if not os.path.exists(extentionPath):
        print subprocess.call(['make', '-C', os.path.dirname(extentionPath)])

    env = Env(moduleArgs='EXTLOAD %s' % extentionPath)

    N = 100
    env.assertOk(env.execute_command(
        'ft.create', 'idx', 'schema', 'f', 'text'))
    for i in range(N):
        env.assertOk(env.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                         'f', 'hello world'))
    res = env.execute_command('ft.search', 'idx', 'hello world')
    env.assertEqual(N, res[0])
    res = env.execute_command('ft.search', 'idx', 'hello world', 'scorer', 'filterout_scorer')
    env.assertEqual(0, res[0])
