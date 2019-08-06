import subprocess
import os
import os.path
import sys
from RLTest import Env

if 'EXT_TEST_PATH' in os.environ:
    EXTPATH = os.environ['EXT_TEST_PATH']
else:
    EXTPATH = 'src/tests/libexample_extension'
    if sys.platform.lower() == 'darwin':
        EXTPATH += '.dylib'
    else:
        EXTPATH += '.so'

EXTPATH = os.path.abspath(EXTPATH)

# Last ditch effort:
SRCFILE = os.path.dirname(__file__) + '/../tests/ext-example/example.c'
INCDIR = os.path.dirname(__file__) + '/../'

if not os.path.exists(EXTPATH):
    EXTPATH = os.path.abspath('libexample_extension.' + ('dylib' if sys.platform.lower() == 'darwin' else 'so'))
    args = ['cc', '-shared', '-fPIC', '-o', EXTPATH, SRCFILE, '-I' + INCDIR]
    print args
    print subprocess.call(args)

def testExt():
    if not os.path.exists(EXTPATH):
        raise Exception("Path ({}) does not exist. "
            "Run from the build directory or set EXT_TEST_PATH in the environment".format(EXTPATH))

    # extentionPath = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/../tests/ext-example/example.so')
    env = Env(moduleArgs='EXTLOAD %s' % EXTPATH)

    if env.env == 'existing-env':
        env.skip()

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
