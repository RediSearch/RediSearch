import subprocess
import os
import os.path
import sys
from RLTest import Env
from includes import *


if 'EXT_TEST_PATH' in os.environ:
    EXTPATH = os.environ['EXT_TEST_PATH']
else:
    EXTPATH = 'tests/ctests/libexample_extension'
    if sys.platform.lower() == 'darwin':
        EXTPATH += '.dylib'
    else:
        EXTPATH += '.so'


def testExt(env):
    if os.path.isabs(EXTPATH):
        ext_path = EXTPATH
    else:
        modpath = env.module[0]
        ext_path = os.path.abspath(os.path.join(os.path.dirname(modpath), EXTPATH))
    
    if not os.path.exists(ext_path):
        raise Exception("Path ({}) does not exist. "
            "Run from the build directory or set EXT_TEST_PATH in the environment".format(ext_path))

    env = Env(moduleArgs='EXTLOAD %s' % ext_path)

    if env.env == 'existing-env':
        env.skip()

    N = 100
    env.assertOk(env.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text'))
    for i in range(N):
        env.assertOk(env.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                         'f', 'hello world'))
    res = env.execute_command('ft.search', 'idx', 'hello world')
    env.assertEqual(N, res[0])
    res = env.execute_command('ft.search', 'idx', 'hello world', 'scorer', 'filterout_scorer')
    env.assertEqual(0, res[0])

    if not env.isCluster():
        res = env.cmd('ft.config', 'get', 'EXTLOAD')[0][1]
        env.assertContains('libexample_extension', res)
