import subprocess
import os
import os.path
import sys
from RLTest import Env
from includes import *
from test_info_modules import info_modules_to_dict
from common import config_cmd


if 'EXT_TEST_PATH' in os.environ:
    EXTPATH = os.environ['EXT_TEST_PATH']
else:
    EXTPATH = 'tests/ctests/ext-example/libexample_extension.so'


def testExt(env):
    if env.env == 'existing-env' or NO_LIBEXT:
        env.skip()

    if os.path.isabs(EXTPATH):
        ext_path = EXTPATH
    else:
        modpath = env.module[0]
        ext_path = os.path.abspath(os.path.join(os.path.dirname(modpath), EXTPATH))

    if not os.path.exists(ext_path):
        raise Exception("Path ({}) does not exist. "
            "Run from the build directory or set EXT_TEST_PATH in the environment".format(ext_path))

    env = Env(moduleArgs='EXTLOAD %s' % ext_path)

    N = 100
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text').ok()
    for i in range(N):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                         'f', 'hello world'))
    res = env.cmd('ft.search', 'idx', 'hello world')
    env.assertEqual(N, res[0])
    res = env.cmd('ft.search', 'idx', 'hello world', 'scorer', 'filterout_scorer')
    env.assertEqual(0, res[0])

    info = info_modules_to_dict(env)
    env.assertTrue('search_extension_load' in info['search_runtime_configurations'])

    if not env.isCluster():
        res = env.cmd(config_cmd(), 'get', 'EXTLOAD')[0][1]
        env.assertContains('libexample_extension', res)
