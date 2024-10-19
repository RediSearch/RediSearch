# -*- coding: utf-8 -*-

import json
import bz2
import numpy as np

from common import *
from includes import *
from RLTest import Env

@skip(cluster=True, no_json=True, asan=True)
def test_aux_save2(env: Env):
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    # Save state to RDB
    env.stop()
    # Restart without modules. 
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')
    # Attempt to load RDB should work because the RDB
    # does not contains module aux data
    env.start()
    env.expect('HGET', 'doc1', 't').equal('hello')

