# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList, check_server_version
from time import sleep
from RLTest import Env

def testSanity(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('ft.create', 'idx', 'ON', 'JSON', 'SCHEMA', 't', 'text')
