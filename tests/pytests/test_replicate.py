import subprocess
import signal
import os
import os.path
from RLTest import Env
import time
import random
from includes import *
from common import waitForIndex


class TimeoutException(Exception):
  pass

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """
    def __init__(self, timeout):
        self.timeout = timeout
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)
    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
    def handler(self, signum, frame):
        raise TimeoutException()

def checkSlaveSynced(env, slaveConn, command, expected_result, time_out=5):
  try:
    with TimeLimit(time_out):
      res = slaveConn.execute_command(*command)
      while res != expected_result:
        res = slaveConn.execute_command(*command)
  except TimeoutException:
    env.assertTrue(False, message='Failed waiting for command to be executed on slave')
  except Exception as e:
    env.assertTrue(False, message=e.message)

def testDelReplicate():
  env = Env(useSlaves=True, forceTcp=True)

  env.skipOnCluster()

  ## on existing env we can not get a slave connection
  ## so we can no test it
  if env.env == 'existing-env':
        env.skip()

  master = env.getConnection()
  slave = env.getSlaveConnection()
  env.assertTrue(master.execute_command("ping"))
  env.assertTrue(slave.execute_command("ping"))
  env.assertOk(master.execute_command('ft.create', 'idx', 'ON', 'HASH', 'FILTER', 'startswith(@__key, "")', 'schema', 'f', 'text'))
  env.cmd('set', 'indicator', '1')
  checkSlaveSynced(env, slave, ('exists', 'indicator'), 1, time_out=20)

  for i in range(10):
    master.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                      'f', 'hello world')
  time.sleep(0.01)
  checkSlaveSynced(env, slave, ('ft.get', 'idx', 'doc9'), ['f', 'hello world'], time_out=20)

  for i in range(10):
    # checking for insertion
    env.assertEqual(['f', 'hello world'],
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(['f', 'hello world'],
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))

    # deleting
    env.assertEqual(1, master.execute_command(
          'ft.del', 'idx', 'doc%d' % i))

  checkSlaveSynced(env, slave, ('ft.get', 'idx', 'doc9'), None, time_out=20)

  for i in range(10):
    # checking for deletion
    env.assertEqual(None,
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(None,
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))

def testDropReplicate():
  env = Env(useSlaves=True, forceTcp=True)

  env.skipOnCluster()

  ## on existing env we can not get a slave connection
  ## so we can no test it
  if env.env == 'existing-env':
        env.skip()

  master = env.getConnection()
  slave = env.getSlaveConnection()
  env.assertTrue(master.execute_command("ping"))
  env.assertTrue(slave.execute_command("ping"))

  '''
  This test first creates documents
  Next, it creates an index so all documents are scanned into the index
  At last the index is dropped right away, before all documents have been indexed.

  The text checks consistency between master and slave.
  '''
  for j in range(100):
    geo = '1.23456,' + str(float(j) / 100)
    master.execute_command('HSET', 'doc%d' % j, 't', 'hello%d' % j, 'tg', 'world%d' % j, 'n', j, 'g', geo)

  # test for FT.DROPINDEX
  master.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO')
  time.sleep(0.0005)
  master.execute_command('FT.DROPINDEX', 'idx', 'DD')

  # check that same docs were deleted by master and slave
  time.sleep(0.1)
  master_keys = sorted(master.execute_command('KEYS', '*'))
  slave_keys = sorted(slave.execute_command('KEYS', '*'))
  env.assertEqual(len(master_keys), len(slave_keys))
  env.assertEqual(master_keys, slave_keys)

  # show the different documents mostly for test debug info
  master_set = set(master_keys)
  slave_set = set(slave_keys)
  env.assertEqual(master_set.difference(slave_set), set([]))
  env.assertEqual(slave_set.difference(master_set), set([]))

  # test for FT.DROP
  master.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO')
  time.sleep(0.001)
  master.execute_command('FT.DROP', 'idx')

  # check that same docs were deleted by master and slave
  time.sleep(0.01)
  master_keys = sorted(master.execute_command('KEYS', '*'))
  slave_keys = sorted(slave.execute_command('KEYS', '*'))
  env.assertEqual(len(master_keys), len(slave_keys))
  env.assertEqual(master_keys, slave_keys)

  # show the different documents mostly for test debug info
  master_set = set(master_keys)
  slave_set = set(slave_keys)
  env.assertEqual(master_set.difference(slave_set), set([]))
  env.assertEqual(slave_set.difference(master_set), set([]))
