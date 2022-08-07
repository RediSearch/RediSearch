import subprocess
import signal
import os
import os.path
from RLTest import Env
import time
import random
from includes import *
from common import *


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

def initEnv():
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

  env.expect('WAIT', '1', '10000').equal(1) # wait for master and slave to be in sync

  return env

def testDelReplicate():
  env = initEnv()
  master = env.getConnection()
  slave = env.getSlaveConnection()

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
  env = initEnv()
  master = env.getConnection()
  slave = env.getSlaveConnection()

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
  env.expect('WAIT', '1', '10000').equal(1) # wait for master and slave to be in sync
  master.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO')
  master.execute_command('FT.DROPINDEX', 'idx', 'DD')
  env.expect('WAIT', '1', '10000').equal(1) # wait for master and slave to be in sync

  # check that same docs were deleted by master and slave
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

def testDropTempReplicate():
  env = initEnv()
  master = env.getConnection()
  slave = env.getSlaveConnection()

  '''
  This test creates creates a temporary index. then it creates a document and check it exists on both shards.
  The index is then expires and dropped.
  The test checks consistency between master and slave where both index and document are deleted.
  '''

  # test for TEMPORARY FT.DROPINDEX
  master.execute_command('FT.CREATE', 'idx', 'TEMPORARY', '1', 'SCHEMA', 't', 'TEXT')

  master.execute_command('HSET', 'doc1', 't', 'hello')

  checkSlaveSynced(env, slave, ('hgetall', 'doc1'), {'t': 'hello'}, time_out=5)

  # check that same index and doc exist on master and slave
  master_index = master.execute_command('FT._LIST')
  slave_index = slave.execute_command('FT._LIST')
  env.assertEqual(master_index, slave_index)

  master_keys = master.execute_command('KEYS', '*')
  slave_keys = slave.execute_command('KEYS', '*')
  env.assertEqual(len(master_keys), len(slave_keys))
  env.assertEqual(master_keys, slave_keys)

  time.sleep(1)
  checkSlaveSynced(env, slave, ('hgetall', 'doc1'), {}, time_out=5)

  # check that index and doc were deleted by master and slave
  env.assertEqual(master.execute_command('FT._LIST'), [])
  env.assertEqual(slave.execute_command('FT._LIST'), [])

  env.assertEqual(master.execute_command('KEYS', '*'), [])
  env.assertEqual(slave.execute_command('KEYS', '*'), [])

def testDropWith__FORCEKEEPDOCS():
  env = initEnv()
  master = env.getConnection()
  slave = env.getSlaveConnection()

  '''
  This test creates creates an index. then it creates a document and check it
  exists on both shards.
  The index is then dropped.
  The test checks consistency between master and slave where the index is
  deleted and the document remains.
  '''

  cmd = ['FT.DROP', 'FT.DROPINDEX']
  for i in range(len(cmd)):
    master.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    master.execute_command('HSET', 'doc1', 't', 'hello')
    checkSlaveSynced(env, slave, ('hgetall', 'doc1'), {'t': 'hello'}, time_out=5)

    master.execute_command(cmd[i], 'idx', '_FORCEKEEPDOCS')
    checkSlaveSynced(env, slave, ['FT._LIST'], [], time_out=5)

    # check that index and doc were deleted by master and slave
    env.assertEqual(master.execute_command('FT._LIST'), [])
    env.assertEqual(slave.execute_command('FT._LIST'), [])

    env.assertEqual(master.execute_command('KEYS', '*'), ['doc1'])
    env.assertEqual(slave.execute_command('KEYS', '*'), ['doc1'])


def testExpireDocs():
  env = initEnv()
  master = env.getConnection()
  master.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
  slave = env.getSlaveConnection()
  slave.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

  '''
  This test creates creates an index and two documents and check they
  exist on both shards.
  One of the documents is expired.
  The test checks the document is removed from both master and slave.
  '''

  for i in range(2):
    sortby_cmd = [] if i == 0 else ['SORTBY', 't']
    master.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    master.execute_command('HSET', 'doc1', 't', 'bar')
    master.execute_command('HSET', 'doc2', 't', 'foo')
    
    # both docs exist
    res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']])
    checkSlaveSynced(env, slave, ('FT.SEARCH', 'idx', '*', 'NOCONTENT'), [2, 'doc1', 'doc2'], time_out=5)

    master.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.01)

    # since there is no sortable, we loaded doc1 at sortby and found out it was deleted
    res = slave.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [1, 'doc2', ['t', 'foo']])
    res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [1, 'doc2', ['t', 'foo']])

    master.execute_command('FLUSHALL')
    checkSlaveSynced(env, slave, ('KEYS','*'), [], time_out=5)


def testExpireDocsSortable():
  env = initEnv()
  master = env.getConnection()
  master.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
  slave = env.getSlaveConnection()
  slave.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

  '''
  This test creates creates an index and two documents and check they
  exist on both shards.
  One of the documents is expired.
  The test checks the document is removed from both master and slave.
  The first iteration, the doc was deleted on redis but not on RediSearch and data is `None`. 
  '''

  for i in range(2):
    sortby_cmd = [] if i == 0 else ['SORTBY', 't']
    master.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')
    master.execute_command('HSET', 'doc1', 't', 'bar')
    master.execute_command('HSET', 'doc2', 't', 'foo')
    
    # both docs exist
    res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']])
    checkSlaveSynced(env, slave, ('FT.SEARCH', 'idx', '*', 'NOCONTENT'), [2, 'doc1', 'doc2'], time_out=5)

    master.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.01)
    # both docs exist but doc1 fail to load field since they were expired passively
    res = slave.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [2, 'doc1', None, 'doc2', ['t', 'foo']])
    res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [2, 'doc1', None, 'doc2', ['t', 'foo']])

    # only 1 doc is left
    res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [1, 'doc2', ['t', 'foo']])
    res = slave.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
    env.assertEqual(res, [1, 'doc2', ['t', 'foo']])

    master.execute_command('FLUSHALL')
    checkSlaveSynced(env, slave, ('KEYS','*'), [], time_out=5)
