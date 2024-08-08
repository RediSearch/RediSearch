import signal
from RLTest import Env
import time
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

def checkSlaveSynced(env, slaveConn, command, expected_result, time_out=5, mapping=lambda x: x):
  try:
    with TimeLimit(time_out):
      res = slaveConn.execute_command(*command)
      while mapping(res) != expected_result:
        time.sleep(0.1)
        res = slaveConn.execute_command(*command)
  except TimeoutException:
    env.assertTrue(False, message='Failed waiting for command to be executed on slave')
  except Exception as e:
    env.assertTrue(False, message=e.message)

def initEnv():
  skipTest(cluster=True) # skip on cluster before creating the env
  env = Env(useSlaves=True, forceTcp=True)

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
  def load_master():
    for j in range(100):
      geo = '1.23456,' + str(float(j) / 100)
      master.execute_command('HSET', 'doc%d' % j, 't', 'hello%d' % j, 'tg', 'world%d' % j, 'n', j, 'g', geo)
    env.assertEqual(master.execute_command('WAIT', '1', '10000'), 1) # wait for master and slave to be in sync

  def master_command(*cmd):
    master.execute_command(*cmd)
    env.assertEqual(master.execute_command('WAIT', '1', '10000'), 1) # wait for master and slave to be in sync

  load_master()

  # test for FT.DROPINDEX
  master_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO')
  # No matter how many documents were indexed, we expect that the master and slave will be in sync
  master_command('FT.DROPINDEX', 'idx', 'DD')

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

  # Make sure there are still documents to index and drop
  load_master()

  # test for FT.DROP
  master_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO')
  # No matter how many documents were indexed, we expect that the master and slave will be in sync
  master_command('FT.DROP', 'idx')

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

def testDropTempReplicate():
  env = initEnv()
  master = env.getConnection()
  slave = env.getSlaveConnection()

  '''
  This test creates creates a temporary index. then it creates a document and check it exists on both shards.
  The index is then expires and dropped.
  The test checks consistency between master and slave where both index and document are deleted.
  '''

  # Create a temporary index, with a long TTL
  master.execute_command('FT.CREATE', 'idx', 'TEMPORARY', '3600', 'SCHEMA', 't', 'TEXT')
  # Pause the index expiration, so we can control when it expires
  env.expect(debug_cmd(), 'TTL_PAUSE', 'idx').ok()

  master.execute_command('HSET', 'doc1', 't', 'hello')

  checkSlaveSynced(env, slave, ('hgetall', 'doc1'), {'t': 'hello'})

  # check that same index and doc exist on master and slave
  master_index = master.execute_command('FT._LIST')
  slave_index = slave.execute_command('FT._LIST')
  env.assertEqual(master_index, slave_index)

  master_keys = master.execute_command('KEYS', '*')
  slave_keys = slave.execute_command('KEYS', '*')
  env.assertEqual(len(master_keys), len(slave_keys))
  env.assertEqual(master_keys, slave_keys)

  # Make the index expire soon
  env.expect(debug_cmd(), 'TTL_EXPIRE', 'idx').ok()
  # Verify that the slave index was dropped as well along with the document
  checkSlaveSynced(env, slave, ('hgetall', 'doc1'), {})

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
    expireDocs(False,  # Without SORTABLE -
              # Without sortby -
              # Documents are sorted according to dicId
              # both docs exist but we failed to load doc1 since it was found to be expired during the query
               [2, 'doc2', ['t', 'foo'], 'doc1', None],
              # With sortby -
              # Loading the value of the expired document failed, so it gets lower priority.
               [2, 'doc2', ['t', 'foo'], 'doc1', None])

def testExpireDocsSortable():
    '''
    Same as test `testExpireDocs` only with SORTABLE
    '''
    expireDocs(True,  # With SORTABLE -
               # Since the field is SORTABLE, the field's value is available to the sorter, and
               # the documents are ordered according to the sortkey values.
               # However, the loader fails to load doc1 and the result is marked as expired so
               # the value does not appear in the result.
             [2, 'doc2', ['t', 'foo'], 'doc1', None],  # Without sortby - ordered by docid, notice doc1 was expired so the notification pushed it to the back of the line
               [2, 'doc1', None, 'doc2', ['t', 'foo']])  # With sortby - ordered by the original value, bar > foo

def expireDocs(isSortable, iter1_expected_without_sortby, iter1_expected_with_sortby):
    '''
    This test creates an index and two documents and check they exist on both shards.
    One of the documents is found to be expired during a query.
    The test checks the dwe get the same results for this case both in the master and the slave.

    When isSortable is True the index is created with `SORTABLE` arg
    '''

    env = initEnv()
    master = env.getConnection()
    slave = env.getSlaveConnection()

    for i in range(2):
        # Use "lazy" expire (expire only when key is accessed)
        master.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
        slave.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

        sortby_cmd = [] if i == 0 else ['SORTBY', 't']
        sortable_arg = [] if not isSortable else ['SORTABLE']
        master.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', *sortable_arg)
        master.execute_command('FT.DEBUG', 'MONITOR_EXPIRATION', 'idx', 'documents', '0')
        slave.execute_command('FT.DEBUG', 'MONITOR_EXPIRATION', 'idx', 'documents', '0')
        master.execute_command('HSET', 'doc1', 't', 'bar')
        master.execute_command('HSET', 'doc2', 't', 'foo')

        # Both docs exist.
        # Enforce propagation to slave
        # (WAIT is propagating WRITE commands but FT.CREATE is not a WRITE command)
        res = master.execute_command('WAIT', '1', '10000')
        env.assertEqual(res, 1)

        res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']])

        res = slave.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']])

        master.execute_command('PEXPIRE', 'doc1', 1)
        # ensure expiration before search
        time.sleep(0.05)

        msg = '{}{} sortby'.format(
            'SORTABLE ' if isSortable else '', 'without' if i == 0 else 'with')
        # First iteration
        expected_res = iter1_expected_without_sortby if i == 0 else iter1_expected_with_sortby
        # Opening the key should fail on both slave and master should and the result should be marked with
        # a null value.
        res = slave.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, expected_res, message=(msg + " slave"))
        res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, expected_res, message=(msg + " master"))

        # Cancel lazy expire to allow the deletion of the key
        master.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        slave.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        # enforce sync.
        res = master.execute_command('WAIT', '1', '10000')
        env.assertEqual(res, 1)

        # Second iteration - only 1 doc is left (master deleted it)
        res = master.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, [1, 'doc2', ['t', 'foo']],
                        message=(msg + " master"))
        res = slave.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, [1, 'doc2', ['t', 'foo']],
                        message=(msg + " slave"))

        master.execute_command('FLUSHALL')
        res = master.execute_command('WAIT', '1', '10000')
        env.assertEqual(res, 1)
