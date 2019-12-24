import subprocess
import signal
import os
import os.path
from RLTest import Env
import time
import random

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

  master = env.getConnection()
  slave = env.getSlaveConnection() 
  env.assertContains("PONG", master.execute_command("ping"))
  env.assertContains("PONG", slave.execute_command("ping"))  
  env.assertOk(master.execute_command('ft.create', 'idx', 'schema', 'f', 'text'))

  checkSlaveSynced(env, slave, ('exists', 'idx:idx'), 1, time_out=20)

  for i in range(10):
    master.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                      'f', 'hello world')

  checkSlaveSynced(env, slave, ('ft.get', 'idx', 'doc9'), ['f', 'hello world'], time_out=20)

  for i in range(10):
    # checking for insertion
    env.assertEqual(['f', 'hello world'], 
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(['f', 'hello world'], 
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))

    # deleting
    env.assertEqual(1, master.execute_command(
          'ft.del', 'idx', 'doc%d' % i, 'DD'))
  
  checkSlaveSynced(env, slave, ('ft.get', 'idx', 'doc9'), None, time_out=20)

  for i in range(10):
    # checking for deletion
    env.assertEqual(None, 
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(None, 
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))