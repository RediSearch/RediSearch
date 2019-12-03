import subprocess
import signal
import os
import os.path
from RLTest import Env
import time

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
        raise Exception()   

def testDelReplicate():
  env = Env(useSlaves=True)
  master = env.getConnection()
  slave = env.getSlaveConnection() 

  env.assertContains("PONG", master.execute_command("ping"))
  env.assertContains("PONG", slave.execute_command("ping"))  
  env.assertOk(master.execute_command('ft.create', 'idx', 'schema', 'f', 'text'))

  for i in range(10):
    master.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                      'f', 'hello world')
  # Ensure slave is updated
  master.execute_command('set foo bar')
  try:
    with TimeLimit(5):
      res = slave.execute_command('get foo')
      while res == None:
        res = slave.execute_command('get foo')
  except Exception:
        env.assertTrue(False, message='Failed waiting for registration to unregister on slave')

  for i in range(10):
    # checking for insertion
    env.assertEqual(['f', 'hello world'], 
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(['f', 'hello world'], 
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))

    # deleting
    env.assertEqual(1, master.execute_command(
          'ft.del', 'idx', 'doc%d' % i, 'DD'))
  
  # Ensure slave is updated
  master.execute_command('set foo baz')
  try:
    with TimeLimit(5):
      res = slave.execute_command('get foo')
      while len(res) == 'bar':
        res = slave.execute_command('get foo')
  except Exception:
        env.assertTrue(False, message='Failed waiting for registration to unregister on slave')

  for i in range(10):
    # checking for deletion
    env.assertEqual(None, 
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(None, 
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))