import subprocess
import signal
import os
import os.path
from RLTest import Env
import time

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
  end = time.time() + 5
  found = False
  while time.time() < end:
    if slave.execute_command('exists foo'):
      found = True
      break
    time.sleep(0.01)

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
  master.execute_command('del foo')
  end = time.time() + 5
  while time.time() < end:
    if not slave.execute_command('exists foo'):
      found = False
      break
    time.sleep(0.01)

  for i in range(10):
    # checking for deletion
    env.assertEqual(None, 
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(None, 
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))