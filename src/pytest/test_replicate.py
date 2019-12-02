import subprocess
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

  time.sleep(5)

  for i in range(10):
    # checking for insertion
    env.assertEqual(['f', 'hello world'], 
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(['f', 'hello world'], 
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))

    # deleting
    env.assertEqual(1, master.execute_command(
          'ft.del', 'idx', 'doc%d' % i, 'DD'))
  
  time.sleep(5)

  for i in range(10):
    # checking for deletion
    env.assertEqual(None, 
      master.execute_command('ft.get', 'idx', 'doc%d' % i))
    env.assertEqual(None, 
      slave.execute_command('ft.get', 'idx', 'doc%d' % i))
            
