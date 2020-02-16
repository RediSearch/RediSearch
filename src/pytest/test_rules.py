from RLTest import Env
import pprint
import time
from includes import *

def testCreateRules(env):
    env.cmd('ft.create', 'idx', 'WITHRULES', 'SCHEMA', 'f1', 'text')  # OK
    env.cmd('ft.ruleadd', 'idx', 'rule1', 'PREFIX', 'user:', 'INDEX')  # OK
    env.cmd('ft.ruleadd', 'idx', 'rule2', 'EXPR', '@year>2015', 'INDEX')  # OK
    # 
    env.cmd('hset', 'user:mnunberg', 'foo', 'bar')
    env.cmd('hset', 'user:mnunberg', 'f1', 'hello world')

    time.sleep(1)
    print env.cmd('ft.search', 'idx', 'hello')
    print("Deleting...")
    env.cmd('del', 'user:mnunberg')
    print env.cmd('ft.search', 'idx', 'hello')

    env.cmd('hset', 'someDoc', 'year', '2019')
    env.cmd('hset', 'someDoc', 'name', 'mark')
    env.cmd('hset', 'someDoc', 'f1', 'goodbye')
    
    time.sleep(1)
    print env.cmd('ft.search', 'idx', 'goodbye')