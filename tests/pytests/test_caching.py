from RLTest import Env
from includes import *
from common import *
import json

# we expect loadIndividualKeys to be used
# we expect getKeyCommonHash to be used
# we expect isValueAvailable to return true
# that implies that RSValue_SendReply will convert the double value into an integer
# Otherwise, hvalToValue would have been used and the double value would have been converted into a string
# due to forceString being 1
def testCacheDoubleWillBeReceivedAsInteger(env):
    env.expect('ft.create', 'idx', 'schema', 'name', 'text', 'num', 'numeric', 'sortable').ok()
    env.expect('hset', 'foo', 'name', 'john', 'num', '3.0').equal(2)
    env.expect('ft.search', 'idx', '@name:john', 'return', '1', 'num').equal([1, 'foo', ['num', '3']])
    env.expect('ft.search', 'idx', '@name:john', 'return', '1', 'num').noEqual([1, 'foo', ['num', '3.0']])


# Make sure hgetall is not being used during the search
def testCacheUsingCommandStats(env):
    env.expect('ft.create', 'idx', 'schema', 'name', 'text', 'num', 'numeric', 'sortable').ok()
    env.expect('hset', 'foo', 'name', 'john', 'num', '3.0').equal(2)
    before = env.cmd('INFO', 'COMMANDSTATS')
    env.expect('ft.search', 'idx', '@name:john', 'return', '1', 'num').equal([1, 'foo', ['num', '3']])
    after = env.cmd('INFO', 'COMMANDSTATS')
    for command in ['hgetall', 'hget']:
        env.assertEqual(after.get(f'cmdstat_{command}', {}).get('calls', 0), before.get(f'cmdstat_{command}', {}).get('calls', 0))
