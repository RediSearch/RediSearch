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
    connection = env.getClusterConnectionIfNeeded()
    connection.execute_command('hset', 'foo', 'name', 'john', 'num', '3.0')
    env.expect('ft.search', 'idx', '@name:john', 'return', '1', 'num').equal([1, 'foo', ['num', '3']])
    env.expect('ft.search', 'idx', '@name:john', 'return', '1', 'num').noEqual([1, 'foo', ['num', '3.0']])

# TODO
# Add a test that checks the cache in a more durable way
# Maybe by adding a cache hit/miss stat to FT.INFO will help write a test that checks the cache(sorting vector)
