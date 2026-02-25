"""
Step definitions for RediSearchDisk compaction (GC) tests.
"""
from common import *


@when(parsers.parse('I run GC on index "{index_name}"'))
def run_gc_on_index(redis_env, index_name):
    """Run garbage collection (compaction) on the specified index."""
    result = redis_env.cmd('_FT.DEBUG', 'GC_FORCEINVOKE', index_name)
    redis_env.assertEqual(result, 'DONE')
