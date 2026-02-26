"""
Step definitions for RediSearchDisk compaction (GC) tests.
"""
from common import *


@when(parsers.parse('I run GC on index "{index_name}"'))
def run_gc_on_index(redis_env, index_name):
    """Run garbage collection (compaction) on the specified index."""
    result = redis_env.cmd('_FT.DEBUG', 'GC_FORCEINVOKE', index_name)
    redis_env.assertEqual(result, 'DONE')


@when(parsers.parse('I disable automatic GC on index "{index_name}"'))
@given(parsers.parse('I disable automatic GC on index "{index_name}"'))
def disable_automatic_gc(redis_env, index_name):
    """Disable automatic GC scheduling on the specified index."""
    result = redis_env.cmd('_FT.DEBUG', 'GC_STOP_SCHEDULE', index_name)
    redis_env.assertEqual(result, 'OK')
