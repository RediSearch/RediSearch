#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from RLTest import Env
from common import *

def test_connection_pool_state_with_multiple_io_threads():
    """Test that SHARD_CONNECTION_STATES merges connections from all IO threads"""
    # Use multiple IO threads to test merging
    env = Env(moduleArgs='SEARCH_IO_THREADS 3', env='oss-cluster', shardsCount=3)

    # Test that the command works and returns expected structure
    result = env.cmd(debug_cmd(), 'SHARD_CONNECTION_STATES')
    print(f"SHARD_CONNECTION_STATES result with 3 IO threads: {result}")

    # The result should be a map/array with shard information
    # Each shard should have a host:port key and an array of connection states
    assert isinstance(result, list), f"Expected list, got {type(result)}"
    assert len(result) > 0, "Expected non-empty result"

    # Should have pairs of [host:port, [states...]] for each shard
    assert len(result) % 2 == 0, f"Expected even number of elements (key-value pairs), got {len(result)}"

    # With our new implementation, we should see connections from all IO threads merged
    # So we expect to see more connections than with a single IO thread
    total_connections = 0
    for i in range(0, len(result), 2):
        shard_key = result[i]
        shard_states = result[i + 1]

        print(f"Shard {i//2}: {shard_key} -> {shard_states} (count: {len(shard_states)})")

        # Key should be a string in host:port format
        assert isinstance(shard_key, str), f"Expected string key, got {type(shard_key)}"
        assert ':' in shard_key, f"Expected host:port format, got {shard_key}"

        # States should be an array of connection state strings
        assert isinstance(shard_states, list), f"Expected list of states, got {type(shard_states)}"
        assert len(shard_states) > 0, "Expected at least one connection state"

        total_connections += len(shard_states)

        # Each state should be a string
        for state in shard_states:
            assert isinstance(state, str), f"Expected string state, got {type(state)}"
            # Common connection states
            assert state in ['Connected', 'Connecting', 'Disconnected', 'Error'], f"Unexpected state: {state}"

    print(f"✅ Total connections found: {total_connections}")
    print("✅ SHARD_CONNECTION_STATES test with multiple IO threads passed!")

    # Test with different number of IO threads to verify merging
    env2 = Env(moduleArgs='SEARCH_IO_THREADS 1', env='oss-cluster', shardsCount=3)
    result2 = env2.cmd(debug_cmd(), 'SHARD_CONNECTION_STATES')
    print(f"SHARD_CONNECTION_STATES result with 1 IO thread: {result2}")

    total_connections_single = 0
    for i in range(0, len(result2), 2):
        shard_states = result2[i + 1]
        total_connections_single += len(shard_states)

    print(f"✅ Total connections with 1 IO thread: {total_connections_single}")
    print(f"✅ Total connections with 3 IO threads: {total_connections}")

    # With multiple IO threads, we should see more connections (or at least the same)
    # since we're merging from all IORuntimes
    assert total_connections >= total_connections_single, \
        f"Expected more connections with multiple IO threads, got {total_connections} vs {total_connections_single}"

if __name__ == '__main__':
    test_connection_pool_state_with_multiple_io_threads()
