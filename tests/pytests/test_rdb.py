import os
import pytest
import multiprocessing
import threading
import time
import signal
import tempfile
from includes import *
from common import *
from RLTest import Env

@skip(cluster=True)
@pytest.mark.timeout(120)
def test_rdb_load_no_deadlock():
    """
    Test that loading from RDB while constantly sending INFO commands doesn't cause deadlock.
    This test starts a clean Redis server, then triggers RDB loading from the client side
    while some subprocesses keep sending INFO commands.
    """
    # Use the existing dataset RDB file
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    source_rdb_path = os.path.join(root_dir, 'datasets', 'arxiv-titles-384-angular-filters-m16-ef-128-dump.rdb')
    # Create a clean Redis environment
    test_env = Env(moduleArgs='')

    # Start the server first
    test_env.start()

    # Verify server is running
    test_env.expect('PING').equal(True)

    # Configure indexer to yield more frequently during loading to increase chance of deadlock
    test_env.cmd('CONFIG', 'SET', 'search-indexer-yield-every-ops', '10')

    # Copy the RDB file to a location where Redis can access it
    # Get Redis working directory
    redis_dir = test_env.cmd('CONFIG', 'GET', 'dir')[1]
    target_rdb_path = os.path.join(redis_dir, 'load_test.rdb')

    # Copy the RDB file
    import shutil
    shutil.copy2(source_rdb_path, target_rdb_path)

    def info_command_process(port):
        """Process that continuously sends INFO commands"""
        import redis

        # Create a new connection in this process
        conn = redis.Redis(host='localhost', port=port, decode_responses=True)

        while True:
            try:
                result = conn.execute_command('INFO', 'everything')
            except Exception as e:
                continue

    # Start the INFO command thread
    redis_port = test_env.getConnection().connection_pool.connection_kwargs['port']
    info_processes = []

    for i in range(20):
        process = multiprocessing.Process(
            target=info_command_process,
            args=(redis_port,),
            daemon=True
        )
        process.start()
        info_processes.append(process)
    # try:
    # Verify current config before changing
    current_dir = test_env.cmd('CONFIG', 'GET', 'dir')[1]
    current_dbfilename = test_env.cmd('CONFIG', 'GET', 'dbfilename')[1]

    # Use DEBUG RELOAD to load the RDB file
    # First, set both the directory and dbfilename to our copied file
    test_env.cmd('CONFIG', 'SET', 'dir', redis_dir)
    test_env.cmd('CONFIG', 'SET', 'dbfilename', 'load_test.rdb')

    # Verify config was set
    new_dir = test_env.cmd('CONFIG', 'GET', 'dir')[1]
    new_dbfilename = test_env.cmd('CONFIG', 'GET', 'dbfilename')[1]
    test_env.cmd('CONFIG', 'SET', 'busy-reply-threshold', 3)

    # Check if the RDB file exists at the expected location
    expected_rdb_path = os.path.join(new_dir, new_dbfilename)

    # Get current database size before reload
    dbsize_before = test_env.cmd('DBSIZE')

    # Trigger the reload - use NOSAVE to prevent overwriting our RDB file
    test_env.cmd('DEBUG', 'RELOAD', 'NOSAVE')
    for process in info_processes:
        process.terminate()
        process.join()

    test_env.expect('PING').equal(True)

    # Check database size to see if anything was loaded
    dbsize = test_env.cmd('DBSIZE')

    # Try to get info about any existing indices
    try:
        indices_info = test_env.cmd('FT._LIST')
        print(f"DEBUG: Found indices: {indices_info}")
        if indices_info:
            # If there are indices, verify we can get info about the first one
            test_env.expect('FT.INFO', indices_info[0]).noError()
            print(f"SUCCESS: Index {indices_info[0]} is accessible")
    except Exception as e:
        print(f"DEBUG: No indices found or error accessing them: {e}")

    print("DEBUG: Test completed. INFO commands were sent by subprocess during RDB loading.")
