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


@pytest.mark.timeout(120)
def test_rdb_load_no_deadlock():
    """
    Test that loading from RDB while constantly sending INFO commands doesn't cause deadlock.
    This test starts a clean Redis server, then triggers RDB loading from the client side
    while a background thread continuously sends INFO commands every 100ms.
    """
    # Use the existing dataset RDB file
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    source_rdb_path = os.path.join(root_dir, 'datasets', 'arxiv-titles-384-angular-filters-m16-ef-128-dump.rdb')

    print(f"DEBUG: Using RDB file: {source_rdb_path}")
    print(f"DEBUG: RDB file exists: {os.path.exists(source_rdb_path)}")
    if os.path.exists(source_rdb_path):
        print(f"DEBUG: RDB file size: {os.path.getsize(source_rdb_path)} bytes")

    # Create a clean Redis environment
    test_env = Env(moduleArgs='')

    # Start the server first
    test_env.start()

    # Verify server is running
    test_env.expect('PING').equal(True)
    print("DEBUG: Redis server started successfully")

    # Configure indexer to yield more frequently during loading to increase chance of deadlock
    test_env.cmd('CONFIG', 'SET', 'search-indexer-yield-every-ops', '10')

    # Verify the configuration was set correctly
    config_result = test_env.cmd('CONFIG', 'GET', 'search-indexer-yield-every-ops')
    print(f"DEBUG: Set search-indexer-yield-every-ops to 10, verified: {config_result}")

    # Copy the RDB file to a location where Redis can access it
    # Get Redis working directory
    redis_dir = test_env.cmd('CONFIG', 'GET', 'dir')[1]
    target_rdb_path = os.path.join(redis_dir, 'load_test.rdb')

    # Copy the RDB file
    import shutil
    shutil.copy2(source_rdb_path, target_rdb_path)
    print(f"DEBUG: Copied RDB to {target_rdb_path}")
    if os.path.exists(target_rdb_path):
      print(f"DEBUG: RDB file size: {os.path.getsize(target_rdb_path)} bytes")

    # # Shared state for the test
    # test_state = {
    #     'stop_info_thread': False,
    #     'info_commands_sent': 0,
    #     'info_errors': [],
    #     'loading_started': False,
    #     'loading_finished': False
    # }

    def info_command_process(port):
        """Process that continuously sends INFO commands"""
        import redis

        # Create a new connection in this process
        conn = redis.Redis(host='localhost', port=port, decode_responses=True)

        while True:
            try:
                print("JOAN DEBUG: Sending INFO command from process")
                result = conn.execute_command('INFO', 'everything')
                print("JOAN DEBUG: INFO command result: " + str(result))
            except Exception as e:
                print(f"JOAN DEBUG: INFO command error: {e}")

            time.sleep(0.01)

    # Start the INFO command thread
    redis_port = test_env.getConnection().connection_pool.connection_kwargs['port']
    info_process = multiprocessing.Process(target=info_command_process, args=(redis_port,), daemon=True)
    info_process.start()

    # Now trigger RDB loading from client side using DEBUG RELOAD
    print("DEBUG: Starting RDB load via DEBUG RELOAD")
    start_time = time.time()

    # try:
    # Verify current config before changing
    current_dir = test_env.cmd('CONFIG', 'GET', 'dir')[1]
    current_dbfilename = test_env.cmd('CONFIG', 'GET', 'dbfilename')[1]
    print(f"DEBUG: Current dir: {current_dir}, dbfilename: {current_dbfilename}")

    # Use DEBUG RELOAD to load the RDB file
    # First, set both the directory and dbfilename to our copied file
    test_env.cmd('CONFIG', 'SET', 'dir', redis_dir)
    test_env.cmd('CONFIG', 'SET', 'dbfilename', 'load_test.rdb')

    # Verify config was set
    new_dir = test_env.cmd('CONFIG', 'GET', 'dir')[1]
    new_dbfilename = test_env.cmd('CONFIG', 'GET', 'dbfilename')[1]
    print(f"DEBUG: Set Redis dir to {new_dir} and dbfilename to {new_dbfilename}")

    # Check if the RDB file exists at the expected location
    expected_rdb_path = os.path.join(new_dir, new_dbfilename)
    print(f"DEBUG: Expected RDB path: {expected_rdb_path}")
    print(f"DEBUG: RDB file exists at expected location: {os.path.exists(expected_rdb_path)}")

    # Get current database size before reload
    dbsize_before = test_env.cmd('DBSIZE')
    print(f"DEBUG: Database size before reload: {dbsize_before}")

    # Trigger the reload - use NOSAVE to prevent overwriting our RDB file
    print("DEBUG: Starting DEBUG RELOAD NOSAVE...")
    test_env.cmd('DEBUG', 'RELOAD', 'NOSAVE')
    print("DEBUG: DEBUG RELOAD NOSAVE command issued")
    time.sleep(1)
    info_process.terminate()
    info_process.join()
        # # Wait for RDB loading to complete by monitoring the loading status
        # # Keep INFO thread running during this entire process
        # loading_timeout = 60  # 60 seconds timeout for RDB loading
        # loading_start_time = time.time()
        # loading_detected = False
        # loading_completed = False

        # print("DEBUG: Waiting for RDB loading to complete...")
        # while time.time() - loading_start_time < loading_timeout:
        #     try:
        #         info_result = test_env.cmd('INFO', 'persistence')
        #         print("JOAN DEBUG: INFO persistence result: " + info_result)
        #         if 'loading:1' in info_result:
        #             if not loading_detected:
        #                 print("DEBUG: RDB loading detected (loading:1)")
        #                 loading_detected = True
        #                 test_state['loading_started'] = True
        #         elif 'loading:0' in info_result and loading_detected:
        #             print("DEBUG: RDB loading completed (loading:0)")
        #             test_state['loading_finished'] = True
        #             loading_completed = True
        #             break
        #         elif 'loading:0' in info_result:
        #             # If we never saw loading:1, the loading might have been very fast
        #             print("DEBUG: Loading status is 0 (either very fast or not loading)")
        #             # Check if database size increased to confirm loading happened
        #             current_dbsize = test_env.cmd('DBSIZE')
        #             if current_dbsize > dbsize_before:
        #                 print(f"DEBUG: Database size increased from {dbsize_before} to {current_dbsize}, loading likely completed")
        #                 loading_completed = True
        #                 test_state['loading_finished'] = True
        #                 break
        #     except Exception as e:
        #         print(f"DEBUG: Error checking loading status: {e}")

        #     time.sleep(2)  # Check every 2s

        # if not loading_completed:
        #     print("ERROR: Timeout waiting for RDB loading to complete!")
        #     test_state['loading_timeout'] = True

        # # Check final database size after reload
        # dbsize_after = test_env.cmd('DBSIZE')
        # print(f"DEBUG: Final database size after reload: {dbsize_after}")

        # if dbsize_after > dbsize_before:
        #     print(f"SUCCESS: RDB loading increased database size from {dbsize_before} to {dbsize_after}")
        # else:
        #     print(f"WARNING: Database size did not increase (before: {dbsize_before}, after: {dbsize_after})")

    # except Exception as e:
    #     print(f"DEBUG: DEBUG RELOAD failed: {e}")
    #     test_state['reload_failed'] = True
    #     print("DEBUG: Continuing test to check for deadlock scenarios")

    # Now stop the INFO process since RDB loading should be complete (or failed)
    #info_thread.join()
    # Verify the server is still responsive
    test_env.expect('PING').equal(True)
    print("DEBUG: Server is still responsive after RDB loading attempt")

    # Check database size to see if anything was loaded
    dbsize = test_env.cmd('DBSIZE')
    print(f"DEBUG: Database size after loading: {dbsize}")

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

    # The INFO commands were sent by the subprocess, so we can't easily count them here
    # But we can verify the server remained responsive throughout the test
    print("DEBUG: Test completed. INFO commands were sent by subprocess during RDB loading.")
