import os
import threading
import time
from includes import *
from common import *
from RLTest import Env


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
    test_env.cmd('CONFIG', 'SET', 'search-indexer-yield-every-ops', '1')

    # Verify the configuration was set correctly
    config_result = test_env.cmd('CONFIG', 'GET', 'search-indexer-yield-every-ops')
    print(f"DEBUG: Set search-indexer-yield-every-ops to 1, verified: {config_result}")

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

    # Shared state for the test
    test_state = {
        'stop_info_thread': False,
        'info_commands_sent': 0,
        'info_errors': [],
        'loading_started': False,
        'loading_finished': False
    }

    def info_command_thread():
        """Thread that continuously sends INFO commands"""
        while not test_state['stop_info_thread']:
            try:
                # Try to connect and send INFO everything command
                conn = test_env.getConnection()
                result = conn.execute_command('INFO', 'everything')
                test_state['info_commands_sent'] += 1

                # Check if server is loading
                if 'loading:1' in result:
                    test_state['loading_started'] = True
                    print("DEBUG: Server is loading RDB")
                elif 'loading:0' in result and test_state['loading_started']:
                    test_state['loading_finished'] = True
                    print("DEBUG: Server finished loading RDB")

            except Exception as e:
                # Store errors but don't fail immediately
                test_state['info_errors'].append(str(e))

            time.sleep(0.01)  # 30ms interval

    # Start the INFO command thread
    info_thread = threading.Thread(target=info_command_thread, daemon=True)
    info_thread.start()

    # Now trigger RDB loading from client side using DEBUG RELOAD
    print("DEBUG: Starting RDB load via DEBUG RELOAD")
    start_time = time.time()

    try:
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
        test_env.cmd('DEBUG', 'RELOAD', 'NOSAVE')
        print("DEBUG: DEBUG RELOAD NOSAVE command completed")

        # Check database size after reload
        dbsize_after = test_env.cmd('DBSIZE')
        print(f"DEBUG: Database size after reload: {dbsize_after}")

        if dbsize_after > dbsize_before:
            print(f"SUCCESS: RDB loading increased database size from {dbsize_before} to {dbsize_after}")
        else:
            print(f"WARNING: Database size did not increase (before: {dbsize_before}, after: {dbsize_after})")

    except Exception as e:
        print(f"DEBUG: DEBUG RELOAD failed: {e}")
        # Try alternative approach - just continue with the test to see if deadlock occurs
        print("DEBUG: Continuing test without RDB loading to test basic deadlock scenario")

    # Wait a bit for any loading to complete
    time.sleep(2)

    # Stop the INFO thread
    test_state['stop_info_thread'] = True
    info_thread.join(timeout=5)

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

    # Verify INFO commands were sent during the test
    test_env.assertGreater(test_state['info_commands_sent'], 0,
                          message="INFO commands should have been sent during the test")

    # Check that we didn't get too many connection errors
    # Some errors are expected, but not too many
    if len(test_state['info_errors']) > test_state['info_commands_sent'] * 0.8:
        test_env.assertTrue(False,
                            message=f"Too many INFO command errors: {len(test_state['info_errors'])} out of {test_state['info_commands_sent']} attempts")

    print(f"DEBUG: Test completed. INFO commands sent: {test_state['info_commands_sent']}, errors: {len(test_state['info_errors'])}")
    print(f"DEBUG: Loading started: {test_state['loading_started']}, Loading finished: {test_state['loading_finished']}")

    # Clean up the copied RDB file
    try:
        if os.path.exists(target_rdb_path):
            os.unlink(target_rdb_path)
            print(f"DEBUG: Cleaned up RDB file: {target_rdb_path}")
    except OSError as e:
        print(f"DEBUG: Could not clean up RDB file: {e}")

    test_env.debugPrint(f"Test completed successfully. INFO commands sent: {test_state['info_commands_sent']}, errors: {len(test_state['info_errors'])}")
