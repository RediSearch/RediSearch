import os
import pytest
import multiprocessing
import threading
import time
import signal
import tempfile
from common import skip, getRDBFile, REDISEARCH_CACHE_DIR, debug_cmd
from RLTest import Env

@skip(cluster=True)
@pytest.mark.timeout(120)
def test_rdb_load_no_deadlock():
    """
    Test that loading from RDB while constantly sending INFO commands doesn't cause deadlock.
    This test starts a clean Redis server, then triggers RDB loading from the client side
    while some subprocesses keep sending INFO commands.
    """
    # Bundled RDB fixture (see tests/pytests/test_rdbs/)
    rdb_filename = 'redisearch_8.0_with_vecsim.rdb'

    # Create a clean Redis environment
    test_env = Env(moduleArgs='')

    # Start the server first
    test_env.start()

    # Verify server is running
    test_env.expect('PING').equal(True)

    # Verify the bundled RDB fixture is available
    if not getRDBFile(test_env, rdb_filename):
        return

    # Configure indexer to yield more frequently during loading to increase chance of deadlock
    test_env.cmd('CONFIG', 'SET', 'search-indexer-yield-every-ops', '1')
    test_env.cmd('CONFIG', 'SET', 'busy-reply-threshold', 1)
    test_env.expect(debug_cmd(), 'INDEXER_SLEEP_BEFORE_YIELD_MICROS', '50000').ok()

    # Get Redis configuration for RDB file location
    dbFileName = test_env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = test_env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)

    # Path to the bundled RDB fixture
    filePath = os.path.join(REDISEARCH_CACHE_DIR, rdb_filename)

    # Disable periodic auto-save to prevent Redis 8.x's initial BGSAVE (or any
    # save-threshold triggered by the CONFIG SET calls above) from overwriting
    # the RDB symlink via rename() after we create it.  NOSAVE in DEBUG RELOAD
    # only skips the pre-reload dump; an independent BGSAVE can still win the
    # race and write an empty DB over our fixture symlink.
    test_env.cmd('CONFIG', 'SET', 'save', '')
    # Wait for any already-running BGSAVE to finish before we swap the file so
    # the BGSAVE child's rename() cannot clobber the symlink.
    for _attempt in range(50):  # up to ~5 s
        info = test_env.cmd('INFO', 'persistence')
        in_progress = (
            info.get('rdb_bgsave_in_progress', 0) if isinstance(info, dict)
            else int('rdb_bgsave_in_progress:1' in str(info))
        )
        if not in_progress:
            break
        time.sleep(0.1)

    # Create symlink to the downloaded RDB file
    try:
        os.unlink(rdbFilePath)
    except OSError:
        pass
    os.symlink(filePath, rdbFilePath)

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

    # Get current database size before reload
    # Trigger the reload - use NOSAVE to prevent overwriting our RDB file
    test_env.cmd('DEBUG', 'RELOAD', 'NOSAVE')
    for process in info_processes:
        process.terminate()
        process.join()

    test_env.expect('PING').equal(True)

    # Check database size to see if anything was loaded
    dbsize = test_env.cmd('DBSIZE')

    # Try to get info about any existing indices
    indices_info = test_env.cmd('FT._LIST')
    assert indices_info, "No indices found after RDB load"
    # If there are indices, verify we can get info about the first one
    test_env.expect('FT.INFO', indices_info[0]).noError()
