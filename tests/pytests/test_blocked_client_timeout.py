from common import *
import threading
import psutil

TIMEOUT_WARNING = "Timeout limit was reached"


def pid_cmd(conn):
    """Get the process ID of a Redis connection."""
    return conn.execute_command('info', 'server')['process_id']


def get_all_shards_pid(env):
    """Get PIDs from all environment shards (excluding the coordinator)."""
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        yield pid_cmd(conn)


def get_docs_per_shard(env, n_shards):
    """Count documents in each shard."""
    docs_per_shard = []
    for i in range(n_shards):
        conn = env.getConnection(i)
        keys = conn.execute_command('KEYS', '*')
        docs_per_shard.append(len(keys))
    return docs_per_shard


def get_coord_timeout_warnings(conn):
    """Get the coordinator timeout warnings count from INFO MODULES."""
    info = conn.execute_command('INFO', 'MODULES')
    # Parse the INFO output for coord_total_query_warnings_timeout
    for line in info.splitlines():
        if 'coord_total_query_warnings_timeout' in line:
            return int(line.split(':')[1])
    return 0


class TestSearchCoordinatorTimeout:
    """Tests for the blocked client timeout mechanism for FT.SEARCH."""

    def __init__(self):
        # Skip if not cluster
        skipTest(cluster=False)

        # Workers are necessary to ensure the query is dispatched before timeout
        self.env = Env(moduleArgs='WORKERS 1', protocol=3)
        self.n_docs = 100

        # Init all shards
        for i in range(self.env.shardsCount):
            verify_shard_init(self.env.getConnection(i))

        conn = getConnectionByEnv(self.env)

        # Create an index
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()

        # Insert documents
        for i in range(self.n_docs):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    # @skip(cluster=False, asan=True)
    def test_partial_results_timeout(self):
        """
        Test the blocked client timeout mechanism for FT.SEARCH.

        This test:
        1. Counts documents per shard
        2. Pauses one shard
        3. Runs FT.SEARCH from the coordinator
        4. Waits for the query to be processed by workers (non-paused shards)
        5. Manually unblocks the client with timeout using CLIENT UNBLOCK
        6. Verifies partial results and timeout warning
        """
        env = self.env

        # Step 1: Count documents per shard
        docs_per_shard = get_docs_per_shard(env, env.shardsCount)

        # Step 4: Get coordinator PID and shard PIDs
        coord_pid = pid_cmd(env.con)
        shards_pid = list(get_all_shards_pid(env))
        shards_pid.remove(coord_pid)

        # We will pause only one shard (not the coordinator)
        shard_to_pause_pid = shards_pid[0]
        shard_to_pause_p = psutil.Process(shard_to_pause_pid)

        # Pause the shard process
        shard_to_pause_p.suspend()
        with TimeLimit(30, 'Timeout while waiting for shard to pause'):
            while shard_to_pause_p.status() != psutil.STATUS_STOPPED:
                time.sleep(0.1)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        # Prepare the query
        query_result = []
        query_args = ['FT.SEARCH', 'idx', '*']

        # Step 5: Run FT.SEARCH in a separate thread
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
            daemon=True
        )
        t_query.start()

        # Step 6: Wait for the worker thread to be created and start processing
        with TimeLimit(30, 'Timeout while waiting for worker to be created'):
            while getWorkersThpoolStats(env)['numThreadsAlive'] == 0:
                time.sleep(0.1)

        # Drain workers to ensure worker thread in coordinator has finished
        env.expect(debug_cmd(), 'WORKERS', 'drain').ok()
        stats = getWorkersThpoolStats(env)
        env.assertEqual(stats['totalJobsDone'], 1)

        # Unblock the client with TIMEOUT
        unblock_result = env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        # Wait for the query thread to finish
        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Resume the paused shard
        shard_to_pause_p.resume()

        # Step 7: Verify partial results and timeout warning
        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['warning'], TIMEOUT_WARNING)

    def test_no_replies_timeout(self):
        """
        Test the blocked client timeout mechanism for FT.SEARCH when no replies are received.

        This test:
        1. Pauses all shards before they can reply
        2. Runs FT.SEARCH from the coordinator
        3. Manually unblocks the client with timeout using CLIENT UNBLOCK
        4. Verifies 0 results and timeout warning
        """
        env = self.env

        # Step 1: Get the client ID for later unblocking
        blocked_client_id = env.cmd('CLIENT', 'ID')

        # Prepare the query
        query_result = []
        query_args = ['FT.SEARCH', 'idx', '*']

        # Step 2: Run FT.SEARCH in a separate thread (pauses shards before reply)
        t_query = threading.Thread(
            target=call_and_store,
            args=(runDebugQueryCommandPauseBeforeRPAfterN,
                (env, query_args, 'Index', 0),
                query_result),
            daemon=True
        )

        t_query.start()

        # Step 3: Wait for all shards to be paused
        while False in allShards_getIsRPPaused(env):
            time.sleep(0.1)

        # Step 4: Unblock the client with TIMEOUT
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        # Wait for the query thread to finish
        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Resume all shards
        allShards_setPauseRPResume(env)

        # Step 5: Verify 0 results and timeout warning
        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message="Expected 0 results")
        env.assertEqual(result['warning'], TIMEOUT_WARNING)
