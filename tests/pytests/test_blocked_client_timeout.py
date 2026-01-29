from common import *
import threading
import psutil

TIMEOUT_ERROR = "Timeout limit was reached"
TIMEOUT_WARNING = TIMEOUT_ERROR
ON_TIMEOUT_CONFIG = 'search-on-timeout'


def run_cmd_expect_timeout(env, query_args):
    env.expect(*query_args).error().contains(TIMEOUT_ERROR)


def get_docs_per_shard(env, n_shards):
    """Count documents in each shard."""
    docs_per_shard = []
    for i in range(n_shards):
        conn = env.getConnection(i)
        keys = conn.execute_command('KEYS', '*')
        docs_per_shard.append(len(keys))
    return docs_per_shard

def pid_cmd(conn):
    """Get the process ID of a Redis connection."""
    return conn.execute_command('info', 'server')['process_id']


def get_all_shards_pid(env):
    """Get PIDs from all environment shards (excluding the coordinator)."""
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        yield pid_cmd(conn)

class TestCoordinatorTimeout:
    """Tests for the blocked client timeout mechanism for the coordinator."""

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

    def _test_fail_timeout_impl(self, query_args):
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        initial_jobs_done = getWorkersThpoolStats(env)['totalJobsDone']

        coord_pid = pid_cmd(env.con)
        shards_pid = list(get_all_shards_pid(env))
        shards_pid.remove(coord_pid)

        shard_to_pause_pid = shards_pid[0]
        shard_to_pause_p = psutil.Process(shard_to_pause_pid)

        shard_to_pause_p.suspend()
        with TimeLimit(30, 'Timeout while waiting for shard to pause'):
            while shard_to_pause_p.status() != psutil.STATUS_STOPPED:
                time.sleep(0.1)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        with TimeLimit(30, 'Timeout while waiting for worker to be created'):
            while getWorkersThpoolStats(env)['numThreadsAlive'] == 0:
                time.sleep(0.1)

        with TimeLimit(30, 'Timeout while waiting for worker to finish job'):
            while getWorkersThpoolStats(env)['totalJobsDone'] <= initial_jobs_done:
                time.sleep(0.1)

        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        shard_to_pause_p.resume()
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_fail_timeout_search(self):
        self._test_fail_timeout_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_profile(self):
        self._test_fail_timeout_impl(['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*'])

    def test_fail_timeout_before_fanout(self):
        """Test timeout occurring before the fanout (before query is dispatched to shards)."""
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        # Pause coordinator thread pool to prevent fanout
        env.cmd(debug_cmd(), 'COORD_THREADS', 'PAUSE')

        blocked_client_id = env.cmd('CLIENT', 'ID')

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.SEARCH', 'idx', '*']),
            daemon=True
        )
        t_query.start()

        # Give the query time to be queued
        time.sleep(0.1)

        # Unblock the client to simulate timeout
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        # Resume coordinator threads and restore config
        env.cmd(debug_cmd(), 'COORD_THREADS', 'RESUME')

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_fail_timeout_after_fanout(self):
        """Test timeout occurring after the fanout (after query is dispatched to shards - best effort)."""
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        # Get initial jobs done count from all shards
        initial_jobs_done = [stats['totalJobsDone'] for stats in getWorkersThpoolStatsFromAllShards(env)]

        # Pause worker thread pool on all shards first
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'PAUSE')

        blocked_client_id = env.cmd('CLIENT', 'ID')

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.SEARCH', 'idx', '*']),
            daemon=True
        )
        t_query.start()

        # Pause coordinator thread pool
        env.cmd(debug_cmd(), 'COORD_THREADS', 'PAUSE')

        # Resume worker thread pool on all shards
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'RESUME')

        # Wait for coordinator to dispatch the query (jobs done should increase on shards)
        with TimeLimit(30, 'Timeout while waiting for shards to process query'):
            while True:
                current_jobs_done = [stats['totalJobsDone'] for stats in getWorkersThpoolStatsFromAllShards(env)]
                if all(current > initial for current, initial in zip(current_jobs_done, initial_jobs_done)):
                    break
                time.sleep(0.1)

        # Unblock the client to simulate timeout
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        # Resume coordinator threads
        env.cmd(debug_cmd(), 'COORD_THREADS', 'RESUME')

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def _test_partial_results_timeout_impl(self, query_args):
        """
        Test the partial results timeout mechanism for FT.SEARCH.

        This test:
        1. Sets timeout policy to 'return-strict' (partial results)
        2. Pauses one shard
        3. Runs FT.SEARCH from the coordinator
        4. Waits for the query to be processed by workers (non-paused shards)
        5. Manually unblocks the client with timeout using CLIENT UNBLOCK
        6. Verifies partial results and timeout warning
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        initial_jobs_done = getWorkersThpoolStats(env)['totalJobsDone']

        coord_pid = pid_cmd(env.con)
        shards_pid = list(get_all_shards_pid(env))
        shards_pid.remove(coord_pid)

        # Pause one shard (not the coordinator)
        shard_to_pause_pid = shards_pid[0]
        shard_to_pause_p = psutil.Process(shard_to_pause_pid)

        shard_to_pause_p.suspend()
        with TimeLimit(30, 'Timeout while waiting for shard to pause'):
            while shard_to_pause_p.status() != psutil.STATUS_STOPPED:
                time.sleep(0.1)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
            daemon=True
        )
        t_query.start()

        # Wait for worker thread to be created and start processing
        with TimeLimit(30, 'Timeout while waiting for worker to be created'):
            while getWorkersThpoolStats(env)['numThreadsAlive'] == 0:
                time.sleep(0.1)

        # Wait for worker to finish job
        with TimeLimit(30, 'Timeout while waiting for worker to finish job'):
            while getWorkersThpoolStats(env)['totalJobsDone'] <= initial_jobs_done:
                time.sleep(0.1)

        # Unblock the client with TIMEOUT
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Resume the paused shard
        shard_to_pause_p.resume()

        # Verify partial results and timeout warning
        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        result = result['Results'] if 'Results' in result else result
        env.assertEqual(result['warning'], TIMEOUT_WARNING)

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_partial_results_timeout_search(self):
        """Test partial results timeout for FT.SEARCH."""
        self._test_partial_results_timeout_impl(['FT.SEARCH', 'idx', '*'])

    def test_partial_results_timeout_profile(self):
        """Test partial results timeout for FT.PROFILE SEARCH."""
        self._test_partial_results_timeout_impl(['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*'])

    def test_partial_results_no_replies_timeout(self):
        """
        Test the partial results timeout mechanism when no replies are received.

        This test:
        1. Sets timeout policy to 'return-strict' (partial results)
        2. Pauses coordinator threads before fanout
        3. Runs FT.SEARCH from the coordinator
        4. Manually unblocks the client with timeout using CLIENT UNBLOCK
        5. Verifies 0 results and timeout warning
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        # Pause coordinator thread pool to prevent fanout
        env.cmd(debug_cmd(), 'COORD_THREADS', 'PAUSE')

        blocked_client_id = env.cmd('CLIENT', 'ID')

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.SEARCH', 'idx', '*'], query_result),
            daemon=True
        )
        t_query.start()

        # Unblock the client to simulate timeout
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        # Resume coordinator threads
        env.cmd(debug_cmd(), 'COORD_THREADS', 'RESUME')

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Verify 0 results and timeout warning
        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message="Expected 0 results")
        env.assertEqual(result['warning'], TIMEOUT_WARNING)

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)
