from common import *
import threading
import psutil

TIMEOUT_ERROR = "Timeout limit was reached"
TIMEOUT_WARNING = TIMEOUT_ERROR
ON_TIMEOUT_CONFIG = 'search-on-timeout'


def run_cmd_expect_timeout(env, query_args):
    env.expect(*query_args).error().contains(TIMEOUT_ERROR)

def pid_cmd(conn):
    """Get the process ID of a Redis connection."""
    return conn.execute_command('info', 'server')['process_id']


def get_all_shards_pid(env):
    """Get PIDs from all environment shards (excluding the coordinator)."""
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        yield pid_cmd(conn)



class TestSearchCoordinatorTimeout:
    """Tests for the blocked client timeout mechanism for FT.SEARCH."""

    def __init__(self):
        # Skip if not cluster
        # skipTest(cluster=False)

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
    def test_fail_timeout(self):
        """
        Test the blocked client timeout mechanism for FT.SEARCH with FAIL policy.
        This test:
        1. Sets on-timeout policy to 'fail'
        2. Gets coordinator and shard PIDs, then pauses one shard
        3. Runs FT.SEARCH in a separate thread from the coordinator
        4. Waits for the worker thread to be created and process the query
        5. Manually unblocks the client with timeout using CLIENT UNBLOCK
        6. Verifies timeout error and resumes the paused shard
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        # Step 2: Get coordinator PID and shard PIDs, then pause one shard
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

        # Step 3: Run FT.SEARCH in a separate thread
        query_args = ['FT.SEARCH', 'idx', '*']
        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        # Step 4: Wait for the worker thread to be created and process the query
        with TimeLimit(30, 'Timeout while waiting for worker to be created'):
            while getWorkersThpoolStats(env)['numThreadsAlive'] == 0:
                time.sleep(0.1)

        with TimeLimit(30, 'Timeout while waiting for worker to finish job'):
            while getWorkersThpoolStats(env)['totalJobsDone'] < 1:
                time.sleep(0.1)

        # Step 5: Manually unblock the client with timeout
        unblock_result = env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        # Step 6: Verify timeout error and resume the paused shard
        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        shard_to_pause_p.resume()

        # Restore the previous on-timeout policy
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)
