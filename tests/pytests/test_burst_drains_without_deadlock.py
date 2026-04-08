from common import *
import threading


RQ_CAPACITY = 50  # CONN_PER_SHARD=1 and SEARCH_IO_THREADS=1 => 1 * PENDING_FACTOR(50)
SHARD_COUNT = 3
WORKER_COUNT = 3
DOC_COUNT = 240

# FT.SEARCH query and expected result
search = ['FT.SEARCH', 'idx', 'searchable', 'LIMIT', 0, 3,
          'SORTBY', 'price', 'DESC', 'NOCONTENT']
expected_search_res = [240, 'doc:239', 'doc:238', 'doc:237']

# FT.AGGREGATE query and expected result
aggregate = ['FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@category',
             'REDUCE', 'COUNT', '0', 'AS', 'count',
             'SORTBY', 2, '@count', 'ASC']
expected_aggregate_res = [
                            3,
                            ['category', 'books', 'count', '80'],
                            ['category', 'food', 'count', '80'],
                            ['category', 'electronics', 'count', '80']
                        ]

def _build_mixed_burst_commands():
    # Intentionally build a 3:1 FT.SEARCH-to-FT.AGGREGATE prefix so the burst
    # stays mixed while still driving the coordinator into the saturation region,
    # then continue draining the aggregate-heavy tail.
    return ([search] * 3 + [aggregate]) * 30 + [aggregate] * 120


def _run_burst_command(env, command, exceptions, completed, lock):
    try:
        conn = getConnectionByEnv(env)
        res = conn.execute_command(*command)
        if command[0] == 'FT.SEARCH':
            env.assertEqual(res, expected_search_res)
        elif command[0] == 'FT.AGGREGATE':
            env.assertEqual(res, expected_aggregate_res)
    except Exception as exc:
        with lock:
            exceptions.append(f'{command}: {exc}')
    finally:
        with lock:
            completed[0] += 1


def _coordinator_reached_rq_limit(env, coord_initial_jobs_done, coord_initial_pending_jobs, completed):
    stats = getCoordThpoolStats(env)
    jobs_done_delta = stats['totalJobsDone'] - coord_initial_jobs_done
    pending_jobs_delta = stats['totalPendingJobs'] - coord_initial_pending_jobs
    done = jobs_done_delta >= RQ_CAPACITY or pending_jobs_delta >= RQ_CAPACITY
    return done, {
        'coord_stats': stats,
        'jobs_done_delta': jobs_done_delta,
        'pending_jobs_delta': pending_jobs_delta,
        'completed': completed[0],
    }


def _shards_started_draining(env, shard_initial_jobs_done):
    current = [stats['totalJobsDone'] for stats in getWorkersThpoolStatsFromAllShards(env)]
    done = all(cur > initial for cur, initial in zip(current, shard_initial_jobs_done))
    return done, {'current_jobs_done': current, 'initial_jobs_done': shard_initial_jobs_done}


def _burst_completed(commands, completed, exceptions):
    return completed[0] == len(commands), {
        'completed': completed[0],
        'total': len(commands),
        'exceptions': exceptions,
    }


def _print_debug_stats(label, env, coord_initial_jobs_done, coord_initial_pending_jobs,
                       shard_initial_jobs_done, completed):
    coord_current_stats = getCoordThpoolStats(env)
    shard_current_jobs_done = [stats['totalJobsDone'] for stats in getWorkersThpoolStatsFromAllShards(env)]
    env.debugPrint('-' * 80, force=True)
    env.debugPrint(
        f'[{label}] '
        f'coord_initial_jobs_done={coord_initial_jobs_done} '
        f'coord_initial_pending_jobs={coord_initial_pending_jobs} '
        f'shard_initial_jobs_done={shard_initial_jobs_done} '
        f'coord_current_stats={coord_current_stats} '
        f'shard_current_jobs_done={shard_current_jobs_done} '
        f'completed={completed[0]}',
        force=True,
    )


@skip(cluster=False)
def test_search_and_aggregate_burst():
    env = Env(
        testName='Search and aggregate burst',
        shardsCount=SHARD_COUNT,
        moduleArgs='SEARCH_IO_THREADS 1',
        enableDebugCommand=True,
    )
    verify_shard_init(env)
    set_workers(env, WORKER_COUNT)
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '1').ok()

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'title', 'TEXT',
               'body', 'TEXT',
               'category', 'TAG',
               'price', 'NUMERIC').ok()

    categories = ['books', 'electronics', 'food']
    for i in range(DOC_COUNT):
        conn.execute_command(
            'HSET', f'doc:{i}',
            'title', f'document {i}',
            'body', f'searchable body text {i}',
            'category', categories[i % len(categories)],
            'price', i,
        )

    waitForIndex(env, 'idx')

    commands = _build_mixed_burst_commands()
    exceptions = []
    completed = [0]
    lock = threading.Lock()
    threads = []

    coord_initial_stats = getCoordThpoolStats(env)
    coord_initial_jobs_done = coord_initial_stats['totalJobsDone']
    coord_initial_pending_jobs = coord_initial_stats['totalPendingJobs']
    shard_initial_jobs_done = [stats['totalJobsDone'] for stats in getWorkersThpoolStatsFromAllShards(env)]

    _print_debug_stats(
        'before_pause',
        env,
        coord_initial_jobs_done,
        coord_initial_pending_jobs,
        shard_initial_jobs_done,
        completed,
    )

    verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'PAUSE')

    try:
        for i, command in enumerate(commands):
            thread = threading.Thread(
                target=_run_burst_command,
                args=(env, command, exceptions, completed, lock),
                name=f'search-and-aggregate-burst-query-{i}',
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        wait_for_condition(
            lambda: _coordinator_reached_rq_limit(
                env,
                coord_initial_jobs_done,
                coord_initial_pending_jobs,
                completed,
            ),
            'Timeout waiting for coordinator to reach the RQ saturation threshold',
            timeout=20,
        )

        # Happy-path handoff: once the coordinator is saturated, resume shard
        # workers so the test can verify queued mixed requests start draining.
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'RESUME')

        wait_for_condition(
            lambda: _shards_started_draining(env, shard_initial_jobs_done),
            'Timeout waiting for shard workers to resume processing queued jobs',
            timeout=20,
        )

        _print_debug_stats(
            'after_shards_started_draining',
            env,
            coord_initial_jobs_done,
            coord_initial_pending_jobs,
            shard_initial_jobs_done,
            completed,
        )

        wait_for_condition(
            lambda: _burst_completed(commands, completed, exceptions),
            'Timeout waiting for mixed FT.SEARCH/FT.AGGREGATE burst to drain',
            timeout=15,
        )

        _print_debug_stats(
            'after_burst_completed',
            env,
            coord_initial_jobs_done,
            coord_initial_pending_jobs,
            shard_initial_jobs_done,
            completed,
        )
    # Best-effort cleanup: if the happy-path RESUME was skipped by a failure,
    # try to leave shard workers runnable before joining the background threads.
    finally:
        try:
            verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'RESUME')
        except Exception:
            pass
        for thread in threads:
            thread.join(timeout=1)

    # Verify all burst queries completed successfully
    env.assertEqual(exceptions, [],
                    message=f'Background burst queries failed: {exceptions}')

    # Verify Redis remains responsive after the burst
    ping_result = env.cmd('PING')
    env.assertTrue(ping_result in ['PONG', True],
                   message='Coordinator should remain responsive after burst')
    env.expect(*search).equal(expected_search_res)
    env.expect(*aggregate).equal(expected_aggregate_res)
