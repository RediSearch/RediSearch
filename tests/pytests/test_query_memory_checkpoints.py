# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""The query memory checkpoints, driven by `_FT.DEBUG MEMORY_PRESSURE AT_CALL <n>`, which reports
pressure on the n-th checkpoint call of the process. The fake pressure works on any Redis, so these
tests do not depend on the server exporting the memory API."""

from common import *

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
SHARD_OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"
COORD_OOM_WARNING = "Coordinator failed to execute the query due to insufficient memory"
# FT.SEARCH and FT.AGGREGATE read memory before building the iterators, after them, and after the
# pipeline; the pipeline checkpoints count from there.
BUILD_CHECKPOINTS = 3
N_DOCS = 10

SEARCH = ('FT.SEARCH', 'idx', '*', 'NOCONTENT')
AGGREGATE = ('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n')


def memory_pressure(env, *args):
    return env.cmd(debug_cmd(), 'MEMORY_PRESSURE', *args)


def checkpoint_calls(env):
    return memory_pressure(env, 'STATUS')[5]


def run_at_call(env, n, *query):
    """Runs `query` with the n-th checkpoint call from now reporting pressure."""
    memory_pressure(env, 'AT_CALL', n)
    try:
        return env.cmd(*query)
    finally:
        memory_pressure(env, 'RESET')


def expect_oom_error_at_call(env, n, *query):
    memory_pressure(env, 'AT_CALL', n)
    try:
        env.expect(*query).error().contains(OOM_QUERY_ERROR)
    finally:
        memory_pressure(env, 'RESET')


class oom_policy:
    """Sets ON_OOM on every shard for a block and restores RETURN after it."""
    def __init__(self, env, policy):
        self.env, self.policy = env, policy

    def __enter__(self):
        allShards_change_oom_policy(self.env, self.policy)

    def __exit__(self, *exc):
        allShards_change_oom_policy(self.env, 'return')


def worker_counts(env):
    """Yields 0 and 2 after setting WORKERS to each, and restores 0 afterwards."""
    try:
        for workers in (0, 2):
            set_workers(env, workers)
            yield workers
    finally:
        set_workers(env, 0)


def create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 'n', i, 't', f'hello{i}')


def expect_empty_with_oom_warning(env, res):
    env.assertEqual(res['results'], [], message=res)
    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)


@skip(cluster=True)
@env_spec(protocol=3)
def test_build_checkpoints_stop_before_execution(env):
    """Every build stage reads memory once: under pressure ON_OOM RETURN replies an empty result
    with the OOM warning and ON_OOM FAIL the OOM error, before any row is read, with and without
    workers. Past the build stages nothing stops the query."""
    create_index(env)
    for workers in worker_counts(env):
        for at_call in range(1, BUILD_CHECKPOINTS + 1):
            for query in (SEARCH, AGGREGATE):
                expect_empty_with_oom_warning(env, run_at_call(env, at_call, *query))
                with oom_policy(env, 'fail'):
                    expect_oom_error_at_call(env, at_call, *query)
                with oom_policy(env, 'ignore'):
                    env.assertEqual(len(run_at_call(env, at_call, *query)['results']), N_DOCS)

        res = run_at_call(env, BUILD_CHECKPOINTS + 1, *AGGREGATE)
        env.assertEqual(len(res['results']), N_DOCS, message=res)
        env.assertEqual(res['warning'], [], message=res)


@skip(cluster=True)
@env_spec(protocol=2)
def test_build_checkpoint_stop_in_resp2(env):
    """RESP2 has no warning channel: a query stopped at build replies an empty result."""
    create_index(env)
    env.assertEqual(run_at_call(env, 1, *SEARCH), [0])
    env.assertEqual(run_at_call(env, 1, *AGGREGATE), [0])


@skip(cluster=True)
@env_spec(protocol=3)
def test_build_checkpoint_stop_is_not_a_timeout(env):
    """Under ON_TIMEOUT FAIL a build-time OOM stop still follows ON_OOM, inline (WORKERS 0, clock
    deadline) and in the background (blocked-client timeout)."""
    create_index(env)
    env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()
    try:
        for workers in worker_counts(env):
            for at_call in range(1, BUILD_CHECKPOINTS + 1):
                expect_empty_with_oom_warning(env, run_at_call(env, at_call, *AGGREGATE))
    finally:
        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()


@skip(cluster=True)
def test_build_checkpoint_stop_ends_the_cursor(env):
    """A cursor stopped at build returns no rows and is not left open; under ON_OOM FAIL the
    query fails and no cursor is created."""
    create_index(env)
    res, cursor = run_at_call(env, 1, *AGGREGATE, 'WITHCURSOR', 'COUNT', 2)
    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)
    with oom_policy(env, 'fail'):
        expect_oom_error_at_call(env, 1, *AGGREGATE, 'WITHCURSOR', 'COUNT', 2)
    env.assertEqual(to_dict(to_dict(env.cmd('FT.INFO', 'idx'))['cursor_stats'])['index_total'], 0)


@skip(cluster=True)
def test_explain_does_not_read_memory(env):
    """FT.EXPLAIN builds a plan it never runs, so pressure neither fails it nor counts as a
    checkpoint call."""
    create_index(env)
    with oom_policy(env, 'fail'):
        memory_pressure(env, 'AT_CALL', 1)
        try:
            env.assertContains('NUMERIC', env.cmd('FT.EXPLAIN', 'idx', '@n:[1 2]'))
            env.assertEqual(checkpoint_calls(env), 0)
        finally:
            memory_pressure(env, 'RESET')


def create_hybrid_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}',
                             'v', np.array([i, 0]).astype(np.float32).tobytes())


HYBRID = ('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$blob',
          'PARAMS', 2, 'blob', np.array([0, 0]).astype(np.float32).tobytes())


def hybrid_result_count(res):
    return len(res[res.index('results') + 1])


@skip(cluster=True)
def test_hybrid_build_checkpoint_fails_before_execution(env):
    """Under ON_OOM FAIL, FT.HYBRID reads memory before building the first subquery (where a
    queued job starts) and after building each one, and pressure at any of them fails the query
    with the OOM error before it runs. Under ON_OOM RETURN it does not read memory at build."""
    create_hybrid_index(env)
    full = hybrid_result_count(env.cmd(*HYBRID))
    env.assertGreater(full, 0)
    with oom_policy(env, 'fail'):
        for workers in worker_counts(env):
            for at_call in (1, 2, 3):
                expect_oom_error_at_call(env, at_call, *HYBRID)
            env.assertEqual(hybrid_result_count(run_at_call(env, 4, *HYBRID)), full)
    memory_pressure(env, 'AT_CALL', 1)
    try:
        env.assertEqual(hybrid_result_count(env.cmd(*HYBRID)), full)
        env.assertEqual(checkpoint_calls(env), 0)
    finally:
        memory_pressure(env, 'RESET')


@skip(cluster=False)
@env_spec(protocol=3)
def test_build_checkpoint_stop_on_shards_reaches_the_coordinator(env):
    """Shards stopped at build answer like shards stopped later in the pipeline: the coordinator
    turns their OOM warning or error into its own ON_OOM outcome."""
    create_index(env)

    def at_first_shard_call(*query):
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
        try:
            return env.cmd(*query)
        finally:
            run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    expect_empty_with_oom_warning(env, at_first_shard_call(*AGGREGATE))
    # The FT.SEARCH coordinator replies the shards' wording.
    res = at_first_shard_call(*SEARCH)
    env.assertEqual(res['results'], [], message=res)
    env.assertEqual(res['warning'], [SHARD_OOM_WARNING], message=res)

    with oom_policy(env, 'fail'):
        for query in (AGGREGATE, SEARCH):
            run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
            try:
                env.expect(*query).error().contains(OOM_QUERY_ERROR)
            finally:
                run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    res = env.cmd(*AGGREGATE)
    env.assertEqual(len(res['results']), N_DOCS, message=res)


N_PREFIX_TERMS = 400


@skip(cluster=True)
@env_spec(protocol=3)
def test_term_expansion_checks_memory_per_expanded_term(env):
    """A prefix or wildcard expansion reads memory as it opens one reader per expanded term, so
    pressure partway through, past the default MAXPREFIXEXPANSIONS, ends the expansion there: the
    build stops under ON_OOM RETURN (empty result with the OOM warning) and fails under ON_OOM FAIL.
    The call count shows that no further term was opened."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_PREFIX_TERMS):
        conn.execute_command('HSET', f'doc{i}', 't', f'pre{i}')
    env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 1000).ok()
    try:
        for workers in worker_counts(env):
            for query in (('FT.SEARCH', 'idx', '@t:pre*', 'NOCONTENT'),
                          ('FT.SEARCH', 'idx', "@t:w'pre*'", 'NOCONTENT')):
                # Call 1 is the build entry; the later calls are the expanded terms.
                for at_call in (5, 250):
                    memory_pressure(env, 'AT_CALL', at_call)
                    try:
                        expect_empty_with_oom_warning(env, env.cmd(*query))
                        env.assertEqual(checkpoint_calls(env), at_call)
                    finally:
                        memory_pressure(env, 'RESET')
                    with oom_policy(env, 'fail'):
                        expect_oom_error_at_call(env, at_call, *query)

                res = env.cmd(*query)
                env.assertEqual(res['total_results'], N_PREFIX_TERMS, message=res)
    finally:
        env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 200).ok()


@skip(cluster=True)
def test_hybrid_term_expansion_checks_memory_under_fail(env):
    """A hybrid text branch reads memory per expanded term under ON_OOM FAIL, so pressure inside the
    expansion ends it there and fails the query with the OOM error through the post-build check;
    the call count shows that no further term was opened. Under ON_OOM RETURN, hybrid does not
    read memory at build and the expansion takes no part."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_PREFIX_TERMS):
        conn.execute_command('HSET', f'doc{i}', 't', f'pre{i}',
                             'v', np.array([i, 0]).astype(np.float32).tobytes())
    env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 1000).ok()
    query = ('FT.HYBRID', 'idx', 'SEARCH', '@t:pre*', 'VSIM', '@v', '$blob',
             'PARAMS', 2, 'blob', np.array([0, 0]).astype(np.float32).tobytes())
    try:
        full = hybrid_result_count(env.cmd(*query))
        env.assertGreater(full, 0)
        for workers in worker_counts(env):
            with oom_policy(env, 'fail'):
                # Call 1 is the pre-build read; the later calls are the expanded terms.
                for at_call in (5, 250):
                    memory_pressure(env, 'AT_CALL', at_call)
                    try:
                        env.expect(*query).error().contains(OOM_QUERY_ERROR)
                        env.assertEqual(checkpoint_calls(env), at_call)
                    finally:
                        memory_pressure(env, 'RESET')
            memory_pressure(env, 'AT_CALL', 5)
            try:
                env.assertEqual(hybrid_result_count(env.cmd(*query)), full)
                env.assertEqual(checkpoint_calls(env), 0)
            finally:
                memory_pressure(env, 'RESET')
    finally:
        env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 200).ok()
