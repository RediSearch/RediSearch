from RLTest import Env
from includes import *
from common import *
import numpy as np
import threading
import time

# MOD-15110 — coverage for the search-_max-foreground-timeout-limit
# (_MAX_FOREGROUND_TIMEOUT_LIMIT) invariant. The knob is enforced only while
# search-workers == 0; a limit of 0 disables the cap entirely. Tests pin the
# knob explicitly because the pytest harness ships it as
# _MAX_FOREGROUND_TIMEOUT_LIMIT 0 (see runtests.sh).

CAP_WARNING = (
    "Query TIMEOUT exceeded the configured maximum "
    "(search-_max-foreground-timeout-limit) while search-workers is disabled; "
    "effective timeout was capped"
)

def _get_warnings(response):
    """Pull warnings out of FT.SEARCH/FT.AGGREGATE ('warning') or
    FT.HYBRID ('warnings') replies in both RESP2 and RESP3 shapes."""
    for key in ('warning', 'warnings'):
        if isinstance(response, dict) and key in response:
            return response[key]
        idx = recursive_index(response, key)
        if idx != -1:
            idx[-1] += 1
            return access_nested_list(response, idx)
    return []


def _setup_hybrid_index(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    for i in range(4):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}',
                             'v', np.array([float(i), 0.0]).astype(np.float32).tobytes())


# ---------------------------- Load-time behaviour --------------------------------
#
# RSConfig_PostLoadNormalize logs a warning when the cross-knob invariant is
# violated at load time but no longer mutates the in-memory value: the
# per-query cap (RSConfig_CapQueryTimeoutToForegroundLimit in AREQ_Compile /
# parseHybridCommand) enforces the limit at runtime so that queries which
# inherit the global also surface the RESP3 MAX_TIMEOUT_CAPPED warning. The
# helper itself is covered by tests/cpptests/test_cpp_config_timeout_cap.cpp.

def test_load_time_no_cap_when_workers_enabled():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])

def test_load_time_preserves_timeout_above_limit():
    # With the simplified accept-and-log model, the load-time value above the
    # limit is preserved verbatim; per-query enforcement (covered below)
    # handles the actual cap.
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])
    env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
        [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000']])


class TestLoadTimeAndRuntimeWorkersZeroLimitDisabled:
    """Shares Env(W=0 T=5000 L=0). The read-only load-time assertion runs
    first; lowering the limit below the (preserved) global TIMEOUT runs last.
    Tests are independent of order — the limit mutation does not touch the
    TIMEOUT field the first test inspects."""

    def __init__(self):
        self.env = Env(moduleArgs='WORKERS 0 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')

    def test_load_time_no_cap_when_limit_disabled(self):
        self.env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])

    def test_runtime_accept_lowering_limit_below_timeout(self):
        env = self.env
        env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000').ok()
        env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
            [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000']])
        env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '500').ok()
        env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
            [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '500']])


# ---------------------------- Runtime CONFIG SET behaviour -----------------------
#
# All six setters (native search-timeout / search-workers /
# search-_max-foreground-timeout-limit and the legacy _FT.CONFIG SET
# equivalents) accept the value when the cross-knob invariant is violated and
# emit a RedisModule_Log warning. Per-query enforcement happens at FT.SEARCH /
# FT.AGGREGATE / FT.HYBRID time.

class TestRuntimeTimeoutMutationsAboveLimit:
    """Shares Env(W=0 T=100 L=1000). Each test fully sets the TIMEOUT field it
    asserts on, so order within the class is irrelevant."""

    def __init__(self):
        self.env = Env(moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')

    def test_runtime_accept_timeout_above_limit(self):
        env = self.env
        env.expect('CONFIG', 'SET', 'search-timeout', '5000').ok()
        env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])
        env.expect(config_cmd(), 'SET', 'TIMEOUT', '7000').ok()
        env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '7000']])

    def test_runtime_accept_unlimited_timeout_when_limit_active_legacy(self):
        # Native search-timeout is gated at the registered min (1); TIMEOUT 0
        # is only reachable via the legacy _FT.CONFIG SET path.
        env = self.env
        env.expect(config_cmd(), 'SET', 'TIMEOUT', '0').ok()
        env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '0']])


def test_runtime_accept_workers_zero_above_limit():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-workers', '0').ok()
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])
    # Re-enable then exercise the legacy path on a fresh env to verify it too.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])

def test_runtime_allowed_when_workers_enabled():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-timeout', '5000').ok()
    env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '100').ok()
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])
    env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
        [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '100']])

def test_runtime_allowed_when_limit_disabled():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    env.expect('CONFIG', 'SET', 'search-timeout', str(2**31)).ok()
    env.expect(config_cmd(), 'SET', 'TIMEOUT', '500').ok()

def test_runtime_accept_activating_limit_with_unlimited_timeout():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 0 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000').ok()
    env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
        [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000']])
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 0 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000').ok()
    env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
        [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000']])

def test_runtime_accept_workers_zero_with_unlimited_timeout():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-workers', '0').ok()
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])


# ---------------------------- Per-query TIMEOUT capping --------------------------

class TestPerQueryCappedResp3:
    """Shares Env(protocol=3, W=0 T=100 L=1000) and a single hybrid-capable
    index. Each test issues an explicit TIMEOUT and asserts the RESP3 warning
    is (or is not) surfaced. None of the tests mutate config, so order is
    irrelevant."""

    def __init__(self):
        self.env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
        _setup_hybrid_index(self.env)

    def test_search_timeout_capped(self):
        env = self.env
        res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '5000', 'NOCONTENT')
        env.assertContains(CAP_WARNING, _get_warnings(res),
                           message=f"FT.SEARCH expected MAX_TIMEOUT_CAPPED warning, got: {res}")

    def test_aggregate_timeout_capped(self):
        env = self.env
        res = env.cmd('FT.AGGREGATE', 'idx', '*', 'TIMEOUT', '5000')
        env.assertContains(CAP_WARNING, _get_warnings(res),
                           message=f"FT.AGGREGATE expected MAX_TIMEOUT_CAPPED warning, got: {res}")

    def test_hybrid_timeout_capped(self):
        env = self.env
        blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
        res = env.cmd('FT.HYBRID', 'idx',
                      'SEARCH', '*',
                      'VSIM', '@v', '$BLOB',
                      'PARAMS', '2', 'BLOB', blob,
                      'TIMEOUT', '5000')
        env.assertContains(CAP_WARNING, _get_warnings(res),
                           message=f"FT.HYBRID expected MAX_TIMEOUT_CAPPED warning, got: {res}")

    def test_no_cap_when_within_limit(self):
        env = self.env
        res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '500', 'NOCONTENT')
        env.assertNotContains(CAP_WARNING, _get_warnings(res),
                              message=f"FT.SEARCH should not warn within limit, got: {res}")


def test_per_query_inherits_global_timeout_capped_resp3():
    # Point 4: when WORKERS == 0 and the global TIMEOUT exceeds the limit,
    # a query that does *not* pass TIMEOUT must still inherit the (capped)
    # global and surface the MAX_TIMEOUT_CAPPED RESP3 warning.
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.SEARCH expected MAX_TIMEOUT_CAPPED warning when "
                               f"inheriting global TIMEOUT, got: {res}")
    res = env.cmd('FT.AGGREGATE', 'idx', '*')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.AGGREGATE expected MAX_TIMEOUT_CAPPED warning when "
                               f"inheriting global TIMEOUT, got: {res}")
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', '*',
                  'VSIM', '@v', '$BLOB',
                  'PARAMS', '2', 'BLOB', blob)
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.HYBRID expected MAX_TIMEOUT_CAPPED warning when "
                               f"inheriting global TIMEOUT, got: {res}")


# ---------------------------- Per-query negative cases ---------------------------

def test_per_query_no_cap_when_workers_enabled():
    env = Env(protocol=3, moduleArgs='WORKERS 2 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '5000', 'NOCONTENT')
    env.assertNotContains(CAP_WARNING, _get_warnings(res),
                          message=f"FT.SEARCH should not warn with workers enabled, got: {res}")


# ---------------------------- Cursor capping --------------------------------
#
# Cursors cache the resolved timeout on the AREQ at WITHCURSOR time, so a
# limit tightened after cursor creation must still cap on subsequent reads.
# runCursor re-applies RSConfig_CapQueryTimeoutToForegroundLimit on every
# FT.CURSOR READ and the QEXEC_S_MAX_TIMEOUT_CAPPED flag is sticky once set.

class TestProtocol3WorkersZeroLimitDisabled:
    """Shares Env(protocol=3, W=0 T=100 L=0) and a single hybrid-capable index.
    The no-cap negative path runs first while the limit is still 0; the
    cursor-tightening test mutates the limit to 1000 at the end. Each test
    asserts the starting limit explicitly so order does not silently corrupt
    later runs."""

    def __init__(self):
        self.env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
        _setup_hybrid_index(self.env)

    def test_per_query_no_cap_when_limit_disabled(self):
        env = self.env
        env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '0').ok()
        res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', str(2**31), 'NOCONTENT')
        env.assertNotContains(CAP_WARNING, _get_warnings(res),
                              message=f"FT.SEARCH should not warn with limit disabled, got: {res}")

    def test_cursor_read_caps_after_limit_tightened(self):
        env = self.env
        env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '0').ok()
        # FT.AGGREGATE WITHCURSOR returns a 2-element array [<results map>, <cursor_id>]
        # in RESP3. Unwrap it before pulling warnings.
        res = env.cmd('FT.AGGREGATE', 'idx', '*',
                      'LOAD', '1', '@t',
                      'TIMEOUT', '5000',
                      'WITHCURSOR', 'COUNT', '1')
        results_map, cursor_id = res[0], res[1]
        # Open a cursor with a large TIMEOUT while the cap is disabled — the
        # initial reply must not carry the warning.
        env.assertNotContains(CAP_WARNING, _get_warnings(results_map),
                              message=f"Initial WITHCURSOR reply should not warn while limit is 0, got: {res}")
        # Tighten the limit below the cached cursor TIMEOUT and read again.
        env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000').ok()
        res = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        results_map = res[0]
        env.assertContains(CAP_WARNING, _get_warnings(results_map),
                           message=f"FT.CURSOR READ expected MAX_TIMEOUT_CAPPED warning after "
                                   f"limit tightened, got: {res}")


# ---------------------------- Coordinator cursor timer cap -----------------------
#
# In cluster mode under ON_TIMEOUT FAIL/RETURN_STRICT, CursorCommand arms the
# blocked-client timer from the cursor's cached queryTimeoutMS *before*
# dispatching to runCursor on the shards. The coord must re-apply the
# foreground cap so the timer fires at the capped budget when shards are
# unreachable; otherwise the coord would wait for the original (uncapped)
# WITHCURSOR TIMEOUT.

def test_cursor_read_coord_timer_capped_after_limit_tightened():
    skipTest(cluster=False)
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 60000 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    _setup_hybrid_index(env)
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', 'fail')
    # Open a cursor while the cap is disabled — cursor caches queryTimeoutMS=60000.
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@t',
                  'TIMEOUT', '60000', 'WITHCURSOR', 'COUNT', '1')
    cursor_id = res[1]
    # Tighten the cap below the cursor's cached TIMEOUT.
    env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '500').ok()
    # Pause the coord threadpool so the cursor-read job cannot be picked up;
    # the blocked-client timer is then the only unblock path.
    env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
        'Timeout while waiting for coordinator threads to pause', timeout=30)
    try:
        outcome = {}
        def run_read():
            try:
                outcome['reply'] = env.cmd('FT.CURSOR', 'READ', 'idx', str(cursor_id))
            except Exception as e:
                outcome['error'] = str(e)
        t = threading.Thread(target=run_read, daemon=True)
        start = time.monotonic()
        t.start()
        # The capped timer should fire in ~500ms; allow generous slack but
        # well under the 60000ms uncapped budget.
        t.join(timeout=10)
        elapsed = time.monotonic() - start
        env.assertFalse(t.is_alive(),
                        message=f"FT.CURSOR READ did not return within 10s (elapsed={elapsed:.2f}s); "
                                f"coord timer was not capped")
        env.assertLess(elapsed, 5.0,
                       message=f"FT.CURSOR READ took {elapsed:.2f}s; coord timer should have "
                               f"fired near the 500ms cap, not the 60s cursor TIMEOUT")
        # FAIL policy surfaces the timeout as a Redis error from the BC timeout callback.
        env.assertIn('error', outcome,
                     message=f"Expected timeout error from FT.CURSOR READ, got reply: {outcome}")
        env.assertContains('Timeout', outcome['error'],
                           message=f"Expected timeout error from FT.CURSOR READ, got: {outcome['error']}")
    finally:
        env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
            'Timeout while waiting for coordinator threads to resume', timeout=30)
