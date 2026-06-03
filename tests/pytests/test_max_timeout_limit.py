from RLTest import Env
from includes import *
from common import *
import numpy as np

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

def test_load_time_no_cap_when_limit_disabled():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])

def test_load_time_preserves_timeout_above_limit():
    # With the simplified accept-and-log model, the load-time value above the
    # limit is preserved verbatim; per-query enforcement (covered below)
    # handles the actual cap.
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])
    env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
        [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000']])


# ---------------------------- Runtime CONFIG SET behaviour -----------------------
#
# All six setters (native search-timeout / search-workers /
# search-_max-foreground-timeout-limit and the legacy _FT.CONFIG SET
# equivalents) accept the value when the cross-knob invariant is violated and
# emit a RedisModule_Log warning. Per-query enforcement happens at FT.SEARCH /
# FT.AGGREGATE / FT.HYBRID time.

def test_runtime_accept_timeout_above_limit():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-timeout', '5000').ok()
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])
    env.expect(config_cmd(), 'SET', 'TIMEOUT', '7000').ok()
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '7000']])

def test_runtime_accept_workers_zero_above_limit():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-workers', '0').ok()
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])
    # Re-enable then exercise the legacy path on a fresh env to verify it too.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])

def test_runtime_accept_lowering_limit_below_timeout():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 5000 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000').ok()
    env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
        [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000']])
    env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '500').ok()
    env.expect(config_cmd(), 'GET', '_MAX_FOREGROUND_TIMEOUT_LIMIT').equal(
        [['_MAX_FOREGROUND_TIMEOUT_LIMIT', '500']])

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

def test_runtime_accept_unlimited_timeout_when_limit_active_legacy():
    # The native search-timeout config is gated by the Redis framework at the
    # registered min (1), so TIMEOUT 0 is only reachable via _FT.CONFIG SET.
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'SET', 'TIMEOUT', '0').ok()
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '0']])

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

def _setup_hybrid_index(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    for i in range(4):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}',
                             'v', np.array([float(i), 0.0]).astype(np.float32).tobytes())

def test_per_query_search_timeout_capped_resp3():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '5000', 'NOCONTENT')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.SEARCH expected MAX_TIMEOUT_CAPPED warning, got: {res}")

def test_per_query_aggregate_timeout_capped_resp3():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'TIMEOUT', '5000')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.AGGREGATE expected MAX_TIMEOUT_CAPPED warning, got: {res}")

def test_per_query_hybrid_timeout_capped_resp3():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', '*',
                  'VSIM', '@v', '$BLOB',
                  'PARAMS', '2', 'BLOB', blob,
                  'TIMEOUT', '5000')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.HYBRID expected MAX_TIMEOUT_CAPPED warning, got: {res}")

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

def test_per_query_no_cap_when_within_limit():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '500', 'NOCONTENT')
    env.assertNotContains(CAP_WARNING, _get_warnings(res),
                          message=f"FT.SEARCH should not warn within limit, got: {res}")

def test_per_query_no_cap_when_limit_disabled():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', str(2**31), 'NOCONTENT')
    env.assertNotContains(CAP_WARNING, _get_warnings(res),
                          message=f"FT.SEARCH should not warn with limit disabled, got: {res}")


# ---------------------------- Cursor capping --------------------------------
#
# Cursors cache the resolved timeout on the AREQ at WITHCURSOR time, so a
# limit tightened after cursor creation must still cap on subsequent reads.
# runCursor re-applies RSConfig_CapQueryTimeoutToForegroundLimit on every
# FT.CURSOR READ and the QEXEC_S_MAX_TIMEOUT_CAPPED flag is sticky once set.

def test_cursor_read_caps_after_limit_tightened():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
    _setup_hybrid_index(env)
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
