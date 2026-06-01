from RLTest import Env
from includes import *
from common import *
import numpy as np

# MOD-15110 — coverage for the search-max-query-timeout-ms (MAX_TIMEOUT_LIMIT)
# invariant. The knob is enforced only while search-workers == 0; a limit of
# 0 disables the cap entirely. Tests pin the knob explicitly because the
# pytest harness ships it as MAX_TIMEOUT_LIMIT 0 (see runtests.sh).

CAP_WARNING = (
    "Query TIMEOUT exceeded the configured maximum (search-max-query-timeout-ms) "
    "while search-workers is disabled; effective timeout was capped"
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


# ---------------------------- Load-time normalization ----------------------------
#
# RSConfig_PostLoadNormalize caps a persisted search-timeout against the
# new search-max-query-timeout-ms only when configs flow through the new
# RedisModule_LoadConfigs path (e.g. saved redis.conf after an upgrade).
# The legacy MODULE LOAD ... args go through setTimeout / setMaxTimeoutLimit,
# which validate eagerly and would reject the invalid combination before the
# normalizer runs, so we cannot reproduce the cap from RLTest's module args.
# RSConfig_CapQueryTimeoutToMaxLimit (the helper PostLoadNormalize delegates
# to) is covered by tests/cpptests/test_cpp_config_timeout_cap.cpp.

def test_load_time_no_cap_when_workers_enabled():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 5000 MAX_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])

def test_load_time_no_cap_when_limit_disabled():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 5000 MAX_TIMEOUT_LIMIT 0')
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])


# ---------------------------- Runtime CONFIG SET guards --------------------------

def test_runtime_reject_timeout_above_limit():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-timeout', '5000').error()\
        .contains('search-timeout (5000) exceeds search-max-query-timeout-ms (1000)')
    env.expect(config_cmd(), 'SET', 'TIMEOUT', '5000').error()\
        .contains('TIMEOUT (5000) exceeds MAX_TIMEOUT_LIMIT (1000)')
    # The rejected updates must not have leaked through.
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '100']])

def test_runtime_reject_workers_zero_above_limit():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 5000 MAX_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-workers', '0').error()\
        .contains('Cannot set search-workers to 0')
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').error()\
        .contains('Cannot set WORKERS to 0')
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '2']])

def test_runtime_reject_lowering_limit_below_timeout():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 5000 MAX_TIMEOUT_LIMIT 0')
    env.expect('CONFIG', 'SET', 'search-max-query-timeout-ms', '1000').error()\
        .contains('Cannot set search-max-query-timeout-ms to 1000')
    env.expect(config_cmd(), 'SET', 'MAX_TIMEOUT_LIMIT', '1000').error()\
        .contains('Cannot set MAX_TIMEOUT_LIMIT to 1000')
    env.expect(config_cmd(), 'GET', 'MAX_TIMEOUT_LIMIT').equal([['MAX_TIMEOUT_LIMIT', '0']])

def test_runtime_allowed_when_workers_enabled():
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-timeout', '5000').ok()
    env.expect(config_cmd(), 'SET', 'MAX_TIMEOUT_LIMIT', '100').ok()
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '5000']])
    env.expect(config_cmd(), 'GET', 'MAX_TIMEOUT_LIMIT').equal([['MAX_TIMEOUT_LIMIT', '100']])

def test_runtime_allowed_when_limit_disabled():
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 0')
    env.expect('CONFIG', 'SET', 'search-timeout', str(2**31)).ok()
    env.expect(config_cmd(), 'SET', 'TIMEOUT', '500').ok()

def test_runtime_reject_unlimited_timeout_when_limit_active_legacy():
    # TIMEOUT 0 (unlimited) would be silently capped on every query at runtime
    # when WORKERS == 0 and the limit is active, so the legacy _FT.CONFIG SET
    # setter must reject it. The native search-timeout config is gated by the
    # Redis framework at the registered min (1), so there is no equivalent
    # path through `CONFIG SET search-timeout 0` to exercise here.
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    env.expect(config_cmd(), 'SET', 'TIMEOUT', '0').error()\
        .contains('TIMEOUT 0 (unlimited)')
    env.expect(config_cmd(), 'GET', 'TIMEOUT').equal([['TIMEOUT', '100']])

def test_runtime_reject_activating_limit_with_unlimited_timeout():
    # Symmetric to the above: with TIMEOUT 0 (unlimited) already active and
    # WORKERS == 0, activating a positive limit would leave every query capped.
    env = Env(moduleArgs='WORKERS 0 TIMEOUT 0 MAX_TIMEOUT_LIMIT 0')
    env.expect('CONFIG', 'SET', 'search-max-query-timeout-ms', '1000').error()\
        .contains('search-timeout is 0 (unlimited)')
    env.expect(config_cmd(), 'SET', 'MAX_TIMEOUT_LIMIT', '1000').error()\
        .contains('TIMEOUT is 0 (unlimited)')
    env.expect(config_cmd(), 'GET', 'MAX_TIMEOUT_LIMIT').equal([['MAX_TIMEOUT_LIMIT', '0']])

def test_runtime_reject_workers_zero_with_unlimited_timeout():
    # Disabling workers while TIMEOUT is unlimited and a limit is active must
    # be rejected for the same reason TIMEOUT > limit is rejected.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0 MAX_TIMEOUT_LIMIT 1000')
    env.expect('CONFIG', 'SET', 'search-workers', '0').error()\
        .contains('search-timeout is 0 (unlimited)')
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').error()\
        .contains('TIMEOUT is 0 (unlimited)')
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '2']])


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
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '5000', 'NOCONTENT')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.SEARCH expected MAX_TIMEOUT_LIMIT warning, got: {res}")

def test_per_query_aggregate_timeout_capped_resp3():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'TIMEOUT', '5000')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.AGGREGATE expected MAX_TIMEOUT_LIMIT warning, got: {res}")

def test_per_query_hybrid_timeout_capped_resp3():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', '*',
                  'VSIM', '@v', '$BLOB',
                  'PARAMS', '2', 'BLOB', blob,
                  'TIMEOUT', '5000')
    env.assertContains(CAP_WARNING, _get_warnings(res),
                       message=f"FT.HYBRID expected MAX_TIMEOUT_LIMIT warning, got: {res}")


# ---------------------------- Per-query negative cases ---------------------------

def test_per_query_no_cap_when_workers_enabled():
    env = Env(protocol=3, moduleArgs='WORKERS 2 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '5000', 'NOCONTENT')
    env.assertNotContains(CAP_WARNING, _get_warnings(res),
                          message=f"FT.SEARCH should not warn with workers enabled, got: {res}")

def test_per_query_no_cap_when_within_limit():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 1000')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '500', 'NOCONTENT')
    env.assertNotContains(CAP_WARNING, _get_warnings(res),
                          message=f"FT.SEARCH should not warn within limit, got: {res}")

def test_per_query_no_cap_when_limit_disabled():
    env = Env(protocol=3, moduleArgs='WORKERS 0 TIMEOUT 100 MAX_TIMEOUT_LIMIT 0')
    _setup_hybrid_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', str(2**31), 'NOCONTENT')
    env.assertNotContains(CAP_WARNING, _get_warnings(res),
                          message=f"FT.SEARCH should not warn with limit disabled, got: {res}")
