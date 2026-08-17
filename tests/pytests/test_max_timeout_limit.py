from RLTest import Env
from includes import *
from common import *
import numpy as np

# MOD-15110 — coverage for the search-_max-foreground-timeout-limit
# (_MAX_FOREGROUND_TIMEOUT_LIMIT) invariant. The knob is enforced only while
# search-workers == 0; a limit of 0 disables the cap entirely. Tests pin the
# knob explicitly because the pytest harness ships it as
# _MAX_FOREGROUND_TIMEOUT_LIMIT 0 (see runtests.sh).
#
# All tests share a single protocol=3 Env: each test calls _reset(...) at the
# start to pin WORKERS / TIMEOUT / _MAX_FOREGROUND_TIMEOUT_LIMIT, so order is
# irrelevant. RESP3 is required because the per-query cap surfaces as a
# RESP3-only warning; CONFIG GET assertions go through _assert_config() to
# normalise the RESP3 map vs RESP2 list shapes.
#
# Load-time PostLoadNormalize semantics are covered by
# tests/cpptests/test_cpp_config_timeout_cap.cpp; the accept-and-log model
# means the load-time path is functionally equivalent to runtime CONFIG SET.

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

class TestMaxForegroundTimeoutLimit:
    """All MOD-15110 coverage lives here, sharing a single protocol=3 Env.

    Each test calls _reset(...) to pin (WORKERS, TIMEOUT, limit) before
    exercising its path. The hybrid-capable index is created once in
    __init__; tests that only touch CONFIG ignore it."""

    def __init__(self):
        self.env = Env(protocol=3,
                       moduleArgs='WORKERS 0 TIMEOUT 100 _MAX_FOREGROUND_TIMEOUT_LIMIT 0')
        conn = getConnectionByEnv(self.env)
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA',
                        't', 'TEXT',
                        'v', 'VECTOR', 'FLAT', '6',
                        'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        for i in range(4):
            conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}',
                                 'v', np.array([float(i), 0.0]).astype(np.float32).tobytes())

    def _reset(self, *, workers, timeout, limit):
        """Pin all three knobs on every shard. timeout=0 routes via legacy
        _FT.CONFIG SET because native search-timeout is gated at min=1.
        Propagation is required so any test that previously set a knob via
        run_command_on_all_shards cannot leak its value into the next test."""
        env = self.env
        # Drop the limit first so intermediate states never violate the
        # cross-knob invariant during the transition.
        verify_command_OK_on_all_shards(env, 'CONFIG', 'SET',
                                        'search-_max-foreground-timeout-limit', '0')
        verify_command_OK_on_all_shards(env, 'CONFIG', 'SET',
                                        'search-workers', str(workers))
        if timeout == 0:
            verify_command_OK_on_all_shards(env, config_cmd(), 'SET', 'TIMEOUT', '0')
        else:
            verify_command_OK_on_all_shards(env, 'CONFIG', 'SET',
                                            'search-timeout', str(timeout))
        verify_command_OK_on_all_shards(env, 'CONFIG', 'SET',
                                        'search-_max-foreground-timeout-limit', str(limit))

    def _assert_config(self, name, expected):
        """Assert a single config value, normalising RESP3 map vs RESP2 list
        shapes from native CONFIG GET / legacy _FT.CONFIG GET."""
        res = self.env.cmd(config_cmd(), 'GET', name)
        if isinstance(res, dict):
            self.env.assertEqual(res.get(name), expected,
                                 message=f"{name}: got {res.get(name)!r}, expected {expected!r}")
        else:
            self.env.assertEqual(res, [[name, expected]],
                                 message=f"{name}: got {res!r}, expected [[{name!r}, {expected!r}]]")

    # ---------------------------- Runtime CONFIG SET behaviour -------------------
    #
    # All six setters (native search-timeout / search-workers /
    # search-_max-foreground-timeout-limit and the legacy _FT.CONFIG SET
    # equivalents) accept the value when the cross-knob invariant is violated
    # and emit a RedisModule_Log warning. Per-query enforcement happens at
    # FT.SEARCH / FT.AGGREGATE / FT.HYBRID time.

    def test_runtime_accept_timeout_above_limit(self):
        self._reset(workers=0, timeout=100, limit=1000)
        env = self.env
        env.expect('CONFIG', 'SET', 'search-timeout', '5000').ok()
        self._assert_config('TIMEOUT', '5000')
        env.expect(config_cmd(), 'SET', 'TIMEOUT', '7000').ok()
        self._assert_config('TIMEOUT', '7000')

    def test_runtime_accept_unlimited_timeout_when_limit_active_legacy(self):
        # Native search-timeout is gated at the registered min (1); TIMEOUT 0
        # is only reachable via the legacy _FT.CONFIG SET path.
        self._reset(workers=0, timeout=100, limit=1000)
        env = self.env
        env.expect(config_cmd(), 'SET', 'TIMEOUT', '0').ok()
        self._assert_config('TIMEOUT', '0')

    def test_runtime_accept_lowering_limit_below_timeout(self):
        self._reset(workers=0, timeout=5000, limit=0)
        env = self.env
        env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000').ok()
        self._assert_config('_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000')
        env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '500').ok()
        self._assert_config('_MAX_FOREGROUND_TIMEOUT_LIMIT', '500')

    def test_runtime_accept_workers_zero_above_limit(self):
        # Native path.
        self._reset(workers=2, timeout=5000, limit=1000)
        env = self.env
        env.expect('CONFIG', 'SET', 'search-workers', '0').ok()
        self._assert_config('WORKERS', '0')
        # Legacy path from a fresh known state.
        self._reset(workers=2, timeout=5000, limit=1000)
        env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
        self._assert_config('WORKERS', '0')

    def test_runtime_accept_activating_limit_with_unlimited_timeout(self):
        # Native path: WORKERS=0, TIMEOUT=0, activate the limit.
        self._reset(workers=0, timeout=0, limit=0)
        env = self.env
        env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000').ok()
        self._assert_config('_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000')
        # Legacy path from a fresh known state.
        self._reset(workers=0, timeout=0, limit=0)
        env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000').ok()
        self._assert_config('_MAX_FOREGROUND_TIMEOUT_LIMIT', '1000')

    def test_runtime_accept_workers_zero_with_unlimited_timeout(self):
        # Native path: TIMEOUT=0, limit active, flip WORKERS to 0.
        self._reset(workers=2, timeout=0, limit=1000)
        env = self.env
        env.expect('CONFIG', 'SET', 'search-workers', '0').ok()
        self._assert_config('WORKERS', '0')
        # Legacy path from a fresh known state.
        self._reset(workers=2, timeout=0, limit=1000)
        env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
        self._assert_config('WORKERS', '0')

    def test_runtime_allowed_when_workers_enabled(self):
        self._reset(workers=2, timeout=100, limit=1000)
        env = self.env
        env.expect('CONFIG', 'SET', 'search-timeout', '5000').ok()
        env.expect(config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', '100').ok()
        self._assert_config('TIMEOUT', '5000')
        self._assert_config('_MAX_FOREGROUND_TIMEOUT_LIMIT', '100')

    def test_runtime_allowed_when_limit_disabled(self):
        self._reset(workers=0, timeout=100, limit=0)
        env = self.env
        env.expect('CONFIG', 'SET', 'search-timeout', str(2**31)).ok()
        env.expect(config_cmd(), 'SET', 'TIMEOUT', '500').ok()

    # ---------------------------- Per-query TIMEOUT capping ----------------------

    def test_search_timeout_capped(self):
        self._reset(workers=0, timeout=100, limit=1000)
        env = self.env
        res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '5000', 'NOCONTENT')
        env.assertContains(CAP_WARNING, _get_warnings(res),
                           message=f"FT.SEARCH expected MAX_TIMEOUT_CAPPED warning, got: {res}")

    def test_aggregate_timeout_capped(self):
        self._reset(workers=0, timeout=100, limit=1000)
        env = self.env
        res = env.cmd('FT.AGGREGATE', 'idx', '*', 'TIMEOUT', '5000')
        env.assertContains(CAP_WARNING, _get_warnings(res),
                           message=f"FT.AGGREGATE expected MAX_TIMEOUT_CAPPED warning, got: {res}")

    def test_hybrid_timeout_capped(self):
        self._reset(workers=0, timeout=100, limit=1000)
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
        self._reset(workers=0, timeout=100, limit=1000)
        env = self.env
        res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '500', 'NOCONTENT')
        env.assertNotContains(CAP_WARNING, _get_warnings(res),
                              message=f"FT.SEARCH should not warn within limit, got: {res}")

    def test_per_query_inherits_global_timeout_capped(self):
        # When WORKERS == 0 and the global TIMEOUT exceeds the limit, a query
        # that does *not* pass TIMEOUT must still inherit the (capped) global
        # and surface the MAX_TIMEOUT_CAPPED RESP3 warning.
        self._reset(workers=0, timeout=5000, limit=1000)
        env = self.env
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

    def test_per_query_no_cap_when_workers_enabled(self):
        self._reset(workers=2, timeout=100, limit=1000)
        env = self.env
        res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', '5000', 'NOCONTENT')
        env.assertNotContains(CAP_WARNING, _get_warnings(res),
                              message=f"FT.SEARCH should not warn with workers enabled, got: {res}")

    def test_per_query_no_cap_when_limit_disabled(self):
        self._reset(workers=0, timeout=100, limit=0)
        env = self.env
        res = env.cmd('FT.SEARCH', 'idx', '*', 'TIMEOUT', str(2**31), 'NOCONTENT')
        env.assertNotContains(CAP_WARNING, _get_warnings(res),
                              message=f"FT.SEARCH should not warn with limit disabled, got: {res}")

    # ---------------------------- Cursor capping --------------------------------
    #
    # Cursors cache the resolved timeout on the AREQ at WITHCURSOR time, so a
    # limit tightened after cursor creation must still cap on subsequent reads.
    # runCursor re-applies RSConfig_CapQueryTimeoutToForegroundLimit on every
    # FT.CURSOR READ and the QEXEC_S_MAX_TIMEOUT_CAPPED flag is sticky once set.
    #
    # Both standalone and cluster modes are covered explicitly: in cluster mode
    # the warning is emitted by the cursor-owning shard's runCursor and must
    # survive coord-side aggregation before reaching the client.

    def _open_cursor_then_tighten_and_read(self, set_limit):
        """Open a WITHCURSOR with TIMEOUT above the (initially-disabled) limit,
        confirm the first reply carries no cap warning, then apply set_limit()
        and FT.CURSOR READ. Returns the READ response for warning assertion."""
        self._reset(workers=0, timeout=100, limit=0)
        env = self.env
        # FT.AGGREGATE WITHCURSOR returns a 2-element [<results>, <cursor_id>]
        # in both RESP2 and RESP3. Unwrap before pulling warnings.
        res = env.cmd('FT.AGGREGATE', 'idx', '*',
                      'LOAD', '1', '@t',
                      'TIMEOUT', '5000',
                      'WITHCURSOR', 'COUNT', '1')
        results_map, cursor_id = res[0], res[1]
        # Cap is still disabled — the initial reply must not carry the warning.
        env.assertNotContains(CAP_WARNING, _get_warnings(results_map),
                              message=f"Initial WITHCURSOR reply should not warn while limit is 0, got: {res}")
        # Tighten the limit below the cached cursor TIMEOUT and read again.
        set_limit()
        return env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)

    def test_cursor_read_caps_after_limit_tightened_standalone(self):
        skipTest(cluster=True)
        env = self.env
        res = self._open_cursor_then_tighten_and_read(
            lambda: env.expect('CONFIG', 'SET',
                               'search-_max-foreground-timeout-limit', '1000').ok())
        env.assertContains(CAP_WARNING, _get_warnings(res[0]),
                           message=f"FT.CURSOR READ expected MAX_TIMEOUT_CAPPED warning after "
                                   f"limit tightened (standalone), got: {res}")

    def test_cursor_read_caps_after_limit_tightened_cluster(self):
        # The cursor lives on whichever shard served FT.AGGREGATE WITHCURSOR;
        # env.cmd is sticky to one node and we do not know which. Propagate the
        # limit to all shards so the cap fires regardless of routing, then
        # verify the warning survives coord-side aggregation back to the client.
        skipTest(cluster=False)
        env = self.env
        res = self._open_cursor_then_tighten_and_read(
            lambda: verify_command_OK_on_all_shards(
                env, 'CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000'))
        env.assertContains(CAP_WARNING, _get_warnings(res[0]),
                           message=f"FT.CURSOR READ expected MAX_TIMEOUT_CAPPED warning after "
                                   f"limit tightened (cluster), got: {res}")
