"""
Coverage for native search-* config startup bounds: values the legacy setter
accepts must not abort the server when they arrive via the config file, and
runtime CONFIG SET must keep rejecting the values that startup now tolerates.
"""
import os
import tempfile
from RLTest import Env
from includes import *
from common import *
from test_config import _grep_file_count, MAX_SEARCH_REQUEST_RESULTS, \
    MAX_AGGREGATE_REQUEST_RESULTS, DEFAULT_MAX_SEARCH_REQUEST_RESULTS, default_module_list


def _confPath(name):
    return os.path.join(tempfile.mkdtemp(), name)


def _writeConfigFile(path, directives):
    with open(path, 'w') as f:
        for name, value in directives:
            f.write(f'{name} {value}\n')


def _stripLoadmoduleLines(path):
    # CONFIG REWRITE persists `loadmodule` directives into the config file.
    # RLTest also passes `--loadmodule` on the command line regardless of
    # noDefaultModuleArgs, so restarting with the rewritten file as-is loads
    # every module twice and aborts on the second (duplicate command
    # registration). Strip them; RLTest supplies loadmodule via argv.
    with open(path) as f:
        lines = [l for l in f if not l.startswith('loadmodule ')]
    with open(path, 'w') as f:
        f.writelines(lines)


def _logFilePath(env):
    logDir = env.cmd('config', 'get', 'dir')[1]
    logFileName = env.cmd('CONFIG', 'GET', 'logfile')[1]
    return os.path.join(logDir, logFileName)


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchTimeoutZeroDoesNotAbort():
    """search-timeout 0 ("no timeout") in the startup config file must not
    abort the server, and must be stored as 0."""
    confPath = _confPath('test_native_bounds_timeout.conf')
    _writeConfigFile(confPath, [('search-timeout', 0)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-timeout'), ['search-timeout', '0'])


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchMultiTextSlopZeroDoesNotAbort():
    """search-multi-text-slop 0 in the startup config file must not abort
    the server, and must be stored as 0. Public docs document the range as
    [0 .. 4294967295]."""
    confPath = _confPath('test_native_bounds_slop.conf')
    _writeConfigFile(confPath, [('search-multi-text-slop', 0)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-multi-text-slop'),
                     ['search-multi-text-slop', '0'])


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchMaxSearchResultsNegativeOneTranslates():
    """search-max-search-results -1 in the startup config file must not
    abort the server, and must translate to MAX_SEARCH_REQUEST_RESULTS,
    exactly like the legacy setter does for _FT.CONFIG SET MAXSEARCHRESULTS -1."""
    confPath = _confPath('test_native_bounds_maxsearch.conf')
    _writeConfigFile(confPath, [('search-max-search-results', -1)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-search-results'),
                     ['search-max-search-results', str(MAX_SEARCH_REQUEST_RESULTS)])


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchMaxAggregateResultsNegativeOneTranslates():
    """search-max-aggregate-results -1 in the startup config file must not
    abort the server, and must translate to MAX_AGGREGATE_REQUEST_RESULTS,
    exactly like the legacy setter does for _FT.CONFIG SET MAXAGGREGATERESULTS -1."""
    confPath = _confPath('test_native_bounds_maxagg.conf')
    _writeConfigFile(confPath, [('search-max-aggregate-results', -1)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-aggregate-results'),
                     ['search-max-aggregate-results', str(MAX_AGGREGATE_REQUEST_RESULTS)])


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchMaxSearchResultsOverCapClamps():
    """search-max-search-results 9999999999 (above MAX_SEARCH_REQUEST_RESULTS)
    in the startup config file must not abort the server: the registered max
    is LLONG_MAX so core lets it through to our setter, which clamps it to
    MAX_SEARCH_REQUEST_RESULTS, matching the legacy setter's silent clamp."""
    confPath = _confPath('test_native_bounds_maxsearch_overcap.conf')
    _writeConfigFile(confPath, [('search-max-search-results', 9999999999)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-search-results'),
                     ['search-max-search-results', str(MAX_SEARCH_REQUEST_RESULTS)])


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchMaxAggregateResultsOverCapClamps():
    """Same as above for search-max-aggregate-results: a startup value above
    MAX_AGGREGATE_REQUEST_RESULTS must not abort the server and must clamp
    down to MAX_AGGREGATE_REQUEST_RESULTS."""
    confPath = _confPath('test_native_bounds_maxagg_overcap.conf')
    _writeConfigFile(confPath, [('search-max-aggregate-results', 9999999999)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-aggregate-results'),
                     ['search-max-aggregate-results', str(MAX_AGGREGATE_REQUEST_RESULTS)])


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchMaxSearchResultsNegativeFiveFallsBackToDefault():
    """search-max-search-results -5 is not the -1 sentinel, so it must not
    silently become unlimited: the server must still start, must log a
    warning naming the rejected config and value, and the effective value
    must be left at the default (DEFAULT_MAX_SEARCH_REQUEST_RESULTS)."""
    confPath = _confPath('test_native_bounds_maxsearch_neg5.conf')
    _writeConfigFile(confPath, [('search-max-search-results', -5)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-search-results'),
                     ['search-max-search-results', str(DEFAULT_MAX_SEARCH_REQUEST_RESULTS)])
    env.assertGreaterEqual(_grep_file_count(
        _logFilePath(env),
        f'search-max-search-results: value -5 is out of range, keeping '
        f'{DEFAULT_MAX_SEARCH_REQUEST_RESULTS}'), 1,
        message="expected a startup warning naming the rejected config, value and kept value")


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchMaxAggregateResultsNegativeFiveFallsBackToDefault():
    """Same as above for search-max-aggregate-results: -5 is not the -1
    sentinel, so the server must still start with the default value and a
    matching warning, rather than aborting or treating -5 as unlimited."""
    confPath = _confPath('test_native_bounds_maxagg_neg5.conf')
    _writeConfigFile(confPath, [('search-max-aggregate-results', -5)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-aggregate-results'),
                     ['search-max-aggregate-results', str(MAX_AGGREGATE_REQUEST_RESULTS)])
    env.assertGreaterEqual(_grep_file_count(
        _logFilePath(env),
        f'search-max-aggregate-results: value -5 is out of range, keeping '
        f'{MAX_AGGREGATE_REQUEST_RESULTS}'), 1,
        message="expected a startup warning naming the rejected config, value and kept value")


@skip(cluster=True, redis_less_than='8.0')
def testStartupSearchForkGcCleanThresholdZeroDoesNotAbort():
    """search-fork-gc-clean-threshold 0 in the startup config file must not
    abort the server, and must be stored as 0, exactly like the legacy
    FORK_GC_CLEAN_THRESHOLD path."""
    confPath = _confPath('test_native_bounds_forkgc.conf')
    _writeConfigFile(confPath, [('search-fork-gc-clean-threshold', 0)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-fork-gc-clean-threshold'),
                     ['search-fork-gc-clean-threshold', '0'])


@skip(redis_less_than='8.0')
def testRuntimeConfigSetAcceptsForkGcCleanThresholdZero(env):
    """search-fork-gc-clean-threshold 0 is a legitimate value, matching the
    legacy path, on the native CONFIG SET path too."""
    env.expect('CONFIG', 'SET', 'search-fork-gc-clean-threshold', '0').ok()
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-fork-gc-clean-threshold'),
                     ['search-fork-gc-clean-threshold', '0'])


@skip(redis_less_than='8.0')
def testRuntimeConfigSetTranslatesMaxAggregateResultsNegativeOne(env):
    """search-max-aggregate-results -1 is now a meaningful sentinel on the
    native CONFIG SET path too, mirroring search-max-search-results: it
    translates to MAX_AGGREGATE_REQUEST_RESULTS rather than being rejected."""
    env.expect('CONFIG', 'SET', 'search-max-aggregate-results', '-1').ok()
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-aggregate-results'),
                     ['search-max-aggregate-results', str(MAX_AGGREGATE_REQUEST_RESULTS)])


@skip(redis_less_than='8.0')
def testRuntimeConfigSetClampsMaxSearchResultsOverCap(env):
    """CONFIG SET search-max-search-results 9999999999 must succeed (the
    registered max is LLONG_MAX) and clamp to MAX_SEARCH_REQUEST_RESULTS,
    matching the legacy setter's silent clamp on the native path too."""
    env.expect('CONFIG', 'SET', 'search-max-search-results', '9999999999').ok()
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-search-results'),
                     ['search-max-search-results', str(MAX_SEARCH_REQUEST_RESULTS)])


@skip(redis_less_than='8.0')
def testRuntimeConfigSetClampsMaxAggregateResultsOverCap(env):
    """Same as above for search-max-aggregate-results."""
    env.expect('CONFIG', 'SET', 'search-max-aggregate-results', '9999999999').ok()
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-aggregate-results'),
                     ['search-max-aggregate-results', str(MAX_AGGREGATE_REQUEST_RESULTS)])


@skip(redis_less_than='8.0')
def testRuntimeConfigSetStillRejectsMaxAggregateResultsNegativeTwo(env):
    """Widening the registered min to LLONG_MIN only lets the value reach our
    setter; the setter itself still treats -1 as the only unlimited sentinel,
    so CONFIG SET must keep rejecting -2 as out of range."""
    env.expect('CONFIG', 'SET', 'search-max-aggregate-results', '-2').error()\
        .contains('CONFIG SET failed').contains('out of range')


@skip(redis_less_than='8.0')
def testRuntimeConfigSetStillRejectsMaxSearchResultsNegativeTwo(env):
    """Same as search-max-aggregate-results: only -1 is the unlimited
    sentinel for search-max-search-results, so CONFIG SET keeps rejecting
    -2 as out of range rather than silently treating it as unlimited."""
    env.expect('CONFIG', 'SET', 'search-max-search-results', '-2').error()\
        .contains('CONFIG SET failed').contains('out of range')


# Skip on ASAN since RedisModule_Unload is not fully implemented (MOD-7161)
@skip(cluster=True, redis_less_than='8.0', asan=True)
def testModuleLoadexRuntimeStillRejectsMaxSearchResultsNegativeTwo():
    """RedisModule_OnLoad also runs for MODULE LOADEX against an
    already-running server, not just at genuine process startup. That path
    must stay as strict as CONFIG SET: search-max-search-results -2 is out
    of range (only -1 is the unlimited sentinel), so it must make MODULE
    LOADEX fail rather than silently substitute a default. This is the
    startup-lenient-vs-runtime-strict asymmetry that the noLoadingStartupConfig
    flag exists for, now exercised through one of the two results caps
    instead of search-fork-gc-clean-threshold, which no longer has any
    out-of-range value to test."""
    env = Env(noDefaultModuleArgs=True, module='', moduleArgs='')
    redisearch_module_path = os.getenv('MODULE')
    if redisearch_module_path is None:
        env.debugPrint('MODULE environment variable is not set. Skipping test')
        env.skip()

    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, default_module_list)
    env.expect('MODULE', 'LOADEX', redisearch_module_path,
               'CONFIG', 'search-max-search-results', '-2').error()\
        .contains('Error loading the extension')
    env.assertTrue(env.isUp())
    env.stop()


@skip(cluster=True, redis_less_than='8.0')
def testSearchTimeoutZeroMeansNoTimeoutUnderRealSlowQuery():
    """search-timeout 0, set via the native CONFIG SET path, must mean a
    genuinely slow query runs to completion with no timeout warning, while
    the same query trips a tiny nonzero timeout."""
    env = Env(protocol=3)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # doc:0 matches '-common' immediately; the run of 'common' docs after it
    # forces the NOT iterator to scan past all of them before reaching the
    # trailing rare doc, giving the query real work to do against a 1ms timeout.
    skipped = 300_000
    with conn.pipeline(transaction=False) as p:
        p.execute_command('HSET', 'doc:0', 't', 'rare')
        for i in range(1, skipped + 1):
            p.execute_command('HSET', f'doc:{i}', 't', 'common')
        p.execute_command('HSET', f'doc:{skipped + 1}', 't', 'rare')
        p.execute()

    # Sanity control: a tiny nonzero timeout on this scan must time out.
    env.expect('CONFIG', 'SET', 'search-timeout', '1').ok()
    res = env.cmd('FT.AGGREGATE', 'idx', '-common', 'GROUPBY', '1', '@t', 'REDUCE', 'COUNT', '0', 'AS', 'count')
    env.assertTrue(res.get('warning'), message=f"expected a timeout warning as a sanity control, got {res}")

    # search-timeout 0 must mean the identical query runs to completion with
    # no timeout warning.
    env.expect('CONFIG', 'SET', 'search-timeout', '0').ok()
    res = env.cmd('FT.AGGREGATE', 'idx', '-common', 'GROUPBY', '1', '@t', 'REDUCE', 'COUNT', '0', 'AS', 'count')
    env.assertEqual(res.get('warning', []), [],
                     message=f"expected no timeout warning with search-timeout 0, got {res}")
    # '-common' excludes every 'common' doc, leaving only the 2 'rare' docs -
    # skipped is the scan cost, not the match count.
    env.assertEqual(int(res['results'][0]['extra_attributes']['count']), 2)


@skip(cluster=True, redis_less_than='8.0')
def testConfigRewriteRoundTripSearchTimeoutZero():
    """CONFIG SET search-timeout 0, CONFIG REWRITE, restart: the rewritten
    config file must re-apply 0 at genuine startup, not just in the live
    process."""
    confPath = _confPath('test_native_bounds_rewrite_timeout.conf')
    _writeConfigFile(confPath, [])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.expect('CONFIG', 'SET', 'search-timeout', '0').ok()
    env.expect('CONFIG', 'REWRITE').ok()
    env.stop()
    _stripLoadmoduleLines(confPath)
    env.start()
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-timeout'), ['search-timeout', '0'])


@skip(cluster=True, redis_less_than='8.0')
def testConfigRewriteRoundTripSearchMaxSearchResultsNegativeOne():
    """Same round trip for search-max-search-results -1: the rewritten
    config file must persist the *translated* value (MAX_SEARCH_REQUEST_RESULTS)
    and re-apply it at genuine startup."""
    confPath = _confPath('test_native_bounds_rewrite_maxsearch.conf')
    _writeConfigFile(confPath, [])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.expect('CONFIG', 'SET', 'search-max-search-results', '-1').ok()
    env.expect('CONFIG', 'REWRITE').ok()
    env.stop()
    _stripLoadmoduleLines(confPath)
    env.start()
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-search-results'),
                     ['search-max-search-results', str(MAX_SEARCH_REQUEST_RESULTS)])
