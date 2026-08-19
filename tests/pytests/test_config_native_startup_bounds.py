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
    DEFAULT_MAX_AGGREGATE_REQUEST_RESULTS, default_module_list


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


@skip(cluster=True, redis_less_than='7.9.227')
def testStartupSearchTimeoutZeroDoesNotAbort():
    """search-timeout 0 ("no timeout") in the startup config file must not
    abort the server, and must be stored as 0."""
    confPath = _confPath('test_native_bounds_timeout.conf')
    _writeConfigFile(confPath, [('search-timeout', 0)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-timeout'), ['search-timeout', '0'])


@skip(cluster=True, redis_less_than='7.9.227')
def testStartupSearchMultiTextSlopZeroDoesNotAbort():
    """search-multi-text-slop 0 in the startup config file must not abort
    the server, and must be stored as 0. Public docs document the range as
    [0 .. 4294967295]."""
    confPath = _confPath('test_native_bounds_slop.conf')
    _writeConfigFile(confPath, [('search-multi-text-slop', 0)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-multi-text-slop'),
                     ['search-multi-text-slop', '0'])


@skip(cluster=True, redis_less_than='7.9.227')
def testStartupSearchMaxSearchResultsNegativeOneTranslates():
    """search-max-search-results -1 in the startup config file must not
    abort the server, and must translate to MAX_SEARCH_REQUEST_RESULTS,
    exactly like the legacy setter does for _FT.CONFIG SET MAXSEARCHRESULTS -1."""
    confPath = _confPath('test_native_bounds_maxsearch.conf')
    _writeConfigFile(confPath, [('search-max-search-results', -1)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-search-results'),
                     ['search-max-search-results', str(MAX_SEARCH_REQUEST_RESULTS)])


@skip(cluster=True, redis_less_than='7.9.227')
def testStartupSearchMaxAggregateResultsNegativeOneFallsBackToDefault():
    """search-max-aggregate-results -1 is not a meaningful sentinel: the
    server must still start, must log a warning naming the rejected config
    and value, and the effective value must be left as the config's current
    value (DEFAULT_MAX_AGGREGATE_REQUEST_RESULTS here), not -1 taken
    verbatim."""
    confPath = _confPath('test_native_bounds_maxagg.conf')
    _writeConfigFile(confPath, [('search-max-aggregate-results', -1)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-max-aggregate-results'),
                     ['search-max-aggregate-results', str(DEFAULT_MAX_AGGREGATE_REQUEST_RESULTS)])
    env.assertGreaterEqual(_grep_file_count(
        _logFilePath(env),
        f'search-max-aggregate-results: value -1 is out of range, keeping '
        f'{DEFAULT_MAX_AGGREGATE_REQUEST_RESULTS}'), 1,
        message="expected a startup warning naming the rejected config, value and kept value")


@skip(cluster=True, redis_less_than='7.9.227')
def testStartupSearchForkGcCleanThresholdZeroFallsBackToDefault():
    """search-fork-gc-clean-threshold 0 is not a meaningful sentinel: the
    server must still start, must log a warning naming the rejected config
    and value, and the effective value must be left as the config's current
    value (100 here), not 0."""
    confPath = _confPath('test_native_bounds_forkgc.conf')
    _writeConfigFile(confPath, [('search-fork-gc-clean-threshold', 0)])
    env = Env(noDefaultModuleArgs=True, redisConfigFile=confPath)
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-fork-gc-clean-threshold'),
                     ['search-fork-gc-clean-threshold', '100'])
    env.assertGreaterEqual(_grep_file_count(
        _logFilePath(env),
        'search-fork-gc-clean-threshold: value 0 is out of range, keeping 100'), 1,
        message="expected a startup warning naming the rejected config, value and kept value")


@skip(redis_less_than='7.9.227')
def testRuntimeConfigSetStillRejectsForkGcCleanThresholdZero(env):
    """Startup tolerance does not relax runtime strictness: CONFIG SET must
    keep rejecting search-fork-gc-clean-threshold 0."""
    env.expect('CONFIG', 'SET', 'search-fork-gc-clean-threshold', '0').error()\
        .contains('CONFIG SET failed').contains('out of range')


@skip(redis_less_than='7.9.227')
def testRuntimeConfigSetStillRejectsMaxAggregateResultsNegativeOne(env):
    """Startup tolerance does not relax runtime strictness: CONFIG SET must
    keep rejecting search-max-aggregate-results -1."""
    env.expect('CONFIG', 'SET', 'search-max-aggregate-results', '-1').error()\
        .contains('CONFIG SET failed').contains('out of range')


# Skip on ASAN since RedisModule_Unload is not fully implemented (MOD-7161)
@skip(cluster=True, redis_less_than='7.9.227')
def testModuleLoadexRuntimeStillRejectsForkGcCleanThresholdZero():
    """RedisModule_OnLoad also runs for MODULE LOADEX against an
    already-running server, not just at genuine process startup. That path
    must stay as strict as CONFIG SET: search-fork-gc-clean-threshold 0 must
    make MODULE LOADEX fail, not silently substitute the default."""
    env = Env(noDefaultModuleArgs=True, module='', moduleArgs='')
    redisearch_module_path = os.getenv('MODULE')
    if redisearch_module_path is None:
        env.debugPrint('MODULE environment variable is not set. Skipping test')
        env.skip()

    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, default_module_list)
    env.expect('MODULE', 'LOADEX', redisearch_module_path,
               'CONFIG', 'search-fork-gc-clean-threshold', '0').error()\
        .contains('Error loading the extension')
    env.assertTrue(env.isUp())
    env.stop()


@skip(cluster=True)
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


@skip(cluster=True, redis_less_than='7.9.227')
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


@skip(cluster=True, redis_less_than='7.9.227')
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
