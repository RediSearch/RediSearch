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
from test_config import MAX_SEARCH_REQUEST_RESULTS


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
