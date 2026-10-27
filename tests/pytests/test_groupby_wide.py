# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import resource
import subprocess

from common import *

# The widest GROUPBY the argument parser accepts (MAX_GROUPBY_PROPERTIES).
WIDEST_GROUPBY = 2**16 - 1
# The default secondary-thread stack size on macOS; musl's default is smaller.
SMALL_STACK = 512 * 1024
FAULT_SIGNALS = (signal.SIGSEGV, signal.SIGBUS)


def start_env_with_stack_limit(limit, **env_args):
    """Start a server whose processes inherit an RLIMIT_STACK of `limit` bytes.

    The limit bounds the main thread's stack, past which the kernel faults
    instead of growing it. The test process gets its own limit back at once.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_STACK)
    if hard != resource.RLIM_INFINITY:
        limit = min(limit, hard)
    resource.setrlimit(resource.RLIMIT_STACK, (limit, hard))
    try:
        return Env(**env_args)
    finally:
        resource.setrlimit(resource.RLIMIT_STACK, (soft, hard))


def groupby_command(nprops):
    # Repeating one property passes argument parsing; the repetition is only
    # rejected while the grouper is built, after its per-property storage exists.
    return ['FT.AGGREGATE', 'idx', '*', 'GROUPBY', nprops, *(['@f'] * nprops),
            'REDUCE', 'COUNT', 0, 'AS', 'n']


@skip(cluster=True)
def test_widest_groupby_on_small_stack():
    # WORKERS 0 builds the pipeline on the main thread. Overflowing a worker
    # thread's stack can jump past its guard page instead of faulting.
    env = start_env_with_stack_limit(SMALL_STACK, moduleArgs='WORKERS 0')
    skipOnExistingEnv(env)
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'f', 'TAG').ok()
    conn.execute_command('HSET', 'doc1', 'f', 'a')
    conn.execute_command('HSET', 'doc2', 'f', 'b')
    waitForIndex(env, 'idx')

    # The same command shape is rejected cleanly when it is narrow.
    env.expect(*groupby_command(2)).error().contains('specified more than once')

    server = env.envRunner.masterProcess
    try:
        reply = conn.execute_command(*groupby_command(WIDEST_GROUPBY))
    except redis_exceptions.ResponseError as e:
        reply = e
    except redis_exceptions.ConnectionError:
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
        rc = server.poll()
        if rc is not None and rc < 0 and -rc in FAULT_SIGNALS:
            cause = (f'server died building a {WIDEST_GROUPBY}-property GROUPBY pipeline '
                     f'on a {SMALL_STACK // 1024} KiB stack')
        else:
            cause = 'connection lost without a memory fault'
        env.assertTrue(False, message=f'{cause} (server exit status {rc})')
        return

    env.assertTrue(isinstance(reply, redis_exceptions.ResponseError),
                   message=f'unexpected reply {reply!r:.200}')
    env.assertContains('specified more than once', str(reply))

    # The server keeps serving grouping requests.
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 1, '@f',
                  'REDUCE', 'COUNT', 0, 'AS', 'n', 'SORTBY', 2, '@f', 'ASC')
    env.assertEqual(res[1:], [['f', 'a', 'n', '1'], ['f', 'b', 'n', '1']])
