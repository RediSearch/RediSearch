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
# Distinct properties grouped over real rows. Narrow enough that building the
# pipeline fits in SMALL_STACK, so a fault can only come from adding rows to
# their groups. Property lookup is linear, so wider requests are slow to build.
WIDE_ROW_GROUPBY = 8192
# The default secondary-thread stack size on macOS; musl's default is smaller.
SMALL_STACK = 512 * 1024
FAULT_SIGNALS = (signal.SIGSEGV, signal.SIGBUS)


def start_env_with_stack_limit(limit):
    """Start a fresh server whose processes inherit an RLIMIT_STACK of `limit` bytes.

    The limit bounds the main thread's stack, past which the kernel faults
    instead of growing it. The test process gets its own limit back at once.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_STACK)
    if hard != resource.RLIM_INFINITY:
        limit = min(limit, hard)
    resource.setrlimit(resource.RLIMIT_STACK, (limit, hard))
    try:
        # WORKERS 0 runs queries on the main thread. Overflowing a worker
        # thread's stack can jump past its guard page instead of faulting.
        return Env(moduleArgs='WORKERS 0', freshEnv=True)
    finally:
        resource.setrlimit(resource.RLIMIT_STACK, (soft, hard))


def execute_or_fault(env, conn, cmd):
    """Run `cmd`. Returns (reply, None), or (None, exit status) if the server
    died of a memory fault. Any other lost connection fails the test."""
    server = env.envRunner.masterProcess
    try:
        return conn.execute_command(*cmd), None
    except redis_exceptions.ResponseError as e:
        return e, None
    except redis_exceptions.ConnectionError:
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
        rc = server.poll()
        if rc is not None and rc < 0 and -rc in FAULT_SIGNALS:
            return None, rc
        raise AssertionError(f'connection lost without a memory fault (server exit status {rc})')


def widest_groupby_pipeline():
    """Build the widest accepted GROUPBY. Returns (cause, exit status) or None."""
    env = start_env_with_stack_limit(SMALL_STACK)
    skipOnExistingEnv(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'f', 'TAG').ok()
    conn.execute_command('HSET', 'doc1', 'f', 'a')
    conn.execute_command('HSET', 'doc2', 'f', 'b')
    waitForIndex(env, 'idx')

    def groupby(nprops):
        # Repeating one property passes argument parsing; the repetition is only
        # rejected while the grouper is built, after its per-property storage exists.
        return ['FT.AGGREGATE', 'idx', '*', 'GROUPBY', nprops, *(['@f'] * nprops),
                'REDUCE', 'COUNT', 0, 'AS', 'n']

    # The same command shape is rejected cleanly when it is narrow.
    env.expect(*groupby(2)).error().contains('specified more than once')

    reply, fault = execute_or_fault(env, conn, groupby(WIDEST_GROUPBY))
    if fault is not None:
        return (f'server died building a {WIDEST_GROUPBY}-property GROUPBY pipeline '
                f'on a {SMALL_STACK // 1024} KiB stack', fault)
    env.assertTrue(isinstance(reply, redis_exceptions.ResponseError),
                   message=f'unexpected reply {reply!r:.200}')
    env.assertContains('specified more than once', str(reply))

    # The server keeps serving grouping requests.
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 1, '@f',
                  'REDUCE', 'COUNT', 0, 'AS', 'n', 'SORTBY', 2, '@f', 'ASC')
    env.assertEqual(res[1:], [['f', 'a', 'n', '1'], ['f', 'b', 'n', '1']])
    return None


def wide_groupby_over_rows():
    """Group a real row by many distinct properties, two of them multi-valued.
    Returns (cause, exit status) or None."""
    env = start_env_with_stack_limit(SMALL_STACK)
    skipOnExistingEnv(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'f', 'TAG').ok()
    fields = [f'g{i}' for i in range(WIDE_ROW_GROUPBY)]
    doc = ['f', 'a']
    for i, name in enumerate(fields):
        doc += [name, str(i)]
    conn.execute_command('HSET', 'doc1', *doc)
    waitForIndex(env, 'idx')

    def groupby(query):
        # The two split() arrays expand the row into 2 x 2 groups.
        props = [f'@{name}' for name in fields] + ['@x', '@y']
        return ['FT.AGGREGATE', 'idx', query, 'LOAD', len(fields), *props[:len(fields)],
                'APPLY', 'split("x1,x2")', 'AS', 'x', 'APPLY', 'split("y1,y2")', 'AS', 'y',
                'GROUPBY', len(props), *props, 'REDUCE', 'COUNT', 0, 'AS', 'n']

    # Without matching rows the same pipeline is built and yields no groups.
    reply, fault = execute_or_fault(env, conn, groupby('@f:{none}'))
    env.assertIsNone(fault, message='server died building the wide row GROUPBY pipeline')
    if fault is not None:
        return None
    env.assertEqual(reply, [0], message=f'unexpected reply {reply!r:.200}')

    reply, fault = execute_or_fault(env, conn, groupby('@f:{a}'))
    if fault is not None:
        return (f'server died grouping rows by {WIDE_ROW_GROUPBY + 2} distinct GROUPBY '
                f'properties on a {SMALL_STACK // 1024} KiB stack', fault)
    env.assertEqual(reply[0], 4, message=f'unexpected reply {str(reply):.200}')
    combos = set()
    for row in reply[1:]:
        values = dict(zip(row[::2], row[1::2]))
        env.assertEqual(values['n'], '1')
        env.assertEqual(values[fields[-1]], str(WIDE_ROW_GROUPBY - 1))
        combos.add((values['x'], values['y']))
    env.assertEqual(combos, {('x1', 'y1'), ('x1', 'y2'), ('x2', 'y1'), ('x2', 'y2')})
    return None


@skip(cluster=True)
def test_widest_groupby_on_small_stack():
    # Each trigger gets its own server, so both are checked even if one kills its server.
    failures = [f for f in (widest_groupby_pipeline(), wide_groupby_over_rows()) if f]
    env = Env.RTestInstance.currEnv
    env.assertFalse(failures, message='; '.join(cause for cause, _ in failures) +
                    f' (server exit statuses {[status for _, status in failures]})')
