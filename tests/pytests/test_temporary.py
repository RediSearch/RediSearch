# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
import time


@skip(cluster=True, redis_less_than='7.2')  # 7.2 added eventloop_cycles
def testLongTemporaryKeepsTheEventLoopIdle(env):
    """Redis truncates the ms until the next module timer to an `int` (MOD-21060). A TEMPORARY
    index has no GC, so with only TEMPORARY indexes its expiry timer is the earliest module timer
    once a shorter one fires. This TTL leaves it about 1 s short of 2^32 ms then, which truncates
    to a small negative value: the event loop woke on every pass, 100% CPU on an idle server."""
    env.expect('FT.CREATE', 'long', 'TEMPORARY', 4294967, 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.CREATE', 'short', 'TEMPORARY', 1, 'SCHEMA', 't', 'TEXT').ok()
    wait_for_condition(lambda: ('short' not in env.cmd('FT._LIST'), {}),
                       'TEMPORARY index never expired', timeout=30)

    cycles = lambda: env.cmd('INFO', 'stats')['eventloop_cycles']
    before = cycles()
    time.sleep(1)
    spent = cycles() - before
    # An idle second takes about hz (10) cycles; a spinning loop takes millions.
    env.assertLess(spent, 1000, message=f'{spent} event-loop cycles in an idle second')
    # Its timer fires early now, at the cap, and re-arms; the index itself must not expire early.
    env.assertEqual(env.cmd('FT._LIST'), ['long'])


@skip(cluster=True)
def testTemporaryOutOfRange(env):
    """The TTL is kept in ms as a long long. A larger one used to wrap, and the index was dropped
    as soon as it was created."""
    env.expect('FT.CREATE', 'idx', 'TEMPORARY', 9223372036854776, 'SCHEMA', 't', 'TEXT') \
        .error().contains('TEMPORARY value out of range')
    env.expect('FT.CREATE', 'idx', 'TEMPORARY', -9223372036854776, 'SCHEMA', 't', 'TEXT') \
        .error().contains('TEMPORARY value out of range')

    # The largest TTL that fits stays put.
    env.expect('FT.CREATE', 'idx', 'TEMPORARY', 9223372036854775, 'SCHEMA', 't', 'TEXT').ok()
    time.sleep(2)
    env.assertEqual(env.cmd('FT._LIST'), ['idx'])
