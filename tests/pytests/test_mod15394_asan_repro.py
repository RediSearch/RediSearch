# MOD-15394 crash reproduction harness (NOT a normal regression test).
# Goal: trigger the coordinator-iterator use-after-free observed in production
# (mrIteratorRedisCB dereferencing a freed MRIterator -> ctx->it == NULL) by
# stressing FT.HYBRID teardown while shard cursor-read replies are still in
# flight. Run the module built with SAN=address; ASAN aborts the offending
# shard and writes a heap-use-after-free report to its log.
#
# Tunables via env vars: UAF_DURATION (s), UAF_THREADS, UAF_NDOCS.
import os
import socket
import random
import threading
import time
from common import *


def _resp(*args):
    out = b'*%d\r\n' % len(args)
    for a in args:
        b = a if isinstance(a, (bytes, bytearray)) else str(a).encode()
        out += b'$%d\r\n%s\r\n' % (len(b), bytes(b))
    return out


@skip(cluster=False)
def test_mod15394_hybrid_teardown_uaf_stress(env):
    DURATION = float(os.environ.get('UAF_DURATION', '90'))
    NTHREADS = int(os.environ.get('UAF_THREADS', '12'))
    NDOCS = int(os.environ.get('UAF_NDOCS', '40000'))
    dim = 8

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim),
               'DISTANCE_METRIC', 'L2').ok()

    # Tight timeout + FAIL so queries abort mid-flight (teardown while shard
    # cursor reads are still outstanding). RediSearch exposes these as standard
    # redis module configs (search-* prefix), set on every shard.
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', 'fail')
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-timeout', '5')

    # Enough docs that a hybrid result set needs many FT.CURSOR READ round-trips.
    vec = np.ones(dim, dtype=np.float32).tobytes()
    p = conn.pipeline(transaction=False)
    for i in range(NDOCS):
        p.execute_command('HSET', f'd{i}', 't', 'hello world alpha beta', 'v', vec)
        if i % 2000 == 1999:
            p.execute()
            p = conn.pipeline(transaction=False)
    p.execute()

    qvec = np.zeros(dim, dtype=np.float32).tobytes()
    hybrid_args = ('FT.HYBRID', 'idx', 'SEARCH', 'hello',
                   'VSIM', '@v', '$B', 'PARAMS', '2', 'B', qvec)
    raw_cmd = _resp(*hybrid_args)

    # Per-node addresses (FT.HYBRID is a coordinator command — send straight to
    # a node, which coordinates). getConnectionByEnv returns a cluster client.
    import redis as _redis
    nodes = [(n.host, n.port) for n in conn.get_primaries()]
    env.assertTrue(len(nodes) > 0)

    stop = threading.Event()
    errors = [0]

    def normal_hammer():
        host, port = random.choice(nodes)
        c = _redis.Redis(host=host, port=port)
        while not stop.is_set():
            try:
                c.execute_command(*hybrid_args)
            except Exception:
                errors[0] += 1
                try:
                    host, port = random.choice(nodes)
                    c = _redis.Redis(host=host, port=port)
                except Exception:
                    pass

    def drop_hammer():
        # Send FT.HYBRID on a raw socket, then close it mid-flight without
        # reading the reply -> coordinator sees the client vanish during the
        # query and tears the request down while shards are still replying.
        # Vary the delay widely so the drop lands across all phases (fan-out,
        # cursor-mapping, and the cursor-read batches where the UAF is suspected).
        while not stop.is_set():
            try:
                host, port = random.choice(nodes)
                s = socket.create_connection((host, port), timeout=2)
                s.sendall(raw_cmd)
                time.sleep(random.uniform(0, 0.025))
                s.close()
            except Exception:
                errors[0] += 1

    def chaos_timeout():
        # Cycle the global query timeout so the FAIL teardown fires at different
        # phases (a fixed tiny timeout always dies in mapping, never reaching the
        # cursor-read teardown path).
        vals = ['2', '5', '10', '20', '40', '80', '150']
        while not stop.is_set():
            try:
                run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-timeout',
                                          random.choice(vals))
            except Exception:
                pass
            time.sleep(0.05)

    threads = []
    ct = threading.Thread(target=chaos_timeout, daemon=True)
    ct.start()
    threads.append(ct)
    for k in range(NTHREADS):
        target = normal_hammer if (k % 2 == 0) else drop_hammer
        t = threading.Thread(target=target, daemon=True)
        t.start()
        threads.append(t)

    alive = True
    t0 = time.time()
    while time.time() - t0 < DURATION:
        time.sleep(2)
        try:
            conn.execute_command('PING')
        except Exception:
            alive = False
            break

    stop.set()
    for t in threads:
        t.join(timeout=5)

    env.assertTrue(
        alive,
        message='A shard became unreachable during the stress run — likely an '
                'ASAN crash (heap-use-after-free). Inspect the shard logs.')
