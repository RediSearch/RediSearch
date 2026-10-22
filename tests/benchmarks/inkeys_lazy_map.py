# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Measure first-use backfill using the accompanying temporary timing patch."""

import argparse, json, pathlib, re, socket, subprocess, threading, time
import redis


def percentiles(samples):
    values = sorted(samples)
    if not values:
        return {'count': 0}
    return {'count': len(values), **{name: values[min(len(values)-1, int(len(values)*q))]
            for name, q in [('p50_ms', .5), ('p99_ms', .99), ('max_ms', 1)]}}


def run(args, case, workers, writes, repeat):
    n, rounds, cap = {'dense': (1000000, 1, 1000000),
                      'churn': (100000, 20, 1000000),
                      'chains': (200000, 1, 1000)}[case]
    directory = pathlib.Path(args.output) / f'{case}-w{workers}-write{int(writes)}-r{repeat}'
    directory.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    logfile = open(directory / 'redis.log', 'w')
    process = subprocess.Popen([args.redis, '--port', str(port), '--bind', '127.0.0.1',
        '--save', '', '--appendonly', 'no', '--dir', str(directory.resolve()),
        '--enable-debug-command', 'yes',
        '--loadmodule', args.module, 'WORKERS', str(workers), 'TIMEOUT', '0',
        'MAXDOCTABLESIZE', str(cap), 'NOGC'], stdout=logfile, stderr=subprocess.STDOUT)
    conn = redis.Redis(port=port, decode_responses=True, socket_timeout=120)
    owned = False
    errors = []
    stop = threading.Event()
    threads = []
    try:
        for _ in range(1000):
            if process.poll() is not None:
                raise RuntimeError((directory / 'redis.log').read_text())
            try:
                if conn.info('server')['process_id'] != process.pid:
                    raise RuntimeError('Port belongs to another process')
                owned = True
                break
            except redis.ConnectionError: time.sleep(.01)
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'SORTABLE')
        for generation in range(rounds):
            for start in range(0, n, 1000):
                pipe = conn.pipeline(transaction=False)
                for i in range(start, min(start+1000, n)):
                    pipe.hset(f'doc:{i}', mapping={'t': 'hello', 'n': generation})
                pipe.execute()
        info = dict(zip(*(iter(conn.execute_command('FT.INFO', 'idx')),)*2))
        assert float(info['key_table_size_mb']) == 0, info
        assert int(info['num_docs']) == n, info
        gate = threading.Event()
        phase = ['before']
        samples = {p: {'query': [], 'write': []} for p in ['before', 'building', 'ready']}
        first = []

        def workload(kind):
            client = redis.Redis(port=port, decode_responses=True, socket_timeout=120)
            count = 0
            try:
                if kind == 'query': gate.wait()
                while not stop.is_set():
                    p = phase[0]
                    begin = time.perf_counter_ns()
                    if kind == 'query':
                        result = client.execute_command('FT.SEARCH', 'idx', 'hello', 'INKEYS', 1,
                                                        'doc:0', 'NOCONTENT')
                        assert result == [1, 'doc:0'], result
                    else:
                        client.hset(f'doc:{count % n}', 'n', rounds + count + 1)
                    ms = (time.perf_counter_ns()-begin)/1e6
                    samples[p][kind].append(ms)
                    if kind == 'query' and not first: first.append(ms)
                    count += 1
            except Exception as e:
                errors.append(repr(e))
                stop.set()
            finally:
                client.close()

        if writes:
            threads.append(threading.Thread(target=workload, args=('write',)))
            threads[-1].start()
        time.sleep(.3)
        phase[0] = 'building'
        threads.append(threading.Thread(target=workload, args=('query',)))
        threads[-1].start()
        begin = time.perf_counter()
        gate.set()
        match = None
        while time.perf_counter()-begin < 120:
            text = (directory / 'redis.log').read_text()
            match = re.search(r'KEYMAP_BENCH elapsed_ns=(\d+) max_hold_ns=(\d+) total_hold_ns=(\d+) batches=(\d+)', text)
            if match or errors: break
            time.sleep(.002)
        assert match, (errors, text[-2000:])
        phase[0] = 'ready'
        time.sleep(.3)
        stop.set()
        for t in threads: t.join(125)
        assert not errors, errors
        assert not any(t.is_alive() for t in threads)
        result = {'case': case, 'workers': workers, 'writes': writes, 'repeat': repeat,
                  'live_docs': n, 'history_ids': n*rounds, 'max_doctable_size': cap,
                  'first_query_ms': first[0],
                  **{k: int(v)/(1e6 if k.endswith('_ms') else 1) for k,v in
                     zip(['build_ms', 'max_hold_ms', 'total_hold_ms', 'batches'], match.groups())},
                  'latency': {p: {k: percentiles(v) for k,v in series.items()}
                              for p, series in samples.items()}}
        (directory / 'result.json').write_text(json.dumps(result, indent=2)+'\n')
        return result
    finally:
        stop.set()
        for t in threads: t.join(2)
        if owned:
            try: conn.shutdown(nosave=True)
            except redis.ConnectionError: pass
        try: process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=10)
        logfile.close()
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--module', required=True)
    parser.add_argument('--redis', default='redis-server')
    parser.add_argument('--output', required=True)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--cases', nargs='+', default=['dense', 'churn', 'chains'])
    args = parser.parse_args()
    pathlib.Path(args.output).mkdir(parents=True, exist_ok=True)
    for repeat in range(args.repeats):
        for case in args.cases:
            for workers in (0, 2):
                for writes in (False, True):
                    print(json.dumps(run(args, case, workers, writes, repeat)), flush=True)
