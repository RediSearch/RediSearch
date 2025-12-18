import multiprocessing
import time
import redis
import sys
import argparse
from urlparse import urlparse
import os
from collections import defaultdict
import math

def ping(r):
    r.ping()

def jsonset(r):
    r.execute_command('JSON.SET', 'j', '.', '{}')

def runWorker(ctx):
    wpid = os.getpid()
    print '{} '.format(wpid),
    sys.stdout.flush()

    rep = defaultdict(int)
    r = redis.StrictRedis(host=ctx['host'], port=ctx['port'])
    work = ctx['work']
    if ctx['pipeline'] == 0:
        for i in range(0, ctx['count']):
            s0 = time.time()
            work(r)
            s1 = time.time() - s0
            bin = int(math.floor(s1 * 1000)) + 1
            rep[bin] += 1
    else:        
        for i in range(0, ctx['count'], ctx['pipeline']):
            p = r.pipeline()
            s0 = time.time()
            for j in range(0, ctx['pipeline']):
                work(p)
            p.execute()
            s1 = time.time() - s0
            bin = int(math.floor(s1 * 1000)) + 1
            rep[bin] += ctx['pipeline']

    return rep

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ReJSON Benchmark', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--count', type=int, default=100000, help='total number of operations')
    parser.add_argument('-p', '--pipeline', type=int, default=0, help='pipeline size')
    parser.add_argument('-w', '--workers', type=int, default=8, help='number of worker processes')
    parser.add_argument('-u', '--uri', type=str, default='redis://localhost:6379', help='Redis server URI')
    args = parser.parse_args()
    uri = urlparse(args.uri)

    r = redis.Redis(host=uri.hostname, port=uri.port)

    pool = multiprocessing.Pool(args.workers)
    s0 = time.time()
    ctx = {
        'count': args.count / args.workers,
        'pipeline': args.pipeline,
        'host': uri.hostname,
        'port': uri.port,
        'work': jsonset,
    }

    print 'Starting workers: ',
    sys.stdout.flush()
    results = pool.map(runWorker, (ctx, ) * args.workers)
    print
    sys.stdout.flush()

    s1 = time.time() - s0
    agg = defaultdict(int)
    for res in results:
        for k, v in res.iteritems():
            agg[k] += v

    print
    print 'Count: {}, Workers: {}, Pipeline: {}'.format(args.count, args.workers, args.pipeline)
    print 'Using hireds: {}'.format(redis.utils.HIREDIS_AVAILABLE)
    print 'Runtime: {} seconds'.format(round(s1, 2))
    print 'Throughput: {} requests per second'.format(round(args.count/s1, 2))
    for k, v in sorted(agg.items()):
        perc = 100.0 * v / args.count
        print '{}% <= {} milliseconds'.format(perc, k)
    
