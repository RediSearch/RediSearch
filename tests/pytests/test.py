# -*- coding: utf-8 -*-

from common import *
import redis
from hotels import hotels
import random
import time
import unittest

def cluster_set_test(env: Env):
    def verify_address(addr):
        try:
            with TimeLimit(10, f'Failed waiting cluster set command to be updated with the new IP address `{addr}`'):
                while env.cmd('SEARCH.CLUSTERINFO')[9][2][1] != addr:
                    pass
        except Exception as e:
            env.assertTrue(False, message=str(e))

    def prepare_env(env):
        # set validation timeout to 5ms so occasionaly we will fail to validate the cluster,
        # this is to test the timeout logic, and help us with ipv6 addresses in containers
        # where the ipv6 address is not available by default
        env.cmd(config_cmd(), 'SET', 'TOPOLOGY_VALIDATION_TIMEOUT', 5)
        env.cmd(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER')
        verify_shard_init(env)

    password = env.password + "@" if env.password else ""

    # test ipv4
    prepare_env(env)
    env.expect('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '16383',
               'ADDR',
               f'{password}127.0.0.1:{env.port}',
               'MASTER'
            ).ok()
    verify_address('127.0.0.1')

    env.stop()
    env.start()

    # test ipv6 test
    prepare_env(env)
    env.expect('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'HASHFUNC',
               'CRC16',
               'NUMSLOTS',
               '16384',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '16383',
               'ADDR',
               f'{password}[::1]:{env.port}',
               'MASTER'
            ).ok()
    verify_address('::1')

    env.stop()
    env.start()

    # test unix socket
    prepare_env(env)
    env.expect('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'HASHFUNC',
               'CRC12',
               'NUMSLOTS',
               '4096',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '4095',
               'ADDR',
               f'{password}localhost:{env.port}',
               'UNIXADDR',
               '/tmp/redis.sock',
               'MASTER'
            ).ok()
    verify_address('localhost')

    shards = []
    for i in range(env.shardsCount):
        shards += ['SHARD', str(i), 'SLOTRANGE', '0', '16383',
                   'ADDR', f'{password}localhost:{env.envRunner.shards[i].port}', 'MASTER']
    env.expect('SEARCH.CLUSTERSET', 'MYID', '0', 'RANGES', str(env.shardsCount), *shards).ok()

try:
    skipTest(cluster=False)
    for i in range(1, 1001):
        globals()[f'test_cluster_set_{i}'] = lambda env: cluster_set_test(env)
except SkipTest:
    pass
