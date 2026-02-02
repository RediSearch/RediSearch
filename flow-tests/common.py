from pytest_bdd import given, when, then, parsers
import os
import time
from pathlib import Path

def configure_search_cluster_single_shard(conn, port):
    """
    Configure Redisearch cluster settings, to run as a single-shard cluster.
    """    

    # Execute SEARCH.CLUSTERSET command to configure the cluster
    # MYID 1: Set this node's ID to 1
    # RANGES 1: Number of shard ranges
    # SHARD 1: Shard ID
    # SLOTRANGE 0 16383: This shard handles all hash slots (0-16383)
    # ADDR password@127.0.0.1:port: Address of this node
    # MASTER: This node is a master
    conn.execute_command(
        'SEARCH.CLUSTERSET',
        'MYID', '1',
        'RANGES', '1',
        'SHARD', '1',
        'SLOTRANGE', '0', '16383',
        'ADDR', f'password@127.0.0.1:{port}',
        'MASTER'
    )

def waitForRdbSaveToFinish(env):
    if env.isCluster():
        conns = env.getOSSMasterNodesConnectionList()
    else:
        conns = [env.getConnection()]

    # Busy wait until all connection are done rdb bgsave
    check_bgsave = True
    while check_bgsave:
        check_bgsave = False
        for conn in conns:
            if conn.execute_command('info', 'Persistence')['rdb_bgsave_in_progress']:
                check_bgsave = True
                break

def waitForIndex(env, idx = 'idx'):
    waitForRdbSaveToFinish(env)
    while True:
        res = env.cmd('ft.info', idx)
        try:
            if res[res.index('indexing') + 1] == 0:
                break
        except:
            # RESP3
            if res['indexing'] == 0:
                break
        time.sleep(0.1)
