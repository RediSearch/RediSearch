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
