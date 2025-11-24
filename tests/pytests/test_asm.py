from common import *
from dataclasses import dataclass
from redis import Redis
import random
import re

@dataclass(frozen=True)
class SlotRange:
    start: int
    end: int

    @staticmethod
    def from_str(s: str):
        start, end = map(int, s.split("-"))
        assert 0 <= start <= end < 2**14
        return SlotRange(start, end)


@dataclass
class ClusterNode:
    id: str
    ip: str
    port: int
    cport: int  # cluster bus port
    hostname: str | None
    flags: set[str]
    master: str  # Either this node's primary replica or '-'
    ping_sent: int
    pong_recv: int
    config_epoch: int
    link_state: bool  # True: connected, False: disconnected
    slots: set[SlotRange]

    @staticmethod
    def from_str(s: str):
        # <id> <ip:port @cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-range> [<slot-range>> ...]
        # e.g. a5e5068caceb2adabed3ed657b21b627deadbfaa 127.0.0.1:6379 @16379 master - 0 1760353421847 1 connected 1000-2000 10000-15000
        parts = s.split()
        node_id, addr, flags, master, ping_sent, pong_recv, config_epoch, link_state, *slots = parts
        match = re.match(r"^(?P<ip>[^:]+):(?P<port>\d+)@(?P<cport>\d+)(?:,(?P<hostname>.+))?$", addr)
        ip = match.group("ip")
        port = int(match.group("port"))
        cport = int(match.group("cport"))
        hostname = match.group("hostname")

        return ClusterNode(
            id=node_id,
            ip=ip,
            port=port,
            cport=cport,
            hostname=hostname,
            flags=set(flags.split(",")),
            master=master,
            ping_sent=int(ping_sent),
            pong_recv=int(pong_recv),
            config_epoch=int(config_epoch),
            link_state=link_state == "connected",
            slots={SlotRange.from_str(s) for s in slots},
        )


def import_middle_slot_range(dest: Redis, source: Redis) -> str:

    def cluster_node_of(conn) -> ClusterNode:
        (result,) = (
            cluster_node
            for line in conn.execute_command("cluster", "nodes").splitlines()
            if "myself" in (cluster_node := ClusterNode.from_str(line)).flags
        )
        return result

    def middle_slot_range(slot_range: SlotRange) -> SlotRange:
        quarter = (slot_range.end - slot_range.start) // 4
        return SlotRange(slot_range.start + quarter, slot_range.end - quarter)

    source_node = cluster_node_of(source)

    slot_range = middle_slot_range(random.choice(list(source_node.slots)))

    return dest.execute_command('CLUSTER', 'MIGRATION', 'IMPORT', slot_range.start, slot_range.end)

def is_migration_complete(conn: Redis, task_id: str) -> bool:
    (migration_status,) = conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", task_id)
    return to_dict(migration_status)["state"] == "completed"

def wait_for_slot_import(conn: Redis, task_id: str, timeout: float = 20.0):
    with TimeLimit(timeout):
        while not is_migration_complete(conn, task_id):
            time.sleep(0.1)

cluster_node_timeout = 60_000 # in milliseconds (1 minute)

# @skip(cluster=False, min_shards=2)
@skip() # Flaky test, until we can guarantee no missing or duplicate results during slot migration
def test_import_slot_range(env: Env):
    n_docs = 2**14

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()

    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            con.execute_command('HSET', f'doc:{i}', 'n', i)

    shard1, shard2 = env.getConnection(1), env.getConnection(2)

    query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    expected = env.cmd(*query)

    shards = env.getOSSMasterNodesConnectionList()

    def query_all_shards():
        results = [shard.execute_command(*query) for shard in shards]
        for idx, res in enumerate(results, start=1):
            docs = res[1::2]
            dups = set(doc for doc in docs if docs.count(doc) > 1)
            env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate document IDs", depth=1)
            env.assertEqual(res, expected, message=f"shard {idx} returned unexpected results", depth=1)

    # Sanity check - all shards should return the same results
    query_all_shards()

    # Test searching while importing slots from shard 2 to shard 1
    with TimeLimit(30):
        task_id = import_middle_slot_range(shard1, shard2)
        while not is_migration_complete(shard1, task_id):
            query_all_shards()
            time.sleep(0.1)
    # And test again after the import is complete
    query_all_shards()

def import_slot_range_sanity_test(env: Env):
    n_docs = 2**14

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()

    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            con.execute_command('HSET', f'doc:{i}', 'n', i)

    shard1, shard2 = env.getConnection(1), env.getConnection(2)

    query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    expected = env.cmd(*query)

    shards = env.getOSSMasterNodesConnectionList()

    def query_all_shards():
        results = [shard.execute_command(*query) for shard in shards]
        for idx, res in enumerate(results, start=1):
            docs = res[1::2]
            dups = set(doc for doc in docs if docs.count(doc) > 1)
            env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate document IDs", depth=1)
            env.assertEqual(res, expected, message=f"shard {idx} returned unexpected results", depth=1)

    # Sanity check - all shards should return the same results
    query_all_shards()

    # Import slots from shard 2 to shard 1, and wait for it to complete
    task_id = import_middle_slot_range(shard1, shard2)
    wait_for_slot_import(shard1, task_id)
    wait_for_slot_import(shard2, task_id)

    # And test again after the import is complete
    query_all_shards()

@skip()
# @skip(cluster=False, min_shards=2)
def test_import_slot_range_sanity():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_sanity_test(env)

@skip()
# @skip(cluster=False, min_shards=2)
def test_import_slot_range_sanity_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_sanity_test(env)

def add_shard_and_migrate_test(env: Env):
    n_docs = 2**14
    initial_shards_count = env.shardsCount

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()

    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            con.execute_command('HSET', f'doc:{i}', 'n', i)

    shard1 = env.getConnection(1)

    query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    expected = env.cmd(*query)

    shards = env.getOSSMasterNodesConnectionList()

    def query_all_shards():
        results = [shard.execute_command(*query) for shard in shards]
        for idx, res in enumerate(results, start=1):
            docs = res[1::2]
            dups = set(doc for doc in docs if docs.count(doc) > 1)
            env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate document IDs", depth=1)
            env.assertEqual(res, expected, message=f"shard {idx} returned unexpected results", depth=1)

    # Sanity check - all shards should return the same results
    query_all_shards()

    # Add a new shard
    env.addShardToClusterIfExists()
    new_shard = env.getConnection(shardId=initial_shards_count+1)
    # ...and migrate slots from shard 1 to the new shard
    task = import_middle_slot_range(new_shard, shard1)
    wait_for_slot_import(new_shard, task)
    wait_for_slot_import(shard1, task)

    # Expect new shard to have the index schema
    env.assertEqual(new_shard.execute_command('FT._LIST'), ['idx'])

    # Make sure all cluster nodes are aware of the new shard (it was ignored until now, as it had no slots)
    env.waitCluster()

    # And expect all shards to return the same results, including the new one
    shards.append(new_shard)
    query_all_shards()

# @skip(cluster=False)
@skip()
def test_add_shard_and_migrate():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env)

# @skip(cluster=False)
@skip()
def test_add_shard_and_migrate_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    add_shard_and_migrate_test(env)

@skip(cluster=True)
def test_slots_info_errors(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()

    env.expect('_FT.SEARCH', 'idx', '*').error().contains('Internal query missing slots specification')
    env.expect('_FT.SEARCH', 'idx', '*', '_SLOTS_INFO', 'invalid_slots_data').error().contains('Failed to deserialize _SLOTS_INFO data')
    env.expect('_FT.SEARCH', 'idx', '*', '_SLOTS_INFO', generate_slots(), '_SLOTS_INFO', generate_slots()).error().contains('_SLOTS_INFO already specified')
