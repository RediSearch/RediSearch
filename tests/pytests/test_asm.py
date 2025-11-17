from common import *
from dataclasses import dataclass
from redis import Redis
import random
import re
import numpy as np

# Random words for generating more diverse text content
RANDOM_WORDS = [
    "apple", "banana", "cherry", "dragon", "elephant", "forest", "guitar", "harmony", "island", "jungle",
    "keyboard", "lighthouse", "mountain", "notebook", "ocean", "piano", "quantum", "rainbow", "sunset", "telescope",
    "umbrella", "volcano", "waterfall", "xylophone", "yacht", "zebra", "adventure", "butterfly", "cascade", "diamond",
    "emerald", "firefly", "galaxy", "horizon", "infinity", "jasmine", "kaleidoscope", "lavender", "melody", "nebula",
    "opal", "paradise", "quasar", "rhapsody", "symphony", "twilight", "universe", "velvet", "whisper", "xenon"
]

def query_shards(env, query, shards, expected, query_type: str = 'FT.SEARCH'):
    if query_type == 'FT.HYBRID':
        return query_shards_hybrid(env, query, shards, expected)
    else:
        return query_shards_original(env, query, shards, expected)

def query_shards_original(env, query, shards, expected):
    """Original query_shards implementation for non-HYBRID queries"""
    results = [shard.execute_command(*query) for shard in shards]
    for idx, res in enumerate(results, start=1):
        docs = res[1::2]
        dups = set(doc for doc in docs if docs.count(doc) > 1)
        env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate document IDs", depth=1)
        env.assertEqual(res, expected, message=f"shard {idx} returned unexpected results", depth=1)

def query_shards_hybrid(env, query, shards, expected):
    """Enhanced query_shards implementation for FT.HYBRID queries with simplified doc ID checking"""
    results = [shard.execute_command(*query) for shard in shards]

    # Helper function to extract document IDs from result
    def extract_doc_ids(result):
        doc_ids = []
        if isinstance(result, list) and len(result) >= 4 and result[0] == 'total_results' and result[2] == 'results':
            docs_list = result[3]
            if isinstance(docs_list, list):
                for doc_entry in docs_list:
                    if isinstance(doc_entry, list) and len(doc_entry) >= 2 and doc_entry[0] == '__key':
                        doc_ids.append(doc_entry[1])
        return doc_ids

    # Extract expected document IDs
    expected_doc_ids = extract_doc_ids(expected)
    print(f'expected_doc_ids: {expected_doc_ids}')

    for idx, res in enumerate(results, start=1):

        # Extract document IDs from this shard's result
        shard_doc_ids = extract_doc_ids(res)
        print(f"shard_doc_ids: {shard_doc_ids}")


        # Check for duplicates within this shard
        dups = set(doc_id for doc_id in shard_doc_ids if shard_doc_ids.count(doc_id) > 1)
        env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate document IDs within shard", depth=1)

        # Compare document IDs (ignoring order and other fields like execution_time)
        env.assertEqual(set(shard_doc_ids), set(expected_doc_ids), message=f"shard {idx} returned unexpected document IDs", depth=1)

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

def create_and_populate_index(env: Env, index_name: str, n_docs: int):
    """Create index with numeric, text, and vector fields and populate with test data"""
    # Create index with multiple field types including vector (using 10 dimensions)
    env.expect('FT.CREATE', index_name, 'SCHEMA',
               'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT',
               'tag', 'TAG',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '10', 'DISTANCE_METRIC', 'L2').ok()

    # Set random seed for reproducible vectors
    np.random.seed(42)

    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            # Generate a 10-dimensional vector with more variation to avoid same scores
            vector = np.array([
                float(i % 10), float((i * 2) % 10), float((i * 3) % 10),
                float((i * 5) % 10), float((i * 7) % 10), float((i * 11) % 10),
                float((i * 13) % 10), float((i * 17) % 10), float((i * 19) % 10),
                float((i * 23) % 10)
            ], dtype=np.float32)

            # Create more diverse text content with random words
            random.seed(i)  # Use document index as seed for reproducible randomness
            random_words = random.sample(RANDOM_WORDS, min(3, len(RANDOM_WORDS)))
            text_content = f"document {i} content {' '.join(random_words)} data"
            tag_value = "even" if i % 2 == 0 else "odd"

            con.execute_command('HSET', f'doc:{i}',
                              'n', i,
                              'text', text_content,
                              'tag', tag_value,
                              'vector', vector.tobytes())

cluster_node_timeout = 60_000 # in milliseconds (1 minute)

def import_slot_range_sanity_test(env: Env, query_type: str = 'FT.SEARCH'):
    n_docs = 2**14
    create_and_populate_index(env, 'idx', n_docs)

    shard1, shard2 = env.getConnection(1), env.getConnection(2)
    if query_type == 'FT.SEARCH':
        query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    elif query_type == 'FT.AGGREGATE':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n')
    elif query_type == 'FT.HYBRID':
        # Create a 10-dimensional query vector for hybrid search
        random_words_to_query = " ".join(random.sample(RANDOM_WORDS, min(3, len(RANDOM_WORDS))))
        query_vector = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], dtype=np.float32).tobytes()
        query = ('FT.HYBRID', 'idx',
                'SEARCH', f'@n:[69 1420] @text:({random_words_to_query})',
                'VSIM', '@vector', query_vector, 'KNN', '2', 'K', str(n_docs),
                'COMBINE', 'RRF', '2', 'CONSTANT', '60')
    expected = env.cmd(*query)

    shards = env.getOSSMasterNodesConnectionList()
    # Sanity check - all shards should return the same results
    query_shards(env, query, shards, expected, query_type)

    # Import slots from shard 2 to shard 1, and wait for it to complete
    task_id = import_middle_slot_range(shard1, shard2)
    wait_for_slot_import(shard1, task_id)
    wait_for_slot_import(shard2, task_id)

    # TODO ASM: When Trimming delay is implemented in core, test with all shards
    # For now, only the shards involved in the migration process will have their topology updated. If the query hits another shard as coordinator would fail, as their
    # topology is outdated, and the slots that would arrive to each shard would lead to errors.
    query_shards(env, query, [shard1, shard2], expected, query_type)

def import_slot_range_test(env: Env, query_type: str = 'FT.SEARCH'):
    n_docs = 2**14
    create_and_populate_index(env, 'idx', n_docs)

    if query_type == 'FT.SEARCH':
        query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    elif query_type == 'FT.AGGREGATE':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n')
    elif query_type == 'FT.HYBRID':
        # Create a 10-dimensional query vector for hybrid search
        random_words_to_query = " ".join(random.sample(RANDOM_WORDS, min(3, len(RANDOM_WORDS))))
        query_vector = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], dtype=np.float32).tobytes()
        query = ('FT.HYBRID', 'idx',
                'SEARCH', f'@n:[69 1420] @text:({random_words_to_query})',
                'VSIM', '@vector', query_vector, 'KNN', '2', 'K', str(n_docs),
                'COMBINE', 'RRF', '2', 'CONSTANT', '60')

    expected = env.cmd(*query)

    shards = env.getOSSMasterNodesConnectionList()
    shard1, shard2 = env.getConnection(1), env.getConnection(2)

    # Sanity check - all shards should return the same results
    query_shards(env, query, shards, expected, query_type)

    # Test searching while importing slots from shard 2 to shard 1
    with TimeLimit(30):
        task_id = import_middle_slot_range(shard1, shard2)
        while not is_migration_complete(shard1, task_id):
            query_shards(env, query, shards, expected, query_type)
            time.sleep(0.1)

    # TODO ASM: When Trimming delay is implemented in core, test with all shards
    # For now, only the shards involved in the migration process will have their topology updated. If the query hits another shard as coordinator would fail, as their
    # topology is outdated, and the slots that would arrive to each shard would lead to errors.
    query_shards(env, query, [shard1, shard2], expected, query_type)

@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_import_slot_range():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_import_slot_range_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_ft_hybrid_import_slot_range():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.HYBRID')

@skip(cluster=False, min_shards=2)
def test_ft_hybrid_import_slot_range_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.HYBRID')

@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range_sanity():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_sanity_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range_sanity_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_sanity_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_import_slot_range_sanity():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_sanity_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_import_slot_range_sanity_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_sanity_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_ft_hybrid_import_slot_range_sanity():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_sanity_test(env, 'FT.HYBRID')

@skip(cluster=False, min_shards=2)
def test_ft_hybrid_import_slot_range_sanity_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_sanity_test(env, 'FT.HYBRID')

def add_shard_and_migrate_test(env: Env, query_type: str = 'FT.SEARCH'):
    initial_shards_count = env.shardsCount
    n_docs = 2**14
    create_and_populate_index(env, 'idx', n_docs)

    shard1 = env.getConnection(1)

    if query_type == 'FT.SEARCH':
        query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    elif query_type == 'FT.AGGREGATE':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n')
    elif query_type == 'FT.HYBRID':
        # Create a 10-dimensional query vector for hybrid search
        random_words_to_query = " ".join(random.sample(RANDOM_WORDS, min(3, len(RANDOM_WORDS))))
        query_vector = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], dtype=np.float32).tobytes()
        query = ('FT.HYBRID', 'idx',
                'SEARCH', f'@n:[69 1420] @text:({random_words_to_query})',
                'VSIM', '@vector', query_vector, 'KNN', '2', 'K', str(n_docs),
                'COMBINE', 'RRF', '2', 'CONSTANT', '60')
    expected = env.cmd(*query)

    # Sanity check - all shards should return the same results
    shards = env.getOSSMasterNodesConnectionList()
    query_shards(env, query, shards, expected, query_type)

    # Add a new shard
    env.addShardToClusterIfExists()
    new_shard = env.getConnection(shardId=initial_shards_count+1)
    # ...and migrate slots from shard 1 to the new shard
    task = import_middle_slot_range(new_shard, shard1)
    wait_for_slot_import(new_shard, task)
    wait_for_slot_import(shard1, task)

    # Expect new shard to have the index schema
    env.assertEqual(new_shard.execute_command('FT._LIST'), ['idx'])

    # TODO ASM: When Trimming delay is implemented in core, test with all shards
    # For now, only the shards involved in the migration process will have their topology updated. If the query hits another shard as coordinator would fail, as their
    # topology is outdated, and the slots that would arrive to each shard would lead to errors.
    query_shards(env, query, [shard1, new_shard], expected, query_type)

@skip(cluster=False)
def test_add_shard_and_migrate():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env, 'FT.SEARCH')

@skip(cluster=False)
def test_add_shard_and_migrate_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    add_shard_and_migrate_test(env, 'FT.SEARCH')

@skip(cluster=False)
def test_add_shard_and_migrate_aggregate():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env, 'FT.AGGREGATE')

@skip(cluster=False)
def test_add_shard_and_migrate_aggregate_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    add_shard_and_migrate_test(env, 'FT.AGGREGATE')

@skip(cluster=False)
def test_add_shard_and_migrate_hybrid():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env, 'FT.HYBRID')

@skip(cluster=False)
def test_add_shard_and_migrate_hybrid_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    add_shard_and_migrate_test(env, 'FT.HYBRID')

@skip(cluster=True)
def test_slots_info_errors(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()

    env.expect('_FT.SEARCH', 'idx', '*').error().contains('Internal query missing slots specification')
    env.expect('_FT.SEARCH', 'idx', '*', '_SLOTS_INFO', 'invalid_slots_data').error().contains('Failed to deserialize _SLOTS_INFO data')
    env.expect('_FT.SEARCH', 'idx', '*', '_SLOTS_INFO', generate_slots(range(0, 0)), '_SLOTS_INFO', generate_slots(range(0, 0))).error().contains('_SLOTS_INFO already specified')
