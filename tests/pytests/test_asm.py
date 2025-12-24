from common import *
from dataclasses import dataclass
from redis import Redis
import random
import re
import numpy as np
import threading
import time
import os
import signal
import psutil

# Random words for generating more diverse text content
RANDOM_WORDS = [
    "apple", "banana", "cherry", "dragon", "elephant", "forest", "guitar", "harmony", "island", "jungle",
    "keyboard", "lighthouse", "mountain", "notebook", "ocean", "piano", "quantum", "rainbow", "sunset", "telescope",
    "umbrella", "volcano", "waterfall", "xylophone", "yacht", "zebra", "adventure", "butterfly", "cascade", "diamond",
    "emerald", "firefly", "galaxy", "horizon", "infinity", "jasmine", "kaleidoscope", "lavender", "melody", "nebula",
    "opal", "paradise", "quasar", "rhapsody", "symphony", "twilight", "universe", "velvet", "whisper", "xenon"
]

def get_expected(env, query, query_type: str = 'FT.SEARCH', protocol=2):
    if query_type == 'FT.AGGREGATE.WITHCURSOR':
        expected = []
        cursor_result = env.cmd(*query)
        cursor_id = cursor_result[1]
        while cursor_id != 0:
            res, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', 10)
            if protocol == 2:
              expected.extend(res)  # Skip the count
            else:
              expected.append(res)
        return expected
    else:
        return env.cmd(*query)

def query_shards(env, query, shards, expected, query_type: str = 'FT.SEARCH'):
    if query_type == 'FT.HYBRID':
        return query_shards_hybrid(env, query, shards, expected)
    elif query_type == 'FT.AGGREGATE':
        return query_shards_ft_aggregate(env, query, shards, expected)
    elif query_type == 'FT.AGGREGATE.WITHCURSOR':
        return query_shards_ft_aggregate_withcursor(env, query, shards, expected)
    else:
        return query_shards_ft_search(env, query, shards, expected)


def query_shards_ft_search(env, query, shards, expected):
    """Original query_shards implementation for FT.SEARCH queries"""
    results = [shard.execute_command(*query) for shard in shards]
    for idx, res in enumerate(results):
        docs = res[1::2]
        dups = set(doc for doc in docs if docs.count(doc) > 1)
        env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate document IDs", depth=1)
        env.assertEqual(res, expected, message=f"shard {idx} returned unexpected results", depth=1)

def extract_values(result):
    values = []
    if isinstance(result, list) and len(result) >= 2:
        for entry in result[1:]:  # Skip the count
            if isinstance(entry, list) and len(entry) >= 2:
                # For entries like ['n', '69'], extract the value '69'
                values.append(entry[1])
    return values

def query_shards_ft_aggregate(env, query, shards, expected):
    """Query_shards implementation for FT.AGGREGATE queries"""
    results = [shard.execute_command(*query) for shard in shards]
    for idx, res in enumerate(results):
        # Extract values from aggregation results
        values = extract_values(res)

        # Check for duplicate values in aggregation results
        dups = set(value for value in values if values.count(value) > 1)
        env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate values in aggregation results", depth=1)
        env.assertEqual(res, expected, message=f"shard {idx} returned unexpected results", depth=1)

def query_shards_ft_aggregate_withcursor(env, query, shards, expected):
    """Query_shards implementation for FT.AGGREGATE queries with cursor"""
    results = [shard.execute_command(*query) for shard in shards]
    for idx, res in enumerate(results):
        # Extract values from aggregation results
        full_result = []
        shard = shards[idx]
        cursor_id = res[1]
        while cursor_id != 0:
            res, cursor_id = shard.execute_command('FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', 10)
            full_result.extend(res)
        values = extract_values(full_result)
        dups = set(value for value in values if values.count(value) > 1)
        env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate values in aggregation results", depth=1)
        env.assertEqual(full_result, expected, message=f"shard {idx} returned unexpected results", depth=1)


def query_shards_hybrid(env, query, shards, expected):
    """Enhanced query_shards implementation for FT.HYBRID queries with score order checking"""
    results = [shard.execute_command(*query) for shard in shards]

    # Helper function to extract scores from result (in order)
    def extract_scores(result):
        scores = []
        if isinstance(result, list) and len(result) >= 4 and result[0] == 'total_results' and result[2] == 'results':
            docs_list = result[3]
            if isinstance(docs_list, list):
                for doc_entry in docs_list:
                    if isinstance(doc_entry, list) and len(doc_entry) >= 4 and doc_entry[0] == '__key' and doc_entry[2] == '__score':
                        scores.append(doc_entry[3])
        return scores

    # Helper function to extract document IDs from result (for duplicate checking)
    def extract_doc_ids(result):
        doc_ids = []
        if isinstance(result, list) and len(result) >= 4 and result[0] == 'total_results' and result[2] == 'results':
            docs_list = result[3]
            if isinstance(docs_list, list):
                for doc_entry in docs_list:
                    if isinstance(doc_entry, list) and len(doc_entry) >= 2 and doc_entry[0] == '__key':
                        doc_ids.append(doc_entry[1])
        return doc_ids

    # Extract expected scores (in order)
    expected_scores = extract_scores(expected)

    for idx, res in enumerate(results):
        # Extract scores from this shard's result (in order)
        shard_scores = extract_scores(res)

        # Extract document IDs for duplicate checking
        shard_doc_ids = extract_doc_ids(res)

        # Check for duplicates within this shard
        dups = set(doc_id for doc_id in shard_doc_ids if shard_doc_ids.count(doc_id) > 1)
        env.assertEqual(dups, set(), message=f"shard {idx} returned {len(dups)} duplicate document IDs within shard", depth=1)

        # Compare scores in order (this ensures ranking consistency)
        env.assertEqual(shard_scores, expected_scores, message=f"shard {idx} returned unexpected score order", depth=1)

class TaskIDFailed(Exception):
    pass

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


def get_shard_pid(conn):
    """Get the PID of a Redis shard connection"""
    return conn.execute_command('info', 'server')['process_id']

def get_all_shards_pids(env):
    """Get PIDs from all environment shards"""
    pids = []
    for shard_conn in env.getOSSMasterNodesConnectionList():
        pid = get_shard_pid(shard_conn)
        pids.append(pid)
    return pids

def get_child_pids(parent_pid):
    """Get all child PIDs of a given parent PID"""
    try:
        parent = psutil.Process(parent_pid)
        children = parent.children(recursive=True)
        return [child.pid for child in children]
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return []

def send_sigabrt_to_children_and_parents(parent_pids):
    """Send SIGABRT signal to all children of the given parent PIDs and to the parents themselves"""
    if not parent_pids:
        print("No parent PIDs provided, skipping SIGABRT")
        return

    total_children_killed = 0
    total_parents_killed = 0

    # First, send SIGABRT to all child processes
    for parent_pid in parent_pids:
        try:
            child_pids = get_child_pids(parent_pid)
            if not child_pids:
                print(f"No child processes found for parent PID {parent_pid}")
            else:
                print(f"Found {len(child_pids)} child processes for parent PID {parent_pid}: {child_pids}")
                for child_pid in child_pids:
                    try:
                        # Verify the process still exists before sending signal
                        if psutil.pid_exists(child_pid):
                            os.kill(child_pid, signal.SIGABRT)
                            print(f"Sent SIGABRT to child process {child_pid} of parent {parent_pid}")
                            total_children_killed += 1
                        else:
                            print(f"Child process {child_pid} no longer exists")
                    except (OSError, ProcessLookupError) as e:
                        print(f"Failed to send SIGABRT to child process {child_pid}: {e}")
        except Exception as e:
            print(f"Error processing children of parent PID {parent_pid}: {e}")

    # Then, send SIGABRT to the parent processes themselves
    for parent_pid in parent_pids:
        try:
            # Verify the parent process still exists before sending signal
            if psutil.pid_exists(parent_pid):
                os.kill(parent_pid, signal.SIGABRT)
                print(f"Sent SIGABRT to parent process {parent_pid}")
                total_parents_killed += 1
            else:
                print(f"Parent process {parent_pid} no longer exists")
        except (OSError, ProcessLookupError) as e:
            print(f"Failed to send SIGABRT to parent process {parent_pid}: {e}")
        except Exception as e:
            print(f"Error processing parent PID {parent_pid}: {e}")

    print(f"Total child processes sent SIGABRT: {total_children_killed}")
    print(f"Total parent processes sent SIGABRT: {total_parents_killed}")

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
    if (to_dict(migration_status)["state"] == "failed"):
        raise TaskIDFailed(f"Migration failed: {migration_status}")
    return to_dict(migration_status)["state"] == "completed"

def create_and_populate_index(env: Env, index_name: str, n_docs: int):
    """Create index with numeric, text, and vector fields and populate with test data"""
    # Create index with multiple field types including vector (using 10 dimensions)
    # Also include fields that will be updated by parallel update threads
    env.expect('FT.CREATE', index_name, 'SCHEMA',
               'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT',
               'tag', 'TAG',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '10', 'DISTANCE_METRIC', 'L2',
               'update_counter', 'NUMERIC',
               'timestamp', 'NUMERIC',
               'extra_data', 'TEXT').ok()

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
            # force each document to a different slot
            con.execute_command('HSET', f'doc-{i}:{{{i % 2**14}}}',
                              'n', i,
                              'text', text_content,
                              'tag', tag_value,
                              'vector', vector.tobytes(),
                              'update_counter', 0,
                              'timestamp', int(time.time() * 1000),
                              'extra_data', f'initial_{i}')

def wait_for_migration_complete(env, dest_shard, source_shard, timeout=300, query_during_migration=None):
    """Helper to wait for slot migration to complete with retry on failure

    Args:
        env: Test environment
        dest_shard: Destination shard connection
        source_shard: Source shard connection
        timeout: Timeout in seconds
        query_during_migration: Optional dict with keys 'query', 'shards', 'expected', 'query_type'
                               to run queries during migration
    """
    task_id = import_middle_slot_range(dest_shard, source_shard)

    # Get all shard PIDs before starting the timeout
    shard_pids = get_all_shards_pids(env)
    env.debugPrint(f"Shard PIDs: {shard_pids}")

    try:
        with TimeLimit(timeout):
            while True:
                try:
                    if query_during_migration:
                        # Pattern with queries during migration
                        while not is_migration_complete(dest_shard, task_id):
                            env.debugPrint("Querying shards while migration is in progress")
                            query_shards(env, query_during_migration['query'],
                                       query_during_migration['shards'],
                                       query_during_migration['expected'],
                                       query_during_migration['query_type'])
                            env.debugPrint("Query passed")
                            time.sleep(0.001)
                    else:
                        # Original pattern checking both shards
                        while not is_migration_complete(dest_shard, task_id) or not is_migration_complete(source_shard, task_id):
                            time.sleep(0.1)
                    break  # Exit outer loop when migration completes successfully
                except TaskIDFailed as e:
                    print(f"Task failed: {e}")
                    time.sleep(0.1)
    except Exception as e:
        # Check if this is a timeout exception from TimeLimit
        # TimeLimit raises Exception with message starting with 'Timeout:'
        print(f"TimeLimit timeout occurred: {e}")
        print(f"Detected timeout with shard PIDs: {shard_pids}")
        print("Sending SIGABRT to child processes and parent processes of all shards")
        send_sigabrt_to_children_and_parents(shard_pids)
        print("SIGABRT signals sent to child and parent processes")
        # Re-raise the exception to maintain original behavior
        raise

cluster_node_timeout = 60_000 # in milliseconds (1 minute)

def import_slot_range_sanity_test(env: Env, query_type: str = 'FT.SEARCH'):
    n_docs = 5 * 2**14
    create_and_populate_index(env, 'idx', n_docs)

    shard1, shard2 = env.getConnection(1), env.getConnection(2)
    if query_type == 'FT.SEARCH':
        query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    elif query_type == 'FT.AGGREGATE':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n')
    elif query_type == 'FT.AGGREGATE.WITHCURSOR':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n', 'WITHCURSOR', 'COUNT', 10)
    elif query_type == 'FT.HYBRID':
        # Create a 10-dimensional query vector for hybrid search
        random_words_to_query = " ".join(random.sample(RANDOM_WORDS, min(3, len(RANDOM_WORDS))))
        query_vector = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], dtype=np.float32).tobytes()
        query = ('FT.HYBRID', 'idx',
                'SEARCH', f'@n:[69 1420] @text:({random_words_to_query})',
                'VSIM', '@vector', '$BLOB', 'KNN', '2', 'K', str(n_docs),
                'COMBINE', 'RRF', '2', 'CONSTANT', '60', 'PARAMS', 2, 'BLOB', query_vector)

    expected = get_expected(env, query, query_type)

    shards = env.getOSSMasterNodesConnectionList()
    # Sanity check - all shards should return the same results
    query_shards(env, query, shards, expected, query_type)

    # Import slots from shard 2 to shard 1, and wait for it to complete
    wait_for_migration_complete(env, shard1, shard2)

    # Run query_shards for 5 seconds
    start_time = time.time()
    while time.time() - start_time < 5:
        query_shards(env, query, shards, expected, query_type)
        time.sleep(0.1)

def parallel_update_worker(env, n_docs, stop_event):
    """Worker function that continuously updates documents and forces GC"""
    with env.getClusterConnectionIfNeeded() as con:
        update_counter = 0
        while not stop_event.is_set():
            # Update some unrelated fields that are not part of the query
            # We'll add a new field 'update_counter' and 'timestamp' that won't affect search results
            doc_id = random.randint(0, n_docs - 1)
            key = f'doc-{doc_id}:{{{doc_id % 2**14}}}'

            # Update fields that are not queried in the test
            con.execute_command('HSET', key,
                              'update_counter', update_counter,
                              'timestamp', int(time.time() * 1000),
                              'extra_data', f'updated_{update_counter}')

            update_counter += 1

            # Force GC collection periodically (every 100 updates)
            if update_counter % 100 == 0:
                forceInvokeGC(env)

            # Small sleep to avoid overwhelming the system
            time.sleep(0.1)

def import_slot_range_test(env: Env, query_type: str = 'FT.SEARCH', parallel_updates: bool = False):
    n_docs = 5 * 2**14
    create_and_populate_index(env, 'idx', n_docs)

    if query_type == 'FT.SEARCH':
        query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    elif query_type == 'FT.AGGREGATE':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n')
    elif query_type == 'FT.AGGREGATE.WITHCURSOR':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n', 'WITHCURSOR', 'COUNT', 10)
    elif query_type == 'FT.HYBRID':
        # Create a 10-dimensional query vector for hybrid search
        random_words_to_query = " ".join(random.sample(RANDOM_WORDS, min(3, len(RANDOM_WORDS))))
        query_vector = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], dtype=np.float32).tobytes()
        query = ('FT.HYBRID', 'idx',
                'SEARCH', f'@n:[69 1420] @text:({random_words_to_query})',
                'VSIM', '@vector', '$BLOB', 'KNN', '2', 'K', str(n_docs),
                'COMBINE', 'RRF', '2', 'CONSTANT', '60', 'PARAMS', 2, 'BLOB', query_vector)

    expected = get_expected(env, query, query_type)

    shards = env.getOSSMasterNodesConnectionList()
    shard1, shard2 = env.getConnection(1), env.getConnection(2)

    # Sanity check - all shards should return the same results
    env.debugPrint("Querying all shards for sanity check")
    query_shards(env, query, shards, expected, query_type)
    env.debugPrint("Sanity check passed")

    update_thread = None
    stop_event = None
    if parallel_updates:
        # Start background threads that will keep doing updates and forcing GC
        env.debugPrint("Starting parallel update thread")
        stop_event = threading.Event()
        update_thread = threading.Thread(target=parallel_update_worker,
                                  args=(env, n_docs, stop_event),
                                  name=f"UpdateWorker")
        update_thread.daemon = True
        update_thread.start()

        time.sleep(0.5)

    # Test searching while importing slots from shard 2 to shard 1
    wait_for_migration_complete(env, shard1, shard2, query_during_migration={'query': query, 'shards': shards, 'expected': expected, 'query_type': query_type})
    env.debugPrint("Querying shards after migration")
    # Run query_shards for 5 seconds
    start_time = time.time()
    while time.time() - start_time < 5:
        query_shards(env, query, shards, expected, query_type)
        time.sleep(0.1)
    env.debugPrint("Query after migration passed")

    if update_thread:
        # Stop and join the update threads
        env.debugPrint("Stopping parallel update thread")
        stop_event.set()
        update_thread.join(timeout=30.0)  # Wait up to 30 seconds for the thread to stop
        env.debugPrint("Parallel update thread stopped")

@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
@skip
def test_ft_aggregate_import_slot_range():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_import_slot_range_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_withcursor_import_slot_range():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.AGGREGATE.WITHCURSOR')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_withcursor_import_slot_range_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.AGGREGATE.WITHCURSOR')

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
def test_ft_hybrid_import_slot_range():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.HYBRID')

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
def test_ft_hybrid_import_slot_range_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.HYBRID')

# Tests with parallel updates enabled
@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range_parallel_updates():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.SEARCH', parallel_updates=True)

@skip(cluster=False, min_shards=2)
def test_ft_search_import_slot_range_parallel_updates_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.SEARCH', parallel_updates=True)

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_import_slot_range_parallel_updates():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.AGGREGATE', parallel_updates=True)

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_import_slot_range_parallel_updates_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.AGGREGATE', parallel_updates=True)

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_withcursor_import_slot_range_parallel_updates():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.AGGREGATE.WITHCURSOR', parallel_updates=True)

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_withcursor_import_slot_range_parallel_updates_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.AGGREGATE.WITHCURSOR', parallel_updates=True)

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
def test_ft_hybrid_import_slot_range_parallel_updates():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_test(env, 'FT.HYBRID', parallel_updates=True)

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
def test_ft_hybrid_import_slot_range_parallel_updates_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_test(env, 'FT.HYBRID', parallel_updates=True)

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
def test_ft_aggregate_withcursor_import_slot_range_sanity():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_sanity_test(env, 'FT.AGGREGATE.WITHCURSOR')

@skip(cluster=False, min_shards=2)
def test_ft_aggregate_withcursor_import_slot_range_sanity_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_sanity_test(env, 'FT.AGGREGATE.WITHCURSOR')

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
def test_ft_hybrid_import_slot_range_sanity():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    import_slot_range_sanity_test(env, 'FT.HYBRID')

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
def test_ft_hybrid_import_slot_range_sanity_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    import_slot_range_sanity_test(env, 'FT.HYBRID')

def add_shard_and_migrate_test(env: Env, query_type: str = 'FT.SEARCH'):
    initial_shards_count = env.shardsCount
    n_docs = 5 * 2**14
    create_and_populate_index(env, 'idx', n_docs)

    shard1 = env.getConnection(1)

    if query_type == 'FT.SEARCH':
        query = ('FT.SEARCH', 'idx', '@n:[69 1420]', 'SORTBY', 'n', 'LIMIT', 0, n_docs, 'RETURN', 1, 'n')
    elif query_type == 'FT.AGGREGATE':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n')
    elif query_type == 'FT.AGGREGATE.WITHCURSOR':
        query = ('FT.AGGREGATE', 'idx', '@n:[69 1420]', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, n_docs, 'LOAD', 1, 'n', 'WITHCURSOR', 'COUNT', 10)
    elif query_type == 'FT.HYBRID':
        # Create a 10-dimensional query vector for hybrid search
        random_words_to_query = " ".join(random.sample(RANDOM_WORDS, min(3, len(RANDOM_WORDS))))
        query_vector = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], dtype=np.float32).tobytes()
        query = ('FT.HYBRID', 'idx',
                'SEARCH', f'@n:[69 1420] @text:({random_words_to_query})',
                'VSIM', '@vector', '$BLOB', 'KNN', '2', 'K', str(n_docs),
                'COMBINE', 'RRF', '2', 'CONSTANT', '60', 'PARAMS', 2, 'BLOB', query_vector)

    expected = get_expected(env, query, query_type)

    # Sanity check - all shards should return the same results
    shards = env.getOSSMasterNodesConnectionList()
    query_shards(env, query, shards, expected, query_type)

    # Add a new shard
    env.addShardToClusterIfExists()
    new_shard = env.getConnection(shardId=initial_shards_count+1)
    # ...and migrate slots from shard 1 to the new shard
    wait_for_migration_complete(env, new_shard, shard1, query_during_migration={'query': query, 'shards': shards, 'expected': expected, 'query_type': query_type})

    # Expect new shard to have the index schema
    env.assertEqual(new_shard.execute_command('FT._LIST'), ['idx'])

    env.waitCluster()
    shards.append(new_shard)
    # Run query_shards for 5 seconds
    start_time = time.time()
    while time.time() - start_time < 5:
        query_shards(env, query, shards, expected, query_type)
        time.sleep(0.1)

@skip(cluster=False, min_shards=2)
def test_add_shard_and_migrate():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
def test_add_shard_and_migrate_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    add_shard_and_migrate_test(env, 'FT.SEARCH')

@skip(cluster=False, min_shards=2)
def test_add_shard_and_migrate_aggregate():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_add_shard_and_migrate_aggregate_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    add_shard_and_migrate_test(env, 'FT.AGGREGATE')

@skip(cluster=False, min_shards=2)
def test_add_shard_and_migrate_aggregate_withcursor():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env, 'FT.AGGREGATE.WITHCURSOR')

@skip(cluster=False, min_shards=2)
def test_add_shard_and_migrate_aggregate_withcursor_BG():
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2')
    add_shard_and_migrate_test(env, 'FT.AGGREGATE.WITHCURSOR')

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
def test_add_shard_and_migrate_hybrid():
    env = Env(clusterNodeTimeout=cluster_node_timeout)
    add_shard_and_migrate_test(env, 'FT.HYBRID')

#TODO: Enable once MOD-13110 is fixed
#@skip(cluster=False, min_shards=2)
@skip
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

def info_modules_to_dict(conn):
    res = conn.execute_command('INFO MODULES')
    info = dict()
    section_name = ""
    for line in res.splitlines():
      if line:
        if line.startswith('#'):
          section_name = line[2:]
          info[section_name] = dict()
        else:
          data = line.split(':', 1)
          info[section_name][data[0]] = data[1]
    return info

def _test_ft_cursors_trimmed(env: Env, protocol: int):
    for shard in env.getOSSMasterNodesConnectionList():
        shard.execute_command('CONFIG', 'SET', 'search-_max-trim-delay-ms', 2500)
    n_docs = 2**14
    create_and_populate_index(env, 'idx', n_docs)

    shard1, shard2 = env.getConnection(1), env.getConnection(2)
    query = ('FT.AGGREGATE', 'idx', '@n:[1 999999]', 'LOAD', 1, 'n', 'WITHCURSOR')

    expected = get_expected(env, query, 'FT.AGGREGATE.WITHCURSOR', protocol)

    _, cursor_id = env.cmd(*query)
    wait_for_migration_complete(env, shard1, shard2)
    time.sleep(5)
    total_results = []
    num_warnings = 0
    while cursor_id != 0:
        res, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', 10)
        if protocol == 2:
          total_results.extend(res)
        else:
          total_results.append(res)
        if protocol == 3:
          if 'warning' in res and len(res['warning']) > 0:
            num_warnings += 1
            env.assertContains('Results may be incomplete due to Atomic Slot Migration', res['warning'][0])

    if protocol == 2:
      results_set = {item[1] for item in total_results if isinstance(item, list) and len(item) == 2 and item[0] == 'n'}
      expected_set = {item[1] for item in expected if isinstance(item, list) and len(item) == 2 and item[0] == 'n'}
      env.assertNotEqual(results_set, expected_set)
      env.assertGreater(len(expected_set), len(results_set))
    else:
      env.assertGreaterEqual(num_warnings, 1)
      # For protocol 3 with RESP3 format, results are in dict format
      results_set = set()
      expected_set = set()

      # Extract from total_results - each item in the list is a cursor response
      for res in total_results:
          if isinstance(res, dict) and 'results' in res:
              for result in res['results']:
                  if 'extra_attributes' in result and 'n' in result['extra_attributes']:
                      results_set.add(result['extra_attributes']['n'])

      # Extract from expected (dict format)
      for res in expected:
          if isinstance(res, dict) and 'results' in res:
              for result in res['results']:
                if 'extra_attributes' in result and 'n' in result['extra_attributes']:
                    expected_set.add(result['extra_attributes']['n'])

      env.assertNotEqual(results_set, expected_set)
      env.assertGreater(len(expected_set), len(results_set))

      shard_total_num_warnings = 0
      coord_total_num_warnings = 0
      for shard_id, shard in enumerate(env.getOSSMasterNodesConnectionList(), 1):
        shard_num_warnings = 0
        coord_num_warnings = 0
        info_dict = info_modules_to_dict(shard)
        if 'search_warnings_and_errors' in info_dict and 'search_shard_total_query_warnings_asm_inaccurate_results' in info_dict['search_warnings_and_errors']:
            shard_num_warnings = int(info_dict['search_warnings_and_errors']['search_shard_total_query_warnings_asm_inaccurate_results'])
        if 'search_coordinator_warnings_and_errors' in info_dict and 'search_coord_total_query_warnings_asm_inaccurate_results' in info_dict['search_coordinator_warnings_and_errors']:
            coord_num_warnings = int(info_dict['search_coordinator_warnings_and_errors']['search_coord_total_query_warnings_asm_inaccurate_results'])
        if shard_id == 1:
            env.assertEqual(shard_num_warnings, 0)
            env.assertGreater(coord_num_warnings, 0)
            if protocol == 3:
              # ShardID 1 is the coordinator so it gets the warnings as nonInternal and are the ones seen in the replies
              env.assertEqual(num_warnings, coord_num_warnings)
        elif (shard_id == 2):
            # ShardID 2 is the one where trimming happens (source shard), so it puts its warnings in the shard
            env.assertGreater(shard_num_warnings, 0)
            env.assertEqual(coord_num_warnings, 0)
        else:
            # Other shards don't have any warnings
            env.assertEqual(shard_num_warnings, 0)
            env.assertEqual(coord_num_warnings, 0)

@skip(cluster=False, min_shards=2)
def test_ft_cursors_trimmed_protocol_2():
    protocol = 2
    env = Env(clusterNodeTimeout=cluster_node_timeout, protocol=protocol)
    _test_ft_cursors_trimmed(env, protocol)

@skip(cluster=False, min_shards=2)
def test_ft_cursors_trimmed_protocol_3():
    protocol = 3
    env = Env(clusterNodeTimeout=cluster_node_timeout, protocol=protocol)
    _test_ft_cursors_trimmed(env, protocol)

@skip(cluster=False, min_shards=2)
def test_ft_cursors_trimmed_BG_protocol_2():
    protocol = 2
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2', protocol=protocol)
    _test_ft_cursors_trimmed(env, protocol)

@skip(cluster=False, min_shards=2)
def test_ft_cursors_trimmed_BG_protocol_3():
    protocol = 3
    env = Env(clusterNodeTimeout=cluster_node_timeout, moduleArgs='WORKERS 2', protocol=protocol)
    _test_ft_cursors_trimmed(env, protocol)

@skip(cluster=False, min_shards=2)
def test_migrate_no_indexes():
    env = Env(clusterNodeTimeout=cluster_node_timeout)

    # Set trim delay to prevent trimming during test
    for shard in env.getOSSMasterNodesConnectionList():
        shard.execute_command('CONFIG', 'SET', 'search-_max-trim-delay-ms', 10000000)
        shard.execute_command('CONFIG', 'SET', 'search-_min-trim-delay-ms', 1000000)

    # Add documents without creating any index
    n_docs = 5 * 2**14
    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            con.execute_command('HSET', f'doc-{i}:{{{i % 2**14}}}',
                              'n', i,
                              'text', f'document {i} content data',
                              'tag', 'even' if i % 2 == 0 else 'odd')

    shard1, shard2 = env.getConnection(1), env.getConnection(2)

    # Measure migration time
    start_time = time.time()
    wait_for_migration_complete(env, shard1, shard2)
    migration_time = time.time() - start_time
    env.debugPrint(f"Migration time: {migration_time}")
    env.assertLess(migration_time, 300.0)
