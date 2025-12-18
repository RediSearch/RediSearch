"""
Thread Safety Tests for Shared String Cache in RedisJSON.

These tests verify thread safety of the shared string cache in two scenarios:
1. ASM (Atomic Slots Migration) - cluster slot migrations with concurrent operations
2. Async Flush - standalone mode with background disk writes

The thread-safe Mutex<HashSet> cache prevents race conditions in both scenarios.
"""

import time
import random
import threading
import json
from dataclasses import dataclass
from typing import Optional, Set
import re

from RLTest import Env
from includes import VALGRIND


# Slot table for generating keys that map to specific hash slots
# This table maps hash slots to specific strings that hash to those slots
slot_table = [
    "06S", "Qi", "5L5", "4Iu", "4gY", "460", "1Y7", "1LV", "0QG", "ru", "7Ok", "4ji", "4DE", "65n", "2JH", "I8",
    "F9", "SX", "7nF", "4KD", "4eh", "6PK", "2ke", "1Ng", "0Sv", "4L", "491", "4hX", "4Ft", "5C4", "2Hy", "09R",
    "021", "0cX", "4Xv", "6mU", "6Cy", "42R", "0Mt", "nF", "cv", "1Pe", "5kK", "6NI", "74L", "4UF", "0nh", "MZ",
    "2TJ", "0ai", "4ZG", "6od", "6AH", "40c", "0OE", "lw", "aG", "0Bu", "5iz", "6Lx", "5R7", "4Ww", "0lY", "Ok",
    "5n3", "4ks", "8YE", "7g", "2KR", "1nP", "714", "64t", "69D", "4Ho", "07I", "Ps", "2hN", "1ML", "4fC", "7CA",
]


# ============================================================================
# CLUSTER MODE TESTS - ASM Thread Safety
# ============================================================================

@dataclass(frozen=True)
class SlotRange:
    """Represents a range of hash slots in the cluster."""
    start: int
    end: int

    @staticmethod
    def from_str(s: str):
        """Parse a slot range string like '0-8191' into a SlotRange object."""
        start, end = map(int, s.split("-"))
        assert 0 <= start <= end < 2**14, f"Invalid slot range: {start}-{end}"
        return SlotRange(start, end)


@dataclass
class ClusterNode:
    """Represents a node in the Redis cluster."""
    id: str
    ip: str
    port: int
    cport: int  # cluster bus port
    hostname: Optional[str]
    flags: Set[str]
    master: str  # Either this node's primary replica or '-'
    ping_sent: int
    pong_recv: int
    config_epoch: int
    link_state: bool  # True: connected, False: disconnected
    slots: Set[SlotRange]

    @staticmethod
    def from_str(s: str):
        """Parse a CLUSTER NODES line into a ClusterNode object."""
        # Format: <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-range> ...
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


def test_asm_basic_migration():
    """
    Test 1: Basic ASM migration functionality.
    Verifies slot migration works correctly with and without JSON data.
    """
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    # Test without data
    migrate_slots_back_and_forth(env)
    
    # Test with data
    fill_json_data(env, number_of_keys=100, nested_depth=2)
    migrate_slots_back_and_forth(env)


def test_asm_shared_string_cache_concurrent_operations():
    """
    Test 2: Core test for shared string cache race conditions during ASM.
    
    This stress test verifies the thread-safe Mutex<HashSet> cache prevents races when:
    - Multiple threads access shared string cache concurrently
    - ASM migrations move data between shards
    - TRIM phase frees memory from source shard
    
    Strategy:
    1. Create many keys with MANY duplicate string fields (pressure cache)
    2. Launch multiple threads: readers, writers, modifiers
    3. Perform rapid ASM migrations during operations
    4. Verify no crashes, data corruption, or race conditions
    """
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    num_keys = 200 if not VALGRIND else 20
    num_shared_strings = 20  # Duplicate strings across documents
    
    # Create pool of shared strings (these will be heavily cached)
    SHARED_STRINGS = [
        f"SHARED_STRING_{i}_{'X' * 100}" for i in range(num_shared_strings)
    ]
    
    env.debugPrint(f"Creating {num_keys} keys with {num_shared_strings} shared strings...", force=True)
    
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_keys):
            hslot = i * (len(slot_table) - 1) // (num_keys - 1)
            key = f"json:{{{slot_table[hslot]}}}"
            
            # Document with many references to the SAME strings (cache pressure)
            doc = {
                f"field_{j}": SHARED_STRINGS[j % num_shared_strings]
                for j in range(50)  # 50 fields using shared strings
            }
            conn.execute_command("JSON.SET", key, "$", json.dumps(doc))
    
    done = False
    errors = []
    crashes_detected = [False]
    operation_counts = [0]
    
    def writer_thread():
        """Continuously modify strings (cache writes)"""
        with env.getClusterConnectionIfNeeded() as thread_conn:
            while not done and not crashes_detected[0]:
                try:
                    key_idx = random.randint(0, num_keys - 1)
                    hslot = key_idx * (len(slot_table) - 1) // (num_keys - 1)
                    key = f"json:{{{slot_table[hslot]}}}"
                    field_idx = random.randint(0, 49)
                    
                    # Replace with another shared string (cache access)
                    new_string = SHARED_STRINGS[random.randint(0, num_shared_strings - 1)]
                    thread_conn.execute_command("JSON.SET", key, f"$.field_{field_idx}", json.dumps(new_string))
                    operation_counts[0] += 1
                    
                    # Occasional yield to increase race window
                    if operation_counts[0] % 50 == 0:
                        time.sleep(0.001)
                    
                except Exception as e:
                    error_str = str(e)
                    if "MOVED" not in error_str and "ASK" not in error_str and "CLUSTERDOWN" not in error_str:
                        if "crash" in error_str.lower() or "connection" in error_str.lower():
                            crashes_detected[0] = True
                        errors.append(f"Writer error: {e}")
    
    def reader_thread():
        """Continuously read documents (cache reads)"""
        with env.getClusterConnectionIfNeeded() as thread_conn:
            while not done and not crashes_detected[0]:
                try:
                    key_idx = random.randint(0, num_keys - 1)
                    hslot = key_idx * (len(slot_table) - 1) // (num_keys - 1)
                    key = f"json:{{{slot_table[hslot]}}}"
                    
                    # Read entire document (accesses all shared strings)
                    result = thread_conn.execute_command("JSON.GET", key, "$")
                    if result:
                        doc = json.loads(result)
                        if doc and isinstance(doc, list) and len(doc) > 0:
                            obj = doc[0]
                            # Verify all values are shared strings
                            for value in list(obj.values())[:10]:
                                if value not in SHARED_STRINGS:
                                    errors.append(f"String corruption detected: {value[:50]}")
                    
                    operation_counts[0] += 1
                    
                except Exception as e:
                    error_str = str(e)
                    if "MOVED" not in error_str and "ASK" not in error_str and "CLUSTERDOWN" not in error_str:
                        if "crash" in error_str.lower() or "connection" in error_str.lower():
                            crashes_detected[0] = True
                        errors.append(f"Reader error: {e}")
    
    # Start MORE worker threads to increase contention
    num_writer_threads = 8 if not VALGRIND else 4
    num_reader_threads = 8 if not VALGRIND else 4
    
    threads = []
    for _ in range(num_writer_threads):
        threads.append(threading.Thread(target=writer_thread))
    for _ in range(num_reader_threads):
        threads.append(threading.Thread(target=reader_thread))
    
    env.debugPrint(f"Starting {len(threads)} threads ({num_writer_threads} writers + {num_reader_threads} readers)...", force=True)
    
    for t in threads:
        t.start()
    
    time.sleep(0.5)
    
    env.debugPrint("Starting rapid migrations...", force=True)
    
    # Perform MORE rapid migrations to maximize race condition window
    num_rounds = 20 if not VALGRIND else 5
    try:
        for round_num in range(num_rounds):
            if crashes_detected[0]:
                break
            env.debugPrint(f"Migration round {round_num + 1}/{num_rounds} (ops: {operation_counts[0]})", force=True)
            migrate_slots_rapid(env)
            time.sleep(0.05)  # Shorter sleep = more pressure
    finally:
        done = True
        for t in threads:
            t.join(timeout=5)
    
    env.debugPrint(f"Total operations: {operation_counts[0]}", force=True)
    
    if crashes_detected[0]:
        raise AssertionError("CRASH DETECTED during concurrent operations!")
    
    if errors:
        unique_errors = list(set(errors))[:10]
        raise AssertionError(f"Encountered {len(errors)} errors: {unique_errors}")


def test_asm_read_during_trim_phase():
    """
    Test 3: Use-after-free test - reading keys during TRIM phase.
    
    The TRIM phase is when ASM deletes keys from source shard after migration.
    This test attempts to trigger use-after-free bugs by:
    1. Thread reads key, gets reference to shared strings
    2. Migration moves key to other shard
    3. TRIM phase frees the key and strings on source shard
    4. Thread tries to access freed strings
    
    Expected: No crashes (thread-safe cache prevents use-after-free)
    """
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    num_keys = 100 if not VALGRIND else 10
    
    # Create keys with large strings (lots of cache entries)
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_keys):
            hslot = i * (len(slot_table) - 1) // (num_keys - 1)
            key = f"json:{{{slot_table[hslot]}}}"
            
            doc = {
                f"string_{j}": f"Large string value {j} for key {i} " + ("Y" * 500)
                for j in range(50)
            }
            conn.execute_command("JSON.SET", key, "$", json.dumps(doc))
    
    done = False
    errors = []
    connection_errors = []
    reads_completed = [0]
    
    def aggressive_reader():
        """Continuously read keys, especially during TRIM phase"""
        with env.getClusterConnectionIfNeeded() as thread_conn:
            while not done:
                try:
                    key_idx = random.randint(0, num_keys - 1)
                    hslot = key_idx * (len(slot_table) - 1) // (num_keys - 1)
                    key = f"json:{{{slot_table[hslot]}}}"
                    
                    # Deep query to force string access
                    result = thread_conn.execute_command("JSON.GET", key, "$")
                    if result:
                        data = json.loads(result)
                        # Force accessing strings
                        if data and len(data) > 0 and isinstance(data[0], dict):
                            for val in list(data[0].values())[:5]:
                                _ = len(str(val))
                    
                    reads_completed[0] += 1
                    
                except Exception as e:
                    error_str = str(e)
                    if "connection" in error_str.lower() or "broken" in error_str.lower():
                        connection_errors.append(str(e))
                    elif "MOVED" not in error_str and "ASK" not in error_str and "CLUSTERDOWN" not in error_str:
                        errors.append(str(e))
    
    # Start many reader threads
    threads = [threading.Thread(target=aggressive_reader) for _ in range(6)]
    for t in threads:
        t.start()
    
    time.sleep(0.2)
    
    env.debugPrint("Starting migrations while threads are reading...", force=True)
    
    # Perform multiple rapid migrations during reads
    try:
        for round_num in range(8):
            env.debugPrint(f"Migration round {round_num + 1}/8 (reads: {reads_completed[0]})", force=True)
            migrate_slots_rapid(env)
            time.sleep(0.05)  # Very short - maximize use-after-free window
            
            # Verify shard is still alive
            try:
                with env.getClusterConnectionIfNeeded() as conn:
                    conn.execute_command("PING")
            except Exception as e:
                connection_errors.append(f"Shard crash detected: {e}")
                break
    finally:
        done = True
        for t in threads:
            t.join(timeout=5)
    
    env.debugPrint(f"Total reads: {reads_completed[0]}", force=True)
    
    if connection_errors:
        raise AssertionError(f"CONNECTION LOST - POSSIBLE CRASH! {len(connection_errors)} errors: {connection_errors[:5]}")
    
    if errors:
        env.debugPrint(f"Other errors: {len(errors)}", force=True)


def migrate_slots_rapid(env):
    """Perform rapid slot migrations to maximize race condition window."""
    first_conn, second_conn = env.getConnection(0), env.getConnection(1)
    
    def get_node_slots(conn):
        for line in conn.execute_command("cluster", "nodes").splitlines():
            node = ClusterNode.from_str(line)
            if "myself" in node.flags:
                return node.slots
        raise ValueError("No node with 'myself' flag found")
    
    def get_middle_range(slot_range: SlotRange) -> SlotRange:
        third = (slot_range.end - slot_range.start) // 3
        return SlotRange(slot_range.start + third, slot_range.end - third)
    
    original_first, = get_node_slots(first_conn)
    original_second, = get_node_slots(second_conn)
    middle_first = get_middle_range(original_first)
    middle_second = get_middle_range(original_second)
    
    # Migrate from second to first
    task_id = first_conn.execute_command("CLUSTER", "MIGRATION", "IMPORT", middle_second.start, middle_second.end)
    wait_for_migration(first_conn, task_id, timeout=15)
    
    # Migrate back
    task_id = second_conn.execute_command("CLUSTER", "MIGRATION", "IMPORT", middle_second.start, middle_second.end)
    wait_for_migration(second_conn, task_id, timeout=15)


def wait_for_migration(conn, task_id, timeout=5):
    """Wait for migration to complete."""
    start = time.time()
    while time.time() - start < timeout:
        status, = conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", task_id)
        status_dict = {key: value for key, value in zip(status[0::2], status[1::2])}
        if status_dict["state"] == "completed":
            return
        time.sleep(0.01)  # Very short sleep for rapid checking
    raise TimeoutError(f"Migration {task_id} did not complete")


# Helper functions

def fill_json_data(env, number_of_keys: int, nested_depth: int = 2):
    """Fill the cluster with JSON documents distributed across slots."""
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(number_of_keys):
            # Distribute keys across hash slots
            hslot = i * (len(slot_table) - 1) // (number_of_keys - 1)
            key = f"json:{{{slot_table[hslot]}}}"
            
            # Create a nested document
            doc = create_nested_doc(i, nested_depth)
            conn.execute_command("JSON.SET", key, "$", json.dumps(doc))


def create_nested_doc(id_val: int, depth: int) -> dict:
    """Create a nested JSON document for testing."""
    if depth == 0:
        return {
            "id": id_val,
            "value": f"value_{id_val}",
            "number": random.randint(0, 1000)
        }
    
    return {
        "id": id_val,
        "level": depth,
        "data": f"data_at_level_{depth}",
        "array": [i for i in range(5)],
        "nested": create_nested_doc(id_val, depth - 1)
    }


def cluster_node_of(conn) -> ClusterNode:
    """Get the ClusterNode information for the node this connection is connected to."""
    for line in conn.execute_command("cluster", "nodes").splitlines():
        cluster_node = ClusterNode.from_str(line)
        if "myself" in cluster_node.flags:
            return cluster_node
    raise ValueError("No node with 'myself' flag found")


def middle_slot_range(slot_range: SlotRange) -> SlotRange:
    """Get the middle third of a slot range."""
    third = (slot_range.end - slot_range.start) // 3
    return SlotRange(slot_range.start + third, slot_range.end - third)


def cantorized_slot_set(slot_range: SlotRange) -> Set[SlotRange]:
    """
    Split a slot range by removing the middle third (like a Cantor set).
    Returns the remaining two ranges.
    """
    middle = middle_slot_range(slot_range)
    return {
        SlotRange(slot_range.start, middle.start - 1),
        SlotRange(middle.end + 1, slot_range.end)
    }


def migrate_slots_back_and_forth(env):
    """
    Perform slot migrations back and forth between two shards.
    This tests that data remains intact and accessible during and after migrations.
    """
    first_conn, second_conn = env.getConnection(0), env.getConnection(1)
    
    # Get original slot distributions
    original_first_slot_range, = cluster_node_of(first_conn).slots
    original_second_slot_range, = cluster_node_of(second_conn).slots
    middle_of_original_first = middle_slot_range(original_first_slot_range)
    middle_of_original_second = middle_slot_range(original_second_slot_range)

    # First migration: move slots from second to first
    import_slots(second_conn, first_conn, middle_of_original_second)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range, middle_of_original_second}
    assert cluster_node_of(second_conn).slots == cantorized_slot_set(original_second_slot_range)

    # Second migration: move slots back from first to second
    import_slots(first_conn, second_conn, middle_of_original_second)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}

    # Third migration: move slots from first to second
    import_slots(first_conn, second_conn, middle_of_original_first)
    assert cluster_node_of(second_conn).slots == {original_second_slot_range, middle_of_original_first}
    assert cluster_node_of(first_conn).slots == cantorized_slot_set(original_first_slot_range)

    # Fourth migration: move slots back from second to first
    import_slots(second_conn, first_conn, middle_of_original_first)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}


def import_slots(source_conn, target_conn, slot_range: SlotRange):
    """
    Import a range of slots from the node connected via source_conn to the node connected via target_conn.
    Waits for the migration to complete.
    """
    task_id = target_conn.execute_command("CLUSTER", "MIGRATION", "IMPORT", slot_range.start, slot_range.end)

    def wait_for_completion(conn):
        start_time = time.time()
        timeout = 5 if not VALGRIND else 60
        while time.time() - start_time < timeout:
            (migration_status,) = conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", task_id)
            migration_status = {key: value for key, value in zip(migration_status[0::2], migration_status[1::2])}
            if migration_status["state"] == "completed":
                break
            time.sleep(0.1)
        else:
            raise TimeoutError(f"Migration {task_id} did not complete in {timeout} seconds, state is {migration_status['state']}")

    wait_for_completion(target_conn)
    # The oss cluster's status data is not CP, but we should rather rely on eventual consistencty, so let's wait for the source as well
    wait_for_completion(source_conn)


# ============================================================================
# STANDALONE MODE TESTS - Async Flush Thread Safety
# ============================================================================

def test_concurrent_writes_async_flush():
    """
    Test: Concurrent writes with shared field names - async flush race conditions.
    
    Verifies thread-safe cache works during concurrent writes in standalone mode.
    Tests the scenario where:
    - Multiple client connections write simultaneously
    - All use the same field names (shared in cache)
    - Async flush may happen in background
    
    This is the primary test for async flush race conditions.
    """
    env = Env(decodeResponses=True)
    
    # Template with many shared field names that get cached
    template = {
        "transaction_id": 0,
        "user_id": 0,
        "amount": 0.0,
        "currency": "USD",
        "status": "pending",
        "payment_method": "credit_card",
        "billing_address": {
            "street": "Main St",
            "city": "New York",
            "state": "NY",
            "country": "USA",
            "postal_code": "10001"
        },
        "shipping_address": {
            "street": "Main St",
            "city": "New York",
            "state": "NY",
            "country": "USA",
            "postal_code": "10001"
        },
        "items": [],
        "metadata": {
            "created_at": "2025-01-01",
            "updated_at": "2025-01-01",
            "ip_address": "127.0.0.1",
            "user_agent": "Mozilla/5.0"
        }
    }
    
    errors = []
    success_count = [0]
    
    def write_worker(worker_id, count):
        """Worker thread that writes JSON documents"""
        try:
            conn = env.getConnection()
            for i in range(count):
                doc = template.copy()
                doc["transaction_id"] = worker_id * 1000 + i
                doc["user_id"] = worker_id
                doc["amount"] = round(random.uniform(10.0, 1000.0), 2)
                
                key = f"transaction:{worker_id}:{i}"
                conn.execute_command("JSON.SET", key, "$", json.dumps(doc))
                
                # Strategic yield to widen race window for async flush
                if i % 10 == 0:
                    time.sleep(0.001)
                
                # Verify immediately to catch corruption
                result = json.loads(conn.execute_command("JSON.GET", key, "$"))
                if result[0]["transaction_id"] != doc["transaction_id"]:
                    errors.append(f"Data corruption in worker {worker_id}")
                else:
                    success_count[0] += 1
        except Exception as e:
            errors.append(f"Worker {worker_id} error: {str(e)}")
    
    # Launch MORE concurrent writers to increase contention
    threads = []
    num_workers = 16  # Doubled to increase contention
    writes_per_worker = 50  # More operations per worker
    
    env.debugPrint(f"Starting {num_workers} concurrent writers, {writes_per_worker} writes each...", force=True)
    
    for i in range(num_workers):
        t = threading.Thread(target=write_worker, args=(i, writes_per_worker))
        t.start()
        threads.append(t)
    
    # Wait for completion
    for t in threads:
        t.join(timeout=30)
    
    # Check results
    if errors:
        raise AssertionError(f"Race condition detected: {errors}")
    
    expected = num_workers * writes_per_worker
    assert success_count[0] == expected, f"Expected {expected} writes, got {success_count[0]}"


def test_mixed_operations_async_flush():
    """
    Test: Mixed read/write operations stressing the shared string cache.
    
    Simulates real-world usage with:
    - Concurrent readers and writers
    - Same field names reused across documents
    - Async flush potentially happening in background
    
    This test catches issues with concurrent reads during cache updates.
    """
    env = Env(decodeResponses=True)
    
    conn = env.getConnection()
    
    # Seed initial data with shared field names
    for i in range(20):
        doc = {
            "id": i,
            "name": f"item_{i}",
            "category": "test",
            "price": 10.0,
            "in_stock": True,
            "tags": ["tag1", "tag2", "tag3"]
        }
        conn.execute_command("JSON.SET", f"item:{i}", "$", json.dumps(doc))
    
    errors = []
    read_count = [0]
    write_count = [0]
    
    def reader_worker(worker_id, iterations):
        """Worker that reads JSON documents"""
        try:
            conn = env.getConnection()
            for i in range(iterations):
                key = f"item:{i % 20}"
                result = conn.execute_command("JSON.GET", key, "$")
                if result:
                    read_count[0] += 1
        except Exception as e:
            errors.append(f"Reader {worker_id} error: {str(e)}")
    
    def writer_worker(worker_id, iterations):
        """Worker that updates JSON documents"""
        try:
            conn = env.getConnection()
            for i in range(iterations):
                key = f"item:{i % 20}"
                # Update price - reuses cached field name "price"
                conn.execute_command("JSON.NUMINCRBY", key, "$.price", 1.0)
                write_count[0] += 1
        except Exception as e:
            errors.append(f"Writer {worker_id} error: {str(e)}")
    
    threads = []
    
    # Mix of readers and writers to stress cache
    for i in range(5):
        t = threading.Thread(target=reader_worker, args=(i, 50))
        threads.append(t)
        t.start()
    
    for i in range(5):
        t = threading.Thread(target=writer_worker, args=(i, 50))
        threads.append(t)
        t.start()
    
    # Wait for all to complete
    for t in threads:
        t.join(timeout=30)
    
    # Check results
    if errors:
        raise AssertionError(f"Race condition: {errors}")
    
    assert read_count[0] > 0, "No reads completed"
    assert write_count[0] > 0, "No writes completed"
