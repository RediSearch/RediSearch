from enum import Enum
import multiprocessing

from RLTest import Env

from includes import *
from common import *
import numpy as np

class IndexType(Enum):
    TAG = 1
    TEXT = 2
    NUMERIC = 3
    VECTOR = 4

def create_tag_index(env):
    env.expect('ft.create', 'idx', 'schema', 'tag1', 'tag').equal('OK')

def create_text_index(env):
    env.expect('ft.create', 'idx', 'schema', 't1', 'text').equal('OK')

def create_numeric_index(env):
    env.expect('ft.create', 'idx', 'schema', 'n1', 'numeric').equal('OK')

def create_vector_index(env):
    env.expect('ft.create', 'idx', 'schema', 'v1', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2').equal('OK')

# All of the above
def create_index(env):
    env.expect('ft.create', 'idx', 'schema', 'tag1', 'tag', 't1', 'text', 'n1', 'numeric', 'v1', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2').equal('OK')

create_commands = {
    IndexType.TAG: create_tag_index,
    IndexType.TEXT: create_text_index,
    IndexType.NUMERIC: create_numeric_index,
    IndexType.VECTOR: create_vector_index
}

def insert_tag_data(env):
    for i in range(10):
        env.cmd('hset', i, 'tag1', 'hello')

def insert_text_data(env):
    for i in range(10):
        env.cmd('hset', i, 't1', 'hello world')


def insert_numeric_data(env):
    for i in range(10):
        env.cmd('hset', i, 'n1', i)

const_vector = np.full(128, 1.0, dtype=np.float32).tobytes()

def insert_vector_data(env):
    for i in range(10):
        env.cmd('hset', i, 'v1', const_vector)

# All of the above
def insert_complete_data(env):
    for i in range(10):
        env.cmd('hset', i, 'tag1', 'hello', 'n1', i, 'v1', const_vector, 't1', 'hello world')

initial_insert_commands = {
    IndexType.TAG: insert_tag_data,
    IndexType.TEXT: insert_text_data,
    IndexType.NUMERIC: insert_numeric_data,
    IndexType.VECTOR: insert_vector_data
}

queries= {
    IndexType.TAG: ['ft.search', 'idx', '@tag1:{hello}', 'NOCONTENT'],
    IndexType.TEXT: ['ft.search', 'idx', '@t1:(hello world)', 'NOCONTENT'],
    IndexType.NUMERIC: ['ft.search', 'idx', '@n1:[0 10]', 'NOCONTENT'],
    IndexType.VECTOR: ['ft.search', 'idx', '*=>[KNN 10 @v1, $BLOB]', 'NOCONTENT', 'PARAMS', '2', 'BLOB', const_vector]
    
}

expected = {
    IndexType.TAG: [str(i) for i in range(10)],
    IndexType.TEXT: [str(i) for i in range(10)],
    IndexType.NUMERIC: [str(i) for i in range(10)],
    IndexType.VECTOR: [str(i) for i in range(10)]

}

def generic_query(env, query, expected):
    # Run the query for up to 1 second, checking the result after each run
    res = env.cmd(*query)
    timeout = time.time() + 1
    # The "while" loop is for tests that modify the index, so we need to wait for the index to hold the new data.
    while time.time() < timeout and res[1:] != expected:
        res = env.execute_command(*query)

    # Check that the result is as expected
    env.assertEqual(res[1:], expected)


def run_test_multiproc(env, n_procs, fn, args=tuple()):
    procs = []

    def tmpfn():
        fn(env, *args)
        return 1

    for i in range(n_procs):
        p = multiprocessing.Process(target=tmpfn)
        p.start()
        procs.append(p)
    
    return procs


def generic_runner(env, nproc, create_command, insert_data_command, query, expect, update_data_command):
    # Create the index
    create_command(env)
    # Insert data (full/partial according to the scenario)
    insert_data_command(env)
    # Run the query in multiple processes. The query should be executed successfully asynchronously when updates occur.
    procs = run_test_multiproc(env, nproc, generic_query, (query, expect))
    # Update the data (if needed)
    if update_data_command:
        update_data_command(env) 
    # Wait for the processes to finish
    [p.join() for p in procs]
    # Drop the index
    env.expect('FT.DROP', 'idx').ok()
    


def test_simple_multi_client():
    # Create an environment with a single shard with half of the cpus available
    number_of_cores = multiprocessing.cpu_count()
    env = Env(moduleArgs=f'DEFAULT_DIALECT 2 ENABLE_THREADS TRUE WORKER_THREADS {number_of_cores // 2}')
    if number_of_cores < 2:
        env.skip()
    # no need to run this test on cluster, since it is a shard test
    env.skipOnCluster()
    

    # For each index type, create an index and run a query in different processes(clients) concurrently.
    # The query should be executed successfully, if not, the new process will not return and will execute the query
    # until the expected result is returned.
    for index_type in IndexType:
        generic_runner(env, number_of_cores // 2,  create_commands[index_type], initial_insert_commands[index_type], queries[index_type], expected[index_type], None)

# def test_write_while_reading():
#     pass

# def test_sorting():
#     pass

# def test_projection():
#     pass

