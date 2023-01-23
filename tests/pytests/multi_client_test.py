from enum import Enum
import multiprocessing

from RLTest import Env

from includes import *
from common import *
import os

class IndexType(Enum):
    TAG = 1
    # TEXT = 2
    # NUMERIC = 3
    # VECTOR = 4

def create_tag_index(env):
    env.expect('ft.create', 'idx', 'schema', 't1', 'tag').equal('OK')

def create_text_index(env):
    env.expect('ft.create', 'idx', 'schema', 't1', 'text').equal('OK')

def create_numeric_index(env):
    env.expect('ft.create', 'idx', 'schema', 'n1', 'numeric').equal('OK')

def create_vector_index(env):
    env.expect('ft.create', 'idx', 'schema', 'v1', 'vector' 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2').equal('OK')

create_commands = {
    IndexType.TAG: create_tag_index,
    # IndexType.TEXT: create_text_index,
    # IndexType.NUMERIC: create_numeric_index,
    # IndexType.VECTOR: create_vector_index
}

def insert_tag_data(env):
    for i in range(10):
        env.expect('hset', i, 't1', 'hello').equal(1)


initial_insert_commands = {
    IndexType.TAG: insert_tag_data,
}


def test_basic_tag(env):
    res = env.execute_command('ft.search', 'idx', '@t1:{hello}')
    while(res != [10, [str(i) for i in range(10)]]):
        res = env.execute_command('ft.search', 'idx', '@t1:{hello}')

def run_test_multiproc(env, n_procs, fn, args=tuple()):
    procs = []

    def tmpfn():
        con = env.get_connection()
        fn(con, *args)
        return 1

    for i in range(n_procs):
        p = multiprocessing.Process(target=tmpfn)
        p.start()
        procs.append(p)
    
    return procs
    



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
    # The "while" loop is for tests that modify the index, so we need to wait for the index to hold the new data.
    for index_type in IndexType:
        # Create the index
        create_commands[index_type](env)
        # Insert the initial data
        initial_insert_commands[index_type](env)
        # Run the query in different processes
        procs = run_test_multiproc(env, number_of_cores//2, query_commands[index_type], (query[index_type], ))
        # Wait for the query to be executed successfully
        [p.join() for p in procs]
        # Finally, drop the index
        env.executeCommand('FT.DROP', 'idx')

def test_write_while_reading():
    pass

def test_sorting():
    pass

def test_projection():
    pass

