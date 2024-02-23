import os
import random
import re
import redis
import numpy as np
import string
import subprocess
import time
from redis.exceptions import ResponseError
from redis.commands.search.commands import Query
from multiprocessing import Process, Value
from threading import Thread
import sys

# Connect to redis-server
host = "localhost"
port = 6379
redis_conn = redis.Redis(decode_responses=True, host=host, port=port)

n_vectors = int(sys.argv[1])
dim = 768
vecs_file = f"vectors_n_{n_vectors}_dim_{dim}.npy"

def generate_and_store_random_vectors(n_vectors):
    print(f"generating random vectors, saving to {vecs_file}")
    with open(vecs_file, 'wb') as f:
        for i in range(n_vectors):
            vector = np.array(np.random.rand(dim), dtype=np.float32)
            np.save(f, vector)

def load_vectors(n_vectors, n_clients, key):
    try:
        redis_conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
                                   'DIM', dim, 'DISTANCE_METRIC', 'COSINE')
    except ResponseError as e:
        if str(e).find("Index already exists"):
            pass

    def insert_vectors_client(my_id):
        print(f"client {my_id} insert vectors")
        with open(vecs_file, 'rb') as f:
            for i in range(n_vectors):
                vec = np.load(f)
                doc = f"doc{i}"
                redis_conn.hset(doc, mapping={"vector": vec.astype(np.float32).tobytes()})

    start = time.time()
    print(f"Loading {n_vectors} vectors using {n_clients} clients...")
    clients = [Process(target=insert_vectors_client, args=(i,)) for i in range(n_clients)]
    [c.start() for c in clients]
    [c.join() for c in clients]
    loading_time = time.time() - start
    print(f"Total loading time is {loading_time}\n")
    redis_conn.hset(key, mapping={'loading_time': loading_time})


def run_queries(n_clients, stop_run, key):
    def run_queries_client(my_id, stop_running, total_queries, total_time):
        with total_queries.get_lock():
            print(f"client {my_id} is running queries")
        k = 10
        my_total_queries = 0
        start = time.time()
        while stop_running.value == 0:
            query = f"*=>[KNN {k} @vector $BLOB]"
            q = (
                Query(query)
                .no_content()
                .sort_by("__vector_score", True)
                .paging(0, k)
                .dialect(2)
            )
            query_vec = np.array(np.random.rand(dim), dtype=np.float32)
            res = redis_conn.ft().search(q, query_params={"BLOB": query_vec.tobytes()})
            my_total_queries += 1
            if len(res.docs) < k:
                print("less than k results which are: ", res)
        my_total_time = time.time() - start
        redis_conn.hset(key, mapping={f"client_{my_id}_queries_num": my_total_queries,
                                      f"client_{my_id}_total_runtime": my_total_time})
        with total_queries.get_lock():
            total_queries.value += my_total_queries
            print(f"client {my_id} ran {my_total_queries} queries in {my_total_time}")
        with total_time.get_lock():
            total_time.value += my_total_time

    acc_queries = Value('i', 0)
    acc_time = Value('d', 0.0)
    print(f"Running queries using {n_clients} clients...")
    start = time.time()
    clients_query = [Process(target=run_queries_client, args=(i, stop_run, acc_queries, acc_time)) for i in
                     range(n_clients)]
    [c.start() for c in clients_query]
    [c.join() for c in clients_query]
    QPS = acc_queries.value / (time.time() - start)
    print(f"{acc_queries.value} queries ran, total QPS is {QPS}")
    print(f"Average latency is {acc_time.value / acc_queries.value} seconds\n")
    redis_conn.hset(key, mapping={"Total queries": acc_queries.value,
                                  f"Avg_query_latency": acc_time.value / acc_queries.value, f"QPS": QPS})


if __name__ == '__main__':
    if os.path.exists(vecs_file) == False:
        generate_and_store_random_vectors(n_vectors)
    else:
        print(f"{vecs_file} exists, skipping generation of random vectors...")

    key = 'scenario_1'
    # Fresh start
    redis_conn.flushall()
    #
    # Scenario 1 - loading data
    np.random.seed(20)
    load_vectors(n_vectors, 5, key)

    # querying data for 1m
    stop = Value('i', 0)
    t = Process(target=run_queries, args=(5, stop, key))
    t.start()
    time.sleep(10)
    stop.value = 1
    t.join()
    # #
    # # # Scenario 2 - updating data
    # # key = 'scenario_2'
    # # print(f"Update all vectors")
    # # load_vectors(n_vectors, 5, key)
    # #
    # Scenario 3 - update data while querying
    print(f"Update all vector while running queries")
    for n_readers in [5, 20, 50]:
        key = f'scenario_3_ratio_5:{n_readers}'
        print(f"Running with 5:{n_readers} writers:readers ratio")
        stop = Value('i', 0)
        t = Process(target=run_queries, args=(n_readers, stop, key))
        t.start()
        load_vectors(n_vectors, 5, key)
        stop.value = 1
        t.join()
    #
    # # Scenario 4 - update data while resharding
    # print(f"Update vectors while resharding")
    # for n_readers in [10]:
    #     key = f'scenario_4_{n_readers}_readers'
    #     print(f"Running with {n_readers} readers")
    #     stop = Value('i', 0)
    #     t = Process(target=run_queries, args=(n_readers, stop, key))
    #     t.start()
    #     while redis_conn.info('Persistence')['loading']:
    #         time.sleep(1)
    #     stop.value = 1
    #     print("Done migration state, printing queries stats")
    #     t.join()
    #
    #     # Trimming phase
    #     stop.value = 0
    #     t = Process(target=run_queries, args=(n_readers, stop, key))
    #     t.start()
    #     while redis_conn.ft().info()['max_doc_id'] < n_vectors:
    #         time.sleep(1)
    #     stop.value = 1
    #     print("Done trimming, printing queries stats")
    #     t.join()
