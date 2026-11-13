# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import os
from urllib.request import urlretrieve
import numpy as np
import time
from VecSim import *
import h5py

# This script is partially based on the work that was made in:
# https://github.com/erikbern/ann-benchmarks/blob/master/ann_benchmarks/datasets.py


def get_vecsim_metric(dataset_metric):
    metrics_map = {'angular': VecSimMetric_Cosine, 'euclidean': VecSimMetric_L2}
    return metrics_map[dataset_metric]


def download(src, dst):
    if not os.path.exists(dst):
        print('downloading %s -> %s...' % (src, dst))
        urlretrieve(src, dst)


# Download dataset from ann-benchmarks, save the file locally
def get_data_set(dataset_name):
    hdf5_filename = os.path.join('data', '%s.hdf5' % dataset_name)
    url = 'http://ann-benchmarks.com/%s.hdf5' % dataset_name
    download(url, hdf5_filename)
    return h5py.File(hdf5_filename, 'r')


# Create an HNSW index from dataset based on specific params.
def load_or_create_hnsw_index(dataset, index_file_name, is_multi, data_type, ef_construction, M):
    # If we are using existing index, we load the index from the existing file.
    if os.path.exists(index_file_name):
        print(index_file_name, "already exist. Remove index file first to override it")
        return HNSWIndex(index_file_name)

    X_train = np.array(dataset['train'])
    distance = dataset.attrs['distance']
    dimension = int(dataset.attrs['dimension']) if 'dimension' in dataset.attrs else len(X_train[0])
    print('got a train set of size (%d * %d)' % (X_train.shape[0], dimension))
    print('metric is: %s' % distance)

    # Init index.
    hnswparams = HNSWParams()
    hnswparams.M = M
    hnswparams.efConstruction = ef_construction
    hnswparams.dim = dimension
    hnswparams.type = data_type
    hnswparams.metric = get_vecsim_metric(distance)
    hnswparams.multi = is_multi

    hnsw_index = HNSWIndex(hnswparams)
    populate_save_index(hnsw_index, index_file_name, X_train)
    return hnsw_index


def populate_save_index(hnsw_index, index_file_name, X_train):
    # Insert vectors one by one.
    t0 = time.time()
    for i, vector in enumerate(X_train):
        hnsw_index.add_vector(vector, i)
    print('Built index time:', (time.time() - t0)/60, "minutes")
    hnsw_index.save_index(index_file_name)



def measure_recall_per_second(hnsw_index, dataset, is_multi, data_type, num_queries, k, ef_runtime):
    X_train = np.array(dataset['train'])
    X_test = np.array(dataset['test'])

    # Create BF index to compare hnsw results with
    bfparams = BFParams()
    bfparams.dim = int(dataset.attrs['dimension']) if 'dimension' in dataset.attrs else len(X_train[0])
    bfparams.type = data_type
    bfparams.metric = get_vecsim_metric(dataset.attrs['distance'])
    bfparams.multi = is_multi
    bf_index = BFIndex(bfparams)

    # Add all the vectors in the train set into the index.
    for i, vector in enumerate(X_train):
        bf_index.add_vector(vector, i)
    assert bf_index.index_size() == hnsw_index.index_size()

    # Measure recall and times
    hnsw_index.set_ef(ef_runtime)

    print("\nRunning queries with ef_runtime =", ef_runtime, "...")
    correct = 0
    bf_total_time = 0
    hnsw_total_time = 0
    for target_vector in X_test[:num_queries]:
        start = time.time()
        hnsw_labels, _ = hnsw_index.knn_query(target_vector, k)
        hnsw_total_time += (time.time() - start)
        start = time.time()
        bf_labels, _ = bf_index.knn_query(target_vector, k)
        bf_total_time += (time.time() - start)
        correct += len(np.intersect1d(hnsw_labels[0], bf_labels[0]))
    # Measure recall
    recall = float(correct)/(k*num_queries)
    print("Average recall is:", recall)
    print("BF query per seconds: ", num_queries/bf_total_time)
    print("HNSW query per seconds: ", num_queries/hnsw_total_time)


def run_benchmark(dataset_name, is_multi, data_type, ef_construction, M, ef_values, k=10):
    if data_type != VecSimType_FLOAT32 and data_type != VecSimType_FLOAT64:
        raise TypeError("run_benchmark: This datatype is not supported")

    print("\nRunning benchmark for:", dataset_name)
    dataset = get_data_set(dataset_name)
    nickname, dim, metric = dataset_name.split('-')
    metric = 'cosine' if metric == 'angular' else metric
    index_file_name = os.path.join('data', '%s-%s-dim%s-M%s-efc%s.hnsw_v3' % (nickname, metric, dim, M, ef_construction))
    hnsw_index = load_or_create_hnsw_index(dataset, index_file_name, is_multi, data_type, ef_construction, M)

    for ef_runtime in ef_values:
        measure_recall_per_second(hnsw_index, dataset, is_multi, data_type, num_queries=1000, k=k, ef_runtime=ef_runtime)

if __name__ == "__main__":
    DATASETS = ['glove-25-angular', 'glove-50-angular', 'glove-100-angular', 'glove-200-angular',
                'mnist-784-euclidean', 'sift-128-euclidean']
    # for every dataset the params are: (is_multi, data_type, ef_construction, M, ef_runtime).
    dataset_params = {'glove-25-angular': (False, VecSimType_FLOAT32, 100, 16, [50, 100, 200]),
                      'glove-50-angular': (False, VecSimType_FLOAT32, 150, 24, [100, 200, 300]),
                      'glove-100-angular': (False, VecSimType_FLOAT32, 250, 36, [150, 300, 500]),
                      'glove-200-angular': (False, VecSimType_FLOAT32, 350, 48, [200, 350, 600]),
                      'mnist-784-euclidean': (False, VecSimType_FLOAT32, 150, 32, [100, 200, 350]),
                      'sift-128-euclidean': (False, VecSimType_FLOAT32, 200, 32, [150, 300, 500])}
    k = 10
    for d_name in DATASETS:
        run_benchmark(d_name, *dataset_params[d_name], k)
