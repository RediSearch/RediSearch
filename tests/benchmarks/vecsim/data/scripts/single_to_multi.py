# This file is a template file for generating multi datasets from single
# Using HNSW-knn for labeling
# In this version, it creates a multi dataset from the "wipedia_single" dataset used for int8
# Refrain from pushing changes unless necessary
import h5py
import numpy as np
import sys
from tqdm import tqdm
import hnswlib
import os

DATASET = 'wikipedia-1024_eng_v3_single'
label_size = 10
select_from_closest = label_size - 1  # Ensures label groups have `label_size` elements

hdf5_output_file_name = f"{DATASET}.hdf5"

# Load dataset
with h5py.File(hdf5_output_file_name, "r") as f:
    dataset = np.array(f['train'])  # Load into memory

n_vectors, vector_dim = dataset.shape
n_labels = n_vectors // label_size

p = hnswlib.Index(space='cosine', dim=1024)
p.init_index(max_elements=n_vectors, ef_construction=100, M=16)
# Controlling the recall by setting ef:
# higher ef leads to better accuracy, but slower search
p.set_ef(64)

# Set number of threads used during batch search/construction
# By default using all available cores
p.set_num_threads(4)

p.add_items(dataset)


available = np.ones(n_vectors, dtype=bool)
result_arr = np.zeros((n_labels, label_size, vector_dim), dtype=np.float32)  # Store actual vectors

groups = {}
count = 0
for i in tqdm(range(n_vectors)):
    if i in groups:
        continue
    labels, distances = p.knn_query(dataset[i], k=label_size)
    labels = labels[0]
    if i not in labels:
        labels[-1] = i
    for lbl in labels:
        groups[lbl] = count
        p.mark_deleted(lbl)
    result_arr[count] = dataset[labels]
    count+=1
    if count== n_labels:
        break

inverse_groups = {}
for vct,lbl in groups.items():
    if lbl not in inverse_groups:
        inverse_groups[lbl] = []
    inverse_groups[lbl].append(vct)

for i in range(n_vectors):
    if i not in groups:
        print("Error: Some vectors appear to be missing!")
        sys.exit(1)
for i in range(n_labels):
    if i not in inverse_groups:
        print("Error: Some labels appear to be missing!")
        sys.exit(1)
    if len(inverse_groups[i]) <label_size:
        print("Error: Not all labels are full")
        sys.exit(1)
    if len(inverse_groups[i]) > label_size:
        print(f"Error: Some labels are bigger then {label_size}")
        sys.exit(1)


# Save to HDF5, replacing the single with multi
output_file = DATASET.replace("_single", "_multi") + ".hdf5"
with h5py.File(output_file, "w") as f:
    f.create_dataset('train', data=result_arr)

    # Copy 'test' dataset
    with h5py.File(hdf5_output_file_name, "r") as f_in:
        test_data = f_in['test'][:]
    f.create_dataset('test', data=test_data)
