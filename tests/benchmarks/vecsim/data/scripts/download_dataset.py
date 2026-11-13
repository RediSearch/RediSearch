# This file is a template file for downloading datasets
# In this version, it downloads the "wipedia_single" dataset used for int8
# Refrain from pushing changes unless necessary
from datasets import load_dataset
import numpy as np
import os
import h5py
from math import ceil
from tqdm import tqdm
INT8_KEY = 'emb_int8'
DATASET = 'wikipedia-1024_eng_v3_single'
hdf5_output_file_name = "%s.hdf5" %DATASET

lang = "en" #Use the English Wikipedia subset

num_vectors_train = 1_000_000
num_vectors_test = 10_000
num_vectors = num_vectors_train + num_vectors_test

dim = 1024
docs = load_dataset("Cohere/wikipedia-2023-11-embed-multilingual-v3-int8-binary", lang, split="train", streaming=True)
label_size = 1
data = np.empty((num_vectors//label_size,label_size, dim), dtype=np.int8)


ids = []
counter = 0
label_index = 0

with tqdm(total=num_vectors) as progress_bar:
    for doc in docs:
        if counter == num_vectors:
            break
        ids.append(doc['_id'])
        emb = doc['emb_int8']
        data[label_index, counter % label_size] = emb
        counter += 1
        if counter % label_size == 0:
            label_index += 1
        progress_bar.update(1)

train_data = data[:num_vectors_train // label_size].reshape(-1, dim)
test_data = data[num_vectors_train // label_size:].reshape(-1, dim)

print(f"Train data shape: {train_data.shape}")
print(f"Test data shape: {test_data.shape}")

with h5py.File(hdf5_output_file_name, 'w') as hdf5_file:
    hdf5_file.create_dataset('train', data=train_data)
    hdf5_file.create_dataset('test', data=test_data)
