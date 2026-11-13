# SPDX-FileCopyrightText: Copyright 2026 Arm Limited and/or its affiliates <open-source-office@arm.com>
# SPDX-License-Identifier: BSD-3-Clause
#
# Convert an existing benchmark HNSW index file
# (*.hnsw_v3) to SQ8 quantized HNSW index (*-sq8.hnsw_v5).
# Usage: poetry run python tests/benchmark/data/scripts/convert_to_sq8.py

import argparse
import numpy as np
from VecSim import *
import os

# Paths
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
DATA_DIR = os.path.join(REPO_ROOT, 'tests', 'benchmark', 'data')

# Standalone SQ8 HNSW does not accept Cosine. Normalize cosine vectors before computing the
# quantization mean and inserting them, then use IP (cosine equals IP for normalized vectors).
# Consumers must normalize query vectors with the same convention before querying this index.
SOURCE_METRIC = VecSimMetric_Cosine
METRIC = VecSimMetric_IP
M = 64
EF_CONSTRUCTION = 512


def configure(dataset, data_type):
    global DIM, MULTI, N_LABELS, TYPE, INSERT_DTYPE, INPUT_INDEX, OUTPUT_INDEX
    prefix, DIM, MULTI, N_LABELS = {
        'dbpedia': ('dbpedia', 768, False, None),
        'fashion': ('fashion_images_multi_value', 512, True, 44441),
    }[dataset]
    TYPE, INSERT_DTYPE = {
        'fp32': (VecSimType_FLOAT32, np.float32),
        'fp16': (VecSimType_FLOAT16, np.float16),
    }[data_type]
    suffix = '-fp16' if data_type == 'fp16' else ''
    INPUT_INDEX = os.path.join(DATA_DIR, f'{prefix}-cosine-dim{DIM}-M64-efc512{suffix}.hnsw_v3')
    OUTPUT_INDEX = INPUT_INDEX.replace('.hnsw_v3', '-sq8.hnsw_v5')


def convert():
    print(f'Loading source index: {INPUT_INDEX}')
    source = HNSWIndex(INPUT_INDEX)
    n_vectors = source.index_size()
    print(f'  Loaded {n_vectors} vectors, dim={DIM}, multi={MULTI}')

    # Extract all vectors from the source index
    print('Extracting vectors...')

    if MULTI:
        # For multi-value indices, each label can have multiple vectors.
        # Discover labels by iterating and collecting non-empty results.
        n_labels = N_LABELS

        # Collect all vectors grouped by label
        label_vectors = {}  # label -> list of vectors
        total_extracted = 0
        for label in range(n_labels):
            vecs = source.get_vector(label)
            if vecs is not None and len(vecs) > 0:
                label_vectors[label] = vecs  # shape (num_vecs_for_label, dim)
                total_extracted += len(vecs)
            if label % 100000 == 0:
                print(f'  Extracted label {label}/{n_labels} ({total_extracted} vectors so far)')
        print(f'  Extracted {n_labels} labels, {total_extracted} vectors total')

        # Gather all vectors into a single array for computing the mean
        for label, vecs in label_vectors.items():
            label_vectors[label] = np.asarray(vecs, dtype=np.float32)
        all_vectors = np.vstack(list(label_vectors.values()))
    else:
        # Single-value index: one vector per label, labels are 0..n_vectors-1
        all_vectors = np.zeros((n_vectors, DIM), dtype=np.float32)
        for label in range(n_vectors):
            vecs = source.get_vector(label)
            all_vectors[label] = vecs[0]  # get_vector returns array of shape (1, dim)
            if label % 100000 == 0:
                print(f'  Extracted {label}/{n_vectors}')
        print(f'  Extracted {n_vectors}/{n_vectors}')

    del source

    if SOURCE_METRIC == VecSimMetric_Cosine:
        print('Normalizing cosine vectors for the IP SQ8 index...')
        norms = np.linalg.norm(all_vectors, axis=1, keepdims=True)
        np.divide(all_vectors, norms, out=all_vectors, where=norms != 0)
        if MULTI:
            offset = 0
            for label, vecs in label_vectors.items():
                count = len(vecs)
                label_vectors[label] = all_vectors[offset:offset + count]
                offset += count

    # Compute mean vector for SQ8 quantization
    print('Computing mean vector...')
    mean = all_vectors.mean(axis=0).astype(np.float32)

    # Create SQ8 HNSW index
    print('Creating SQ8 HNSW index...')
    params = HNSWParams()
    params.dim = DIM
    params.metric = METRIC
    params.multi = MULTI
    params.type = TYPE
    params.M = M
    params.efConstruction = EF_CONSTRUCTION
    params.quantType = VecSimQuant_SQ8
    sq8_index = HNSWIndex(params, quantization_mean=mean)

    # Add vectors
    print('Indexing vectors...')
    if MULTI:
        added = 0
        for label, vecs in label_vectors.items():
            for vec in vecs:
                sq8_index.add_vector(vec.astype(INSERT_DTYPE, copy=False), label)
                added += 1
            if label % 100000 == 0:
                print(f'  label {label}/{n_labels} ({added} vectors added)')
        print(f'  Done: {added} vectors added across {len(label_vectors)} labels')
    else:
        for label in range(n_vectors):
            sq8_index.add_vector(all_vectors[label].astype(INSERT_DTYPE, copy=False), label)
            if label % 100000 == 0:
                print(f'  {label}/{n_vectors}')
        print(f'  {n_vectors}/{n_vectors}')

    # Save
    print(f'Saving SQ8 index to: {OUTPUT_INDEX}')
    sq8_index.save_index(OUTPUT_INDEX)

    # Verify
    print('Verifying saved index...')
    loaded = HNSWIndex(OUTPUT_INDEX)
    if not loaded.check_integrity():
        raise RuntimeError('Converted SQ8 graph failed its integrity check')
    expected_vectors = n_vectors
    if loaded.index_size() != expected_vectors:
        raise RuntimeError(f'Expected {expected_vectors} vectors, got {loaded.index_size()}')
    file_size = os.path.getsize(OUTPUT_INDEX)
    print(f'  File size: {file_size / (1024**3):.2f} GB')
    print(f'  Vectors: {loaded.index_size()}')
    print('Done!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert benchmark cosine HNSW data to SQ8.')
    parser.add_argument('--dataset', choices=['dbpedia', 'fashion'], default='fashion')
    parser.add_argument('--type', choices=['fp32', 'fp16'], default='fp16', dest='data_type')
    args = parser.parse_args()
    configure(args.dataset, args.data_type)
    convert()
