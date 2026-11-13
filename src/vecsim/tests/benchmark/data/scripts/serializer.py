# HNSW index serializer for the benchmark.
# This script is used to serialize datasets to the HNSW index format. The serialized files are used
# by the C++ and Python benchmarks. Note that the serialized files that are used by the Python
# benchmarks by default have the same names as the serialized files this script generates.
# You can use `bm_datasets.py` to generate the datasets it uses.

import numpy as np
from VecSim import *
import h5py
import os
from bfloat16 import bfloat16

CUR_SERIALIZING_VERSION = 3

location = os.path.abspath('.')
print('at: ', location)
if location.endswith('/data'):
    location = os.path.join(location, '')
elif location.endswith('/VectorSimilarity'):
    location = os.path.join(location, 'tests', 'benchmark', 'data', '')
else:
    print('unexpected location: ', location)
    print('expected to be in `./VectorSimilarity/tests/benchmark/data` or `./VectorSimilarity`')
    exit(1)
print('working at: ', location)

# Default files to serialize. Each file is expected to be a dictionary with the following attributes:
# REQUIRED:
#   filename:        HDF5 file, that contains the train and test sets. Should be in the data directory.
#   metric:          metric of the vectors in the file, from the VecSimMetric enum.
# OPTIONAL:
#   nickname:        the prefix of the output file, defaults to filename.
#   dim:             dimension of the vectors in the file, defaults to the length of the first vector.
#   type:            type of the vectors in the file, from the VecSimType enum. defaults to VecSimType_FLOAT32.
#   M:               M parameter of the HNSW index, defaults to 64.
#   efConstruction:  efConstruction parameter of the HNSW index, defaults to 512.
#   multi:           whether the vectors in the file are multi-value or not, defaults to False.
#   skipRaw:         whether to skip exporting the test set to a raw file, defaults to False (raw file is exported).

DEFAULT_FILES = [
    {
        'filename': 'dbpedia-768',
        'nickname': 'dbpedia',
        'dim': 768,
        'metric': VecSimMetric_Cosine,
    },
    {
        'filename': 'dbpedia-768',
        'nickname': 'dbpedia',
        'dim': 768,
        'type': VecSimType_FLOAT64,
        'metric': VecSimMetric_Cosine,
    },
    {
        'filename': 'dbpedia-768',
        'nickname': 'dbpedia',
        'dim': 768,
        'type': VecSimType_BFLOAT16,
        'metric': VecSimMetric_Cosine,
    },
    {
        'filename': 'dbpedia-768',
        'nickname': 'dbpedia',
        'dim': 768,
        'type': VecSimType_FLOAT16,
        'metric': VecSimMetric_Cosine,
    },
    {
        'filename': 'fashion_images_multi_value',
        'metric': VecSimMetric_Cosine,
        'multi': True,
    },
    {
        'filename': 'fashion_images_multi_value',
        'metric': VecSimMetric_Cosine,
        'type': VecSimType_FLOAT64,
        'multi': True,
    },
    {
        'filename': 'fashion_images_multi_value',
        'metric': VecSimMetric_Cosine,
        'type': VecSimType_BFLOAT16,
        'multi': True,
    },
    {
        'filename': 'fashion_images_multi_value',
        'metric': VecSimMetric_Cosine,
        'type': VecSimType_FLOAT16,
        'multi': True,
    },
    {
        'filename': 'glove-25-angular',
        'nickname': 'glove',
        'M': 16,
        'efConstruction': 100,
        'metric': VecSimMetric_Cosine,
        'skipRaw': True,
    },
    {
        'filename': 'glove-50-angular',
        'nickname': 'glove',
        'M': 24,
        'efConstruction': 150,
        'metric': VecSimMetric_Cosine,
        'skipRaw': True,
    },
    {
        'filename': 'glove-100-angular',
        'nickname': 'glove',
        'M': 36,
        'efConstruction': 250,
        'metric': VecSimMetric_Cosine,
        'skipRaw': True,
    },
    {
        'filename': 'glove-200-angular',
        'nickname': 'glove',
        'M': 48,
        'efConstruction': 350,
        'metric': VecSimMetric_Cosine,
        'skipRaw': True,
    },
    {
        'filename': 'mnist-784-euclidean',
        'nickname': 'mnist',
        'M': 32,
        'efConstruction': 150,
        'metric': VecSimMetric_L2,
        'skipRaw': True,
    },
    {
        'filename': 'sift-128-euclidean',
        'nickname': 'sift',
        'M': 32,
        'efConstruction': 200,
        'metric': VecSimMetric_L2,
        'skipRaw': True,
    },
    {
        'filename': 'wikipedia_single-1024_eng_v3',
        'nickname': 'wipedia_single',
        'dim': 1024,
        'type': VecSimType_INT8,
        'metric': VecSimMetric_Cosine,
    },
    {
        'filename': 'wikipedia_single-1024_eng_v3_uint8',
        'nickname': 'wipedia_single_uint8',
        'dim': 1024,
        'type': VecSimType_UINT8,
        'metric': VecSimMetric_Cosine,
    },
    {
        'filename': 'wikipedia_multi-1024_eng_v3_uint8',
        'nickname': 'wipedia_multi_uint8',
        'dim': 1024,
        'type': VecSimType_UINT8,
        'metric': VecSimMetric_Cosine,
        'multi': True
    },
]

TYPES_ATTR = {
    VecSimType_FLOAT32: {"size_in_bytes": 4, "vector_type": np.float32},
    VecSimType_FLOAT64: {"size_in_bytes": 8, "vector_type": np.float64},
    VecSimType_BFLOAT16: {"size_in_bytes": 2, "vector_type": bfloat16},
    VecSimType_FLOAT16: {"size_in_bytes": 2, "vector_type": np.float16},
    VecSimType_INT8: {"size_in_bytes": 1, "vector_type": np.int8}
}


def check_file(file_name, vecsim_type, dim, num_vectors_to_print=10):
    size_in_bytes = os.path.getsize(os.path.join(location, file_name))
    print(f"File size: {size_in_bytes/1000}k bytes")
    np.set_printoptions(threshold=20)

    type_attr = TYPES_ATTR[vecsim_type]
    print(f"first {num_vectors_to_print} vectors in the file:")
    with open(os.path.join(location, file_name), 'rb') as vecsfile:
        vector_size_in_bytes = type_attr["size_in_bytes"] * dim
        vecs = vecsfile.read(num_vectors_to_print * vector_size_in_bytes)
        for i in range(num_vectors_to_print):
            vec = np.frombuffer(
                vecs[i*vector_size_in_bytes:(i+1)*vector_size_in_bytes], dtype=type_attr["vector_type"])
            print(vec)


def serialize(files=DEFAULT_FILES):
    for file in files:
        filename = file['filename']
        nickname = file.get('nickname', filename)
        print('working on: ', filename)

        with h5py.File(os.path.join(location, filename + '.hdf5'), 'r') as f:
            hnswparams = HNSWParams()

            hnswparams.multi = file.get('multi', False)
            hnswparams.dim = file.get('dim', len(f['train'][0][0]) if hnswparams.multi else len(f['train'][0]))
            hnswparams.metric = file['metric'] # must be set
            hnswparams.type = file.get('type', VecSimType_FLOAT32)
            hnswparams.M = file.get('M', 64)
            hnswparams.efConstruction = file.get('efConstruction', 512)

            metric = {VecSimMetric_Cosine: 'cosine', VecSimMetric_L2: 'euclidean'}[hnswparams.metric]
            serialized_raw_name = '%s-%s-dim%d' % (nickname, metric, hnswparams.dim)
            serialized_file_name = serialized_raw_name + '-M%d-efc%d' % (hnswparams.M, hnswparams.efConstruction)
            if hnswparams.type == VecSimType_FLOAT64:
                serialized_file_name = serialized_file_name + '-fp64'
                serialized_raw_name = serialized_raw_name + '-fp64'
            elif hnswparams.type == VecSimType_BFLOAT16:
                serialized_file_name = serialized_file_name + '-bf16'
                serialized_raw_name = serialized_raw_name + '-bf16'
            elif hnswparams.type == VecSimType_FLOAT16:
                serialized_file_name = serialized_file_name + '-fp16'
                serialized_raw_name = serialized_raw_name + '-fp16'
            elif hnswparams.type == VecSimType_INT8:
                serialized_file_name = serialized_file_name + '-int8'
                serialized_raw_name = serialized_raw_name + '-int8'
            print('first, exporting test set to binary')
            if not file.get('skipRaw', False):
                test = f['test']
                if hnswparams.type == VecSimType_FLOAT64:
                    test = test.astype(np.float64)
                elif hnswparams.type == VecSimType_BFLOAT16:
                    test_set = test[:]
                    test = np.array(test_set, dtype=bfloat16)
                elif hnswparams.type == VecSimType_FLOAT16:
                    test = test.astype(np.float16)
                elif hnswparams.type == VecSimType_INT8:
                    test = test.astype(np.int8)
                print(f"creating test set of {len(test)} vectors")
                with open(os.path.join(location, serialized_raw_name + '-test_vectors.raw'), 'wb') as testfile:
                    for vec in test:
                        testfile.write(vec.tobytes())
                check_file(serialized_raw_name + '-test_vectors.raw',
                           hnswparams.type, hnswparams.dim)
            else:
                print('nevermind, skipping test set export')

            print('second, indexing train set')

            index = HNSWIndex(hnswparams)

            data = f['train']
            if hnswparams.type == VecSimType_FLOAT64:
                data = data.astype(np.float64)
            elif hnswparams.type == VecSimType_BFLOAT16:
                data_set = data[:]
                data = np.array(data_set, dtype=bfloat16)
            elif hnswparams.type == VecSimType_FLOAT16:
                data = data.astype(np.float16)
            elif hnswparams.type == VecSimType_INT8:
                data = data.astype(np.int8)
            print(f"creating index with {hnswparams.initialCapacity} vectors")
            for label, cur in enumerate(data):
                for vec in cur if hnswparams.multi else [cur]:
                    index.add_vector(vec, label)
                    print(label, len(data), sep='/', end='\r')
            print(len(data), len(data), sep='/')

        # Finished indexing, going back an indentation level so we close the input file

        print('lastly, serializing the index')
        output = os.path.join(location, serialized_file_name + '.hnsw_v' + str(CUR_SERIALIZING_VERSION))
        index.save_index(output)
        print('saved to: ', output)

        # Sanity check - attempt to load the index
        index = HNSWIndex(output)
        index.check_integrity()
        file_size_in_bytes = os.path.getsize(output)
        print(f"Serialized index file size: {file_size_in_bytes/(1024**3)}GB")

        # Release memory before next iteration
        del index

if __name__ == '__main__':
    serialize()
