# SVS index serializer for benchmarks.
# Serializes datasets to SVS index format for use by C++ and Python benchmarks.

import numpy as np
import VecSim
import h5py
import os

# Determine working directory
location = os.path.abspath('.')
if location.endswith('/data'):
    location = os.path.join(location, '')
elif location.endswith('/VectorSimilarity'):
    location = os.path.join(location, 'tests', 'benchmark', 'data', '')
else:
    print('unexpected location:', location)
    print('expected to be in `./VectorSimilarity/tests/benchmark/data` or `./VectorSimilarity`')
    exit(1)
print('working at:', location)

DEFAULT_FILES = [
    {
        'filename': 'dbpedia-768',
        'nickname': 'dbpedia',
        'dim': 768,
        'metric': VecSim.VecSimMetric_Cosine,
        'hdf5_file': 'dbpedia-cosine-dim768.hdf5',
    },
    {
        'filename': 'fashion_images_multi_value',
        'nickname': 'fashion_images_multi_value',
        'hdf5_file': 'fashion_images_multi_value-cosine-dim512.hdf5',
        'dim': 512,
        'metric': VecSim.VecSimMetric_Cosine,
        'multi': True,
    },
]

TYPES_ATTR = {
    VecSim.VecSimType_FLOAT32: {"size_in_bytes": 4, "vector_type": np.float32},
}

def load_vectors_and_labels_from_hdf5(input_file):
    """
    Load vectors and labels from an HDF5 file.
    Returns: (vectors, labels) numpy arrays, or (None, None) on failure.
    """
    try:
        with h5py.File(input_file, 'r') as f:
            vectors = f['vectors'][:]
            labels = f['labels'][:]

            print(f"Loaded {input_file}: vectors {vectors.shape}, labels {labels.shape}")
            return vectors, labels

    except Exception as e:
        print(f"Error loading HDF5 file: {e}")
        return None, None

def serialize(files=DEFAULT_FILES):
    for file in files:
        filename = file['filename']
        nickname = file.get('nickname', filename)
        dim = file.get('dim', None)
        metric = file['metric']
        is_multi = file.get('multi', False)
        vec_type = file.get('type', VecSim.VecSimType_FLOAT32)

        # Load vectors/labels
        hdf5_file = file.get('hdf5_file', f"{filename}.hdf5")
        hdf5_path = os.path.join(location, hdf5_file)
        print(f"Loading vectors from {hdf5_path}")

        if is_multi:
            if vectors.ndim == 3:
                vectors = vectors.reshape(-1, vectors.shape[-1])
                labels = np.repeat(labels, vectors.shape[0] // labels.shape[0])

        if vectors is None or labels is None:
            print(f"Failed to load data from {hdf5_path}, skipping...")
            continue

        # Handle shape (N, 1, D) -> (N, D)
        if not is_multi:
            if vectors.ndim == 3 and vectors.shape[1] == 1:
                vectors = vectors.squeeze(axis=1)
            elif vectors.ndim != 2:
                print(f"Error: Expected 2D vectors, got shape {vectors.shape}")
                continue

        # Update dimension if not specified
        if dim is None:
            dim = vectors.shape[1]
            print(f"Auto-detected dimension: {dim}")

        assert dim == vectors.shape[1], f"Dimension mismatch: {dim} != {vectors.shape[1]}"

        # Create SVS parameters
        bits_to_str = {
            VecSim.VecSimSvsQuant_NONE: '_none',
            VecSim.VecSimSvsQuant_8: '_8',
        }
        for bits in [VecSim.VecSimSvsQuant_8, VecSim.VecSimSvsQuant_NONE]:
            svs_params = VecSim.SVSParams()
            svs_params.type = vec_type
            svs_params.dim = dim
            svs_params.metric = metric
            svs_params.graph_max_degree = file.get('graph_max_degree', 128)
            svs_params.construction_window_size = file.get('construction_window_size', 512)
            svs_params.quantBits = bits
            svs_params.multi = is_multi


            print(f"Creating SVS index for {filename} (dim={dim}, metric={metric})")
            if vectors.dtype != np.float32:
                print(f"Converting vectors from {vectors.dtype} to float32")
                vectors = vectors.astype(np.float32)
            if labels.dtype != np.uint64:
                print(f"Converting labels from {labels.dtype} to uint64")
                labels = labels.astype(np.uint64)

            # Create index and add vectors
            svs_index = VecSim.SVSIndex(svs_params)
            print(f"Adding {len(vectors)} vectors...")
            svs_index.add_vector_parallel(vectors, labels)

            # Save index
            dir = os.path.join(location, nickname + '_svs'+bits_to_str[bits])
            os.makedirs(dir, exist_ok=True)
            svs_index.save_index(dir)
            print(f"Index saved to {dir}")
            print(f"Final index size: {svs_index.index_size()}")

            # Verify
            print("Verifying saved index...")
            svs_index_verify = VecSim.SVSIndex(svs_params)
            svs_index_verify.load_index(dir)
            svs_index_verify.check_integrity()
            print(f"Verified index size: {svs_index_verify.index_size()}")
            assert svs_index_verify.get_labels_count() == labels.max() + 1


if __name__ == '__main__':
    serialize()
