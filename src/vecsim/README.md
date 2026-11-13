# VectorSimilarity
Starting with version 8.0, RediSearch and this vector similarity library is an integral part of Redis. See https://github.com/redis/redis

This component exposes the vector similarity C API used by RediSearch.
Development and releases are managed in the RediSearch repository.

Run `make vecsim-tests` from the repository root, or run the commands below from
`src/vecsim`. Product flow tests live in RediSearch `tests/pytests`.
Allows creating indices of vectors and searching for top K similar to some vector in two methods: brute force, and by using the hnsw algorithm (probabilistic).

The API header files are `vec_sim.h` and `query_results.h`, which are located in `src/vecsim`.

## Algorithms

All of the algorithms in this library are designed to work inside RediSearch and support the following features:
1. In place insert, delete, and update vectors in the index.
2. KNN queries - results can be ordered by score or ID.
3. Iterator interface for consecutive KNN queries.
4. Range queries
5. Multiple vector indexing for the same label (multi-value indexing)
6. 3rd party allocators

### Flat (Brute Force)

Brute force comparison of the query vector `q` with the stored vectors. Vectors are stored in vector blocks, which are contiguous memory blocks with a configurable size.


### HNSW
Modified implementation of [hnswlib](https://github.com/nmslib/hnswlib). Modified to accommodate the above feature set.

## Metrics
We support three popular distance metrics to measure the degree of similarity between two vectors:

| Distance metric | Description                                                    | Value range      |
|-----------------|----------------------------------------------------------------|------------------|
| L2              | Euclidean distance between two vectors.                        | [0, +∞)          |
| IP              | Inner product distance (vectors are assumed to be normalized). | [0, 2]           |
| COSINE          | Cosine distance of two vectors.                                | [0, 2]           |

The above metrics calculate the distance between two vectors, where smaller values indicate that the vectors are closer in the vector space.

### Datatypes and SIMD support
The library supports the following data types for distance computation:
* `float32`
* `float64`
* `bfloat16`
* `float16`
* `int8`
* `uint8`

To accelerate performance, we leverage SIMD instructions on both x86 and ARM CPUs.
The tables below detail the supported SIMD instruction sets (CPU flags) used for each data type on each architecture.

#### x86_64 SIMD Support
| Operation          | CPU flags                                                   |
|--------------------|---------------------------------------------------------------------|
| FP32 IP & Cosine   | SSE, AVX, AVX512F                                                  |
| FP32 L2 distance   | SSE, AVX, AVX512F                                                  |
| FP64 IP & Cosine   | SSE, AVX, AVX512F                                                  |
| FP64 L2 distance   | SSE, AVX, AVX512F                                                  |
| FP16 IP & Cosine   | F16C+FMA+AVX, AVX512F, AVX512FP16+AVX512VL                         |
| FP16 L2 distance   | F16C+FMA+AVX, AVX512F, AVX512FP16+AVX512VL                         |
| BF16 IP & Cosine   | SSE3, AVX2, AVX512BW+AVX512VBMI2, AVX512BF16+AVX512VL              |
| BF16 L2 distance   | SSE3, AVX2, AVX512BW+AVX512VBMI2           |
| INT8 IP & Cosine   | AVX512F+AVX512BW+AVX512VL+AVX512VNNI                               |
| INT8 L2 distance   | AVX512F+AVX512BW+AVX512VL+AVX512VNNI                               |
| UINT8 IP & Cosine  | AVX512F+AVX512BW+AVX512VL+AVX512VNNI                               |
| UINT8 L2 distance  | AVX512F+AVX512BW+AVX512VL+AVX512VNNI                               |

#### ARM SIMD Support (arm64v8 & Apple Silicon)
| Operation           | CPU flags                             |
|---------------------|-------------------------------------------|
| FP32 IP & Cosine    | NEON, SVE, SVE2                           |
| FP32 L2 distance    | NEON, SVE, SVE2                           |
| FP64 IP & Cosine    | NEON, SVE, SVE2                           |
| FP64 L2 distance    | NEON, SVE, SVE2                           |
| FP16 IP & Cosine    | NEON_HP, SVE, SVE2                        |
| FP16 L2 distance    | NEON_HP, SVE, SVE2                        |
| BF16 IP & Cosine    | NEON_BF16, SVE_BF16                       |
| BF16 L2 distance    | NEON_BF16, SVE_BF16                       |
| INT8 IP & Cosine    | NEON, NEON_DOTPROD, SVE, SVE2            |
| INT8 L2 distance    | NEON, NEON_DOTPROD, SVE, SVE2            |
| UINT8 IP & Cosine   | NEON, NEON_DOTPROD, SVE, SVE2            |
| UINT8 L2 distance   | NEON, NEON_DOTPROD, SVE, SVE2            |

## Build
For building you will need:
1. Python 3 as `python` (either by creating a virtual environment or setting your system python to point to the right python distribution)
2. gcc >= 11
3. cmake version >= 3.10. (To build the `python bindings` you will need cmake < 3.26 due to `pybind11` policy version handling).

To build the main library and unit tests in one command run

```
make
```

## Unit tests
To execute unit tests run

```
make unit_test
```

## Memory check

To run the unit tests with AddressSanitizer run
```
make unit_test SAN=address
```

## Python bindings

These are developer tools built from a complete Search checkout. Python package
CI and standalone source-distribution support are not maintained. The CMake build
uses Search's sibling test utilities and SVS checkout.

To build the Python wheel, first create a dedicated virtualenv using Python 3.10 or 3.11. Then, activate the environment, install the dependencies, and build the package. Please note, due to the way poetry generates a setup.py, you may have to erase it before re-running *poetry build*.

```
python -m venv venv
source venv/bin/activate
pip install poetry
poetry install
poetry build
```

To run in debug mode, replace the last two lines with:

```
DEBUG=1 poetry install
DEBUG=1 poetry build
```

After building the wheel, if you want to use the package you built, you will need to manually execute a *pip install dist/<package>.whl*. Remember to replace <package> with the complete package name.

## Benchmark

To benchmark the capabilities of this library, follow the instructions in the [benchmarks user guide](../../docs/vecsim/benchmarks.md).
If you'd like to create your own benchmarks, you can find more information in the [developer guide](../../docs/vecsim/benchmarks_developer.md).


## License

Starting with Redis 8, this library is licensed under your choice of: (i) Redis Source Available License 2.0 (RSALv2); (ii) the Server Side Public License v1 (SSPLv1); or (iii) the GNU Affero General Public License version 3 (AGPLv3). Please review the license folder for the full license terms and conditions. Prior versions remain subject to (i) and (ii).

## Code contributions


By contributing code to this Redis module in any form, including sending a pull request via GitHub, a code fragment or patch via private email or public discussion groups, you agree to release your code under the terms of the Redis Software Grant and Contributor License Agreement. Please see the CONTRIBUTING.md file in this source distribution for more information. For security bugs and vulnerabilities, please see SECURITY.md.
