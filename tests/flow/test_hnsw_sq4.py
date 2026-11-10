# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
import pytest
from common import *

DIM = 33
NUM_VECTORS = 500
NUM_QUERIES = 20
K = 10
MIN_RECALL = 0.7


def make_sq4_params(metric, data_type=VecSimType_FLOAT32):
    params = create_hnsw_params(dim=DIM, num_elements=NUM_VECTORS, metric=metric,
                                data_type=data_type, ef_construction=200, m=16, ef_runtime=200)
    params.quantType = VecSimQuant_SQ4
    return params


def brute_force_labels(data, queries, metric):
    if metric == VecSimMetric_L2:
        dists = ((queries[:, None, :] - data[None, :, :]) ** 2).sum(axis=2)
    else:
        dists = 1.0 - queries @ data.T
    return np.argsort(dists, axis=1)[:, :K]


@pytest.mark.parametrize("metric", [
    pytest.param(VecSimMetric_IP, id="ip"),
    pytest.param(VecSimMetric_L2, id="l2"),
])
@pytest.mark.parametrize("centered", [False, True], ids=["plain", "centered"])
def test_sq4_hnsw_plain_and_centered(metric, centered):
    rng = np.random.default_rng(42)
    data = rng.random((NUM_VECTORS, DIM), dtype=np.float32)
    queries = rng.random((NUM_QUERIES, DIM), dtype=np.float32)
    if metric == VecSimMetric_IP:
        data /= np.linalg.norm(data, axis=1, keepdims=True)
        queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    params = make_sq4_params(metric)
    mean = data.mean(axis=0).astype(np.float32) if centered else None
    index = HNSWIndex(params, quantization_mean=mean)
    for label, vec in enumerate(data):
        index.add_vector(vec, label)
    assert index.index_size() == NUM_VECTORS

    expected = brute_force_labels(data, queries, metric)
    hits = 0
    for i, query in enumerate(queries):
        labels, distances = index.knn_query(query, K)
        assert np.isfinite(distances).all(), distances
        hits += len(set(labels[0]) & set(expected[i]))
    recall = hits / (NUM_QUERIES * K)
    assert recall >= MIN_RECALL, recall


def test_sq4_rejects_fp16_and_cosine():
    with pytest.raises(ValueError, match="Unsupported vector index parameters"):
        HNSWIndex(make_sq4_params(VecSimMetric_L2, VecSimType_FLOAT16))
    with pytest.raises(ValueError, match="Unsupported vector index parameters"):
        HNSWIndex(make_sq4_params(VecSimMetric_Cosine))
