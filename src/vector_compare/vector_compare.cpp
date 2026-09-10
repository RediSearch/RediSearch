/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "vector_compare.h"

#include "VecSim/vec_sim_index.h"
#include "VecSim/vec_sim_tiered_index.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"

#include <vector>

namespace {

/**
 * Compare the vector(s) stored under `label` against `blobs`, order-insensitively.
 *
 * `getDataByLabel` is typed while the index handle is not, so the caller below dispatches on
 * the index's data type -- the same shape `VecSimDebug_GetElementNeighborsInHNSWGraph` uses
 * for the same reason.
 *
 * A cosine index stores the blob normalized, so a copy is normalized before comparing;
 * without that, an unchanged vector would compare unequal every time. Only the elements take
 * part: `getDataByLabel` omits any trailing norm, and the norm is a function of the elements
 * anyway.
 *
 * Order-insensitive because a tiered index's `getDataByLabel` returns frontend vectors
 * before backend ones, not insertion order, so a multi-value label split across tiers can
 * list the same vectors in a different order than `blobs`. Each caller blob consumes one
 * matching stored entry (`matched`) rather than testing membership in a deduplicated set, so
 * a real change to how many times a value repeats is still caught: with the size check above
 * already requiring equal counts, consuming one match per blob is what tells `[A, A]` apart
 * from `[A, B]`, where a byte-deduplicated comparison would see `{A}` either way.
 *
 * A vector that has been written to the backend but not yet removed from the frontend during
 * an in-flight ingest is a false negative -- it is counted twice in `stored`, against `blobs`'
 * one, so this reports 'changed' rather than relabeling.
 */
template <typename DataType, typename DistType>
bool holdsVectors(VecSimIndex *index, const VecSimIndexBasicInfo &info, size_t label,
                  const void *blobs, size_t numBlobs) {
  std::vector<std::vector<DataType>> stored;
  if (info.isTiered) {
    dynamic_cast<VecSimTieredIndex<DataType, DistType> *>(index)->getDataByLabel(label, stored);
  } else {
    dynamic_cast<VecSimIndexAbstract<DataType, DistType> *>(index)->getDataByLabel(label, stored);
  }
  // Absent, or holding a different number of vectors than the caller offers.
  if (stored.size() != numBlobs) {
    return false;
  }

  const size_t elementsSize = info.dim * sizeof(DataType);
  const bool normalize = info.metric == VecSimMetric_Cosine;
  std::vector<char> scratch(
      normalize ? VecSimParams_GetQueryBlobSize(info.type, info.dim, info.metric) : 0);

  std::vector<bool> matched(stored.size(), false);
  const char *blob = static_cast<const char *>(blobs);
  for (size_t i = 0; i < numBlobs; ++i, blob += elementsSize) {
    const void *comparand = blob;
    if (normalize) {
      memcpy(scratch.data(), blob, elementsSize);
      VecSim_Normalize(scratch.data(), info.dim, info.type);
      comparand = scratch.data();
    }
    bool found = false;
    for (size_t j = 0; j < stored.size(); ++j) {
      if (!matched[j] && memcmp(stored[j].data(), comparand, elementsSize) == 0) {
        matched[j] = true;
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }
  }
  return true;
}

}  // namespace

extern "C" bool VectorIndex_HoldsVectors(VecSimIndex *index, size_t label, const void *blobs,
                                         size_t numBlobs) {
  if (!index || !blobs || numBlobs == 0) {
    return false;
  }
  const VecSimIndexBasicInfo info = index->basicInfo();
  switch (info.type) {
    case VecSimType_FLOAT32:
      return holdsVectors<float, float>(index, info, label, blobs, numBlobs);
    case VecSimType_FLOAT64:
      return holdsVectors<double, double>(index, info, label, blobs, numBlobs);
    case VecSimType_BFLOAT16:
      return holdsVectors<vecsim_types::bfloat16, float>(index, info, label, blobs, numBlobs);
    case VecSimType_FLOAT16:
      return holdsVectors<vecsim_types::float16, float>(index, info, label, blobs, numBlobs);
    case VecSimType_INT8:
      return holdsVectors<int8_t, float>(index, info, label, blobs, numBlobs);
    case VecSimType_UINT8:
      return holdsVectors<uint8_t, float>(index, info, label, blobs, numBlobs);
    default:
      // A data type this function has not been taught: reindex rather than guess.
      return false;
  }
}
