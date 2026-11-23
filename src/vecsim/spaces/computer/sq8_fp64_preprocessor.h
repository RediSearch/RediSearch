/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vecsim/spaces/computer/preprocessors.h"
#include "vecsim/types/sq8_fp64.h"

// Cosine inputs must already be normalized by the caller.
template <VecSimMetric Metric>
using SQ8FP64Preprocessor = QuantPreprocessor<double, Metric, false, vecsim_types::sq8_fp64>;
