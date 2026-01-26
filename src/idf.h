/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stddef.h>  // for size_t

double CalculateIDF(size_t totalDocs, size_t termDocs);

double CalculateIDF_BM25(size_t totalDocs, size_t termDocs);
