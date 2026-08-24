/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once
#include "pipeline/pipeline.h"
#include "aggregate/aggregate.h"

typedef enum {
  EXEC_NO_FLAGS = 0x00,
  EXEC_WITH_PROFILE = 0x01,
  EXEC_WITH_PROFILE_LIMITED = 0x02,
  EXEC_DEBUG = 0x04,
} ProfileOptions;

// Apply profile flags to request flags
// Returns true if any profile flags were applied
bool ApplyProfileFlags(QEFlags *flags, ProfileOptions profileOptions);

// Apply profile flags to request flags and query processing context
void ApplyProfileOptions(QueryProcessingCtx* qctx, QEFlags *flags, ProfileOptions profileOptions);
