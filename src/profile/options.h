#pragma once
#include "pipeline/pipeline.h"
#include "aggregate/aggregate.h"

typedef enum {
  EXEC_NO_FLAGS = 0x00,
  EXEC_WITH_PROFILE = 0x01,
  EXEC_WITH_PROFILE_LIMITED = 0x02,
} ProfileOptions;

// Apply profile flags to request flags
// Returns true if any profile flags were applied
bool ApplyProfileFlags(QEFlags *flags, ProfileOptions profileOptions);

// Apply profile flags to request flags and query processing context
void ApplyProfileOptions(QueryProcessingCtx* qctx, QEFlags *flags, ProfileOptions profileOptions);