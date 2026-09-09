/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "redisearch_caps.h"

#include <stddef.h>

#include "version.h"

// An inclusive [min, max] span of `REDISEARCH_MODULE_VERSION`-encoded versions that
// share one minor release line, e.g. every 8.11.x patch.
typedef struct {
  int minInclusive;
  int maxInclusive;
} CapVersionRange;

// Encode a `major.minor` line's version floor/ceiling using the same
// major*10000 + minor*100 + patch scheme as REDISEARCH_MODULE_VERSION, spanning
// every patch (0-99) of that line.
#define MINOR_LINE_MIN(major, minor) ((major) * 10000 + (minor) * 100)
#define MINOR_LINE_MAX(major, minor) (MINOR_LINE_MIN((major), (minor)) + 99)

// Minor lines known to decode the row-block reply format (src/aggregate/row_block.h).
//
// The format has not shipped in a numbered release yet, so the only version known
// to be capable today is the development sentinel every master build reports
// (REDISEARCH_VERSION_{MAJOR,MINOR,PATCH} == 99.99.99, see version.h). When a
// release first ships the format, add its minor line here, e.g.
// `{MINOR_LINE_MIN(8, 11), MINOR_LINE_MAX(8, 11)}`. If the format is later
// backported to an older line, add a second entry for that line rather than
// widening this one's bounds — a single threshold could not express that without
// wrongly including every unpatched version of the older line's earlier point
// releases.
static const CapVersionRange kRowBlockCapableRanges[] = {
    {REDISEARCH_MODULE_VERSION, REDISEARCH_MODULE_VERSION},
};

bool RediSearchCaps_HasRowBlock(int moduleVersion) {
  // Fail closed on zero (never parsed / explicitly absent) and any negative
  // sentinel a caller might use for "unknown".
  if (moduleVersion <= 0) {
    return false;
  }
  for (size_t i = 0; i < sizeof(kRowBlockCapableRanges) / sizeof(kRowBlockCapableRanges[0]); i++) {
    if (moduleVersion >= kRowBlockCapableRanges[i].minInclusive &&
        moduleVersion <= kRowBlockCapableRanges[i].maxInclusive) {
      return true;
    }
  }
  return false;
}
