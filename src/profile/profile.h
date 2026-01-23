/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "value.h"
#include "util/timeout.h"
#include "iterators/profile_iterator.h"

// Forward declarations
typedef struct ResultProcessor ResultProcessor;
typedef struct AREQ AREQ;
typedef struct HybridRequest HybridRequest;

#define printProfileType(vtype) RedisModule_ReplyKV_SimpleString(reply, "Type", (vtype))
#define printProfileTime(vtime) RedisModule_ReplyKV_Double(reply, "Time", (vtime))
#define printProfileIteratorCounter(vcount) RedisModule_ReplyKV_LongLong(reply, "Number of reading operations", (vcount))
#define printProfileRPCounter(vcount) RedisModule_ReplyKV_LongLong(reply, "Results processed", (vcount))
// For now we only print the total counter in order to avoid breaking the response format of profile
// If we get a chance to break it then consider splitting the count into separate fields
#define printProfileCounters(counters) printProfileIteratorCounter(counters->read + counters->skipTo - counters->eof)

#define printProfileGILTime(vtime) RedisModule_ReplyKV_Double(reply, "GIL-Time", (vtime))

#define printProfileNumBatches(hybrid_reader) \
  RedisModule_ReplyKV_LongLong(reply, "Batches number", (hybrid_reader)->numIterations)
#define printProfileMaxBatchSize(hybrid_reader) \
  RedisModule_ReplyKV_LongLong(reply, "Largest batch size", (hybrid_reader)->maxBatchSize)
#define printProfileMaxBatchIteration(hybrid_reader) \
  RedisModule_ReplyKV_LongLong(reply, "Largest batch iteration (zero based)", (hybrid_reader)->maxBatchIteration)
#define printProfileOptimizationType(oi) \
  RedisModule_ReplyKV_SimpleString(reply, "Optimizer mode", QOptimizer_PrintType((oi)->optim))
#define printProfileVectorSearchMode(searchMode) \
  RedisModule_ReplyKV_SimpleString(reply, "Vector search mode", VecSimSearchMode_ToString(searchMode))

/**
 * @brief Add profile iterators to all nodes in the iterator tree
 *
 * This recursively adds profile iterators to all nodes in the iterator tree.
 *
 * @param root The root iterator
 */
void Profile_AddIters(QueryIterator **root);

// Print the profile of a single shard
void Profile_Print(RedisModule_Reply *reply, void *ctx);

// Print the profile of a single shard in hybrid search
void Profile_PrintHybrid(RedisModule_Reply *reply, void *ctx);

#define PROFILE_STR "Profile"
#define PROFILE_SHARDS_STR "Shards"
#define PROFILE_COORDINATOR_STR "Coordinator"

void Profile_PrepareMapForReply(RedisModule_Reply *reply);

// A bitset of warnings
typedef uint8_t ProfileWarnings;

// Profile warnings - stored in AREQ profileCtx, printed in profile output.
// Not to be confused with QueryWarnings (query error/warning status in QueryError).
typedef enum {
  PROFILE_WARNING_TYPE_TIMEOUT = 1 << 0,
  PROFILE_WARNING_TYPE_MAX_PREFIX_EXPANSIONS = 1 << 1,
  PROFILE_WARNING_TYPE_QUERY_OOM = 1 << 2,
  PROFILE_WARNING_TYPE_BG_SCAN_OOM = 1 << 3,
  PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS = 1 << 4,
} ProfileWarningType;

// Compile-time assertion: ProfileWarnings is uint8_t (8 bits), so we can only have 8 warning types (bits 0-7)
// If you add more warning types, you must increase the size of ProfileWarnings (e.g., to uint16_t)
static_assert(PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS <= (1 << 7),
               "ProfileWarningType exceeds uint8_t bitset limit (max 8 warning types)");

static void ProfileWarnings_Add(ProfileWarnings *profileWarnings, ProfileWarningType code) {
  RS_ASSERT(profileWarnings);
  RS_ASSERT(code <= (1 << (sizeof(ProfileWarnings) * 8 - 1)));
  *profileWarnings |= code;
}

static bool ProfileWarnings_Has(const ProfileWarnings *profileWarnings, ProfileWarningType code) {
  RS_ASSERT(profileWarnings);
  RS_ASSERT(code <= (1 << (sizeof(ProfileWarnings) * 8 - 1)));
  return *profileWarnings & code;
}

typedef struct {
  ProfileWarnings warnings;
  // Number of cursor reads: 1 for the initial FT.AGGREGATE WITHCURSOR,
  // plus 1 for each subsequent FT.CURSOR READ call.
  size_t cursor_reads;
} ProfilePrinterCtx; // Context for the profile printing callback

typedef struct {
  /** Profile variables */
  rs_wall_clock initClock;                      // Time of start. Reset for each cursor call
  rs_wall_clock_ns_t profileTotalTime;          // Total time. Used to accumulate cursors times
  rs_wall_clock_ns_t profileQueueTime;          // Time spent waiting in workers thread pool queue
  rs_wall_clock_ns_t profileParseTime;          // Time for parsing the query
  rs_wall_clock_ns_t profilePipelineBuildTime;  // Time for creating the pipeline

  /** Coordinator dispatch time tracking */
  rs_wall_clock_ns_t coordStartTime;    // Coordinator: when command was received (for dispatch time calc)
  rs_wall_clock_ns_t coordDispatchTime; // Shard: dispatch latency from coordinator (for profile output)
} ProfileClocks;

// Type of request for profile printing
typedef enum {
  PROFILE_REQUEST_TYPE_AREQ,    // Standard AREQ request
  PROFILE_REQUEST_TYPE_HYBRID   // HybridRequest
} ProfileRequestType;

 // Tagged union for profile printing requests
typedef struct {
  ProfileRequestType type;
  union {
    AREQ *req;
    HybridRequest *hreq;
  };
} ProfileRequest;

void Profile_PrintDefault(RedisModule_Reply *reply, void *ctx);

typedef void (*ProfilePrinterCB)(RedisModule_Reply *reply, void *ctx);

void Profile_PrintInFormat(RedisModule_Reply *reply,
                           ProfilePrinterCB shards_cb, void *shards_ctx,
                           ProfilePrinterCB coordinator_cb, void *coordinator_ctx);

// Print result processors chain - useful for printing additional RP chains
void Profile_PrintResultProcessors(RedisModule_Reply *reply,
                                   ResultProcessor *rp, bool verbose);

// Extended version of Profile_PrintHybrid that allows adding extra content
// before closing the map
// extraCB is called after printing the main profile content but before closing
// the map
void Profile_PrintHybridExtra(RedisModule_Reply *reply, void *ctx,
                           ProfilePrinterCB extraCB, void *extraCtx);
