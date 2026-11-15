/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_DISK_GC_POLICY_H
#define RS_DISK_GC_POLICY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
  uint64_t dirtySinceMs;
  uint64_t lastAttemptEndMs;
  bool dirty;
  bool attempted;
} DiskGCPolicyState;

static inline bool DiskGC_CountReached(size_t writes, size_t deletes, size_t updates,
                                       size_t threshold) {
  if (writes >= threshold) return true;
  threshold -= writes;
  if (deletes >= threshold) return true;
  return updates >= threshold - deletes;
}

static inline void DiskGCPolicy_Observe(DiskGCPolicyState* state, bool dirty, uint64_t nowMs) {
  if (dirty && !state->dirty) state->dirtySinceMs = nowMs;
  state->dirty = dirty;
}

static inline bool DiskGCPolicy_CooledDown(const DiskGCPolicyState* state, uint64_t nowMs,
                                          uint64_t cooldownMs) {
  return !state->attempted ||
         (nowMs >= state->lastAttemptEndMs && nowMs - state->lastAttemptEndMs >= cooldownMs);
}

static inline bool DiskGCPolicy_Due(const DiskGCPolicyState* state, uint64_t nowMs,
                                   uint64_t cooldownMs, uint64_t maxAgeMs, size_t deleted,
                                   size_t deleteThreshold, bool l0Pressure) {
  if (!DiskGCPolicy_CooledDown(state, nowMs, cooldownMs)) return false;
  return l0Pressure || (deleted && deleted >= deleteThreshold) ||
         (state->dirty && nowMs >= state->dirtySinceMs && nowMs - state->dirtySinceMs >= maxAgeMs);
}

static inline void DiskGCPolicy_Completed(DiskGCPolicyState* state, uint64_t nowMs, bool success) {
  state->lastAttemptEndMs = nowMs;
  state->attempted = true;
  if (success) state->dirty = false;
}

#endif
