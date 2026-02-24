/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "profile_iterator.h"
#include "rmalloc.h"
#include <time.h>

struct ProfileCounters {
  size_t read;
  size_t skipTo;
  int eof;
};

struct ProfileIterator {
  QueryIterator base;
  QueryIterator *child;
  ProfileCounters counters;
  rs_wall_clock_ns_t wallTime; // This field serves as a time accumulator, so using rs_wall_clock_ns_t is required.
};

static inline void SyncWithChild(ProfileIterator *pi) {
  pi->base.current = pi->child->current;
  pi->base.lastDocId = pi->child->lastDocId;
  pi->base.atEOF = pi->child->atEOF;
}

static IteratorStatus PI_Read(struct QueryIterator *self) {
  ProfileIterator *pi = (ProfileIterator *)self;
  rs_wall_clock begin;
  rs_wall_clock_init(&begin);
  pi->counters.read++;

  IteratorStatus ret = pi->child->Read(pi->child);
  if (ret == ITERATOR_EOF) {
    pi->counters.eof = 1;
  }

  // Copy the current result from the child
  SyncWithChild(pi);

  pi->wallTime += rs_wall_clock_elapsed_ns(&begin);
  return ret;
}

static IteratorStatus PI_SkipTo(struct QueryIterator *self, t_docId docId) {
  ProfileIterator *pi = (ProfileIterator *)self;
  rs_wall_clock begin;
  rs_wall_clock_init(&begin);
  pi->counters.skipTo++;

  IteratorStatus ret = pi->child->SkipTo(pi->child, docId);
  if (ret == ITERATOR_EOF) {
    pi->counters.eof = 1;
  }

  // Copy the current result from the child
  SyncWithChild(pi);

  pi->wallTime += rs_wall_clock_elapsed_ns(&begin);
  return ret;
}

static void PI_Free(struct QueryIterator *self) {
  ProfileIterator *pi = (ProfileIterator *)self;
  pi->child->Free(pi->child);
  rm_free(self);
}

static size_t PI_NumEstimated(struct QueryIterator *self) {
  ProfileIterator *pi = (ProfileIterator *)self;
  return pi->child->NumEstimated(pi->child);
}

static void PI_Rewind(struct QueryIterator *self) {
  ProfileIterator *pi = (ProfileIterator *)self;
  pi->child->Rewind(pi->child);
  SyncWithChild(pi);
}

static ValidateStatus PI_Revalidate(struct QueryIterator *self) {
  ProfileIterator *pi = (ProfileIterator *)self;
  ValidateStatus val = pi->child->Revalidate(pi->child);
  if (val == VALIDATE_MOVED) {
    SyncWithChild(pi);
  }
  return val;
}

// ProfileCounters getters
size_t ProfileCounters_GetReadCount(ProfileCounters *c) {
  return c->read;
}

size_t ProfileCounters_GetSkipToCount(ProfileCounters *c) {
  return c->skipTo;
}

int ProfileCounters_GetEof(ProfileCounters *c) {
  return c->eof;
}

// ProfileIterator getters
QueryIterator *ProfileIterator_GetChild(ProfileIterator *pi) {
  return pi->child;
}

ProfileCounters *ProfileIterator_GetCounters(ProfileIterator *pi) {
  return &pi->counters;
}

rs_wall_clock_ns_t ProfileIterator_GetWallTimeNs(ProfileIterator *pi) {
  return pi->wallTime;
}

/* Create a new profile iterator */
QueryIterator *NewProfileIterator(QueryIterator *child) {
  ProfileIterator *pc = rm_calloc(1, sizeof(*pc));
  pc->child = child;
  pc->counters.read = 0;
  pc->counters.skipTo = 0;
  pc->wallTime = 0;
  pc->counters.eof = 0;

  QueryIterator *ret = &pc->base;
  ret->type = PROFILE_ITERATOR;
  SyncWithChild(pc);
  ret->Free = PI_Free;
  ret->Read = PI_Read;
  ret->SkipTo = PI_SkipTo;
  ret->NumEstimated = PI_NumEstimated;
  ret->Rewind = PI_Rewind;
  ret->Revalidate = PI_Revalidate;

  return ret;
}
