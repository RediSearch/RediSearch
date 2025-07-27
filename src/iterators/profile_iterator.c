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

static IteratorStatus PI_Read(struct QueryIterator *self) {
  ProfileIterator *pi = (ProfileIterator *)self;
  clock_t begin = clock();
  pi->counters.read++;

  IteratorStatus ret = pi->child->Read(pi->child);
  if (ret == ITERATOR_EOF) {
    pi->counters.eof = 1;
  }

  // Copy the current result from the child
  self->current = pi->child->current;
  self->lastDocId = pi->child->lastDocId;
  self->atEOF = pi->child->atEOF;

  pi->cpuTime += clock() - begin;
  return ret;
}

static IteratorStatus PI_SkipTo(struct QueryIterator *self, t_docId docId) {
  ProfileIterator *pi = (ProfileIterator *)self;
  clock_t begin = clock();
  pi->counters.skipTo++;

  IteratorStatus ret = pi->child->SkipTo(pi->child, docId);
  if (ret == ITERATOR_EOF) {
    pi->counters.eof = 1;
  }

  // Copy the current result from the child
  self->current = pi->child->current;
  self->lastDocId = pi->child->lastDocId;
  self->atEOF = pi->child->atEOF;

  pi->cpuTime += clock() - begin;
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
  self->lastDocId = pi->child->lastDocId;
  self->current = pi->child->current;
  self->atEOF = pi->child->atEOF;
}

static ValidateStatus PI_Revalidate(struct QueryIterator *self) {
  ProfileIterator *pi = (ProfileIterator *)self;
  ValidateStatus val = pi->child->Revalidate(pi->child);
  if (val == VALIDATE_MOVED) {
    self->lastDocId = pi->child->lastDocId;
    self->current = pi->child->current;
    self->atEOF = pi->child->atEOF;
  }
  return val;
}

/* Create a new profile iterator */
QueryIterator *NewProfileIterator(QueryIterator *child) {
  ProfileIterator *pc = rm_calloc(1, sizeof(*pc));
  pc->child = child;
  pc->counters.read = 0;
  pc->counters.skipTo = 0;
  pc->cpuTime = 0;
  pc->counters.eof = 0;

  QueryIterator *ret = &pc->base;
  ret->type = PROFILE_ITERATOR;
  ret->atEOF = child->atEOF;
  ret->lastDocId = child->lastDocId;
  ret->current = child->current;

  ret->Free = PI_Free;
  ret->Read = PI_Read;
  ret->SkipTo = PI_SkipTo;
  ret->NumEstimated = PI_NumEstimated;
  ret->Rewind = PI_Rewind;
  ret->Revalidate = PI_Revalidate;

  return ret;
}
