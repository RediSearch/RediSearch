/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "empty_iterator.h"

static size_t EOI_NumEstimated(QueryIterator *base) {
  return 0;
}

static IteratorStatus EOI_Read(QueryIterator *base) {
  return ITERATOR_EOF;
}

static IteratorStatus EOI_SkipTo(QueryIterator *base, t_docId docId) {
  return ITERATOR_EOF;
}

static void EOI_Rewind(QueryIterator *base) {}

static void EOI_Free(QueryIterator *base) {}

static QueryIterator eofIterator = {.Read = EOI_Read,
                                    .Free = EOI_Free,
                                    .SkipTo = EOI_SkipTo,
                                    .NumEstimated = EOI_NumEstimated,
                                    .Rewind = EOI_Rewind,
                                    .type = EMPTY_ITERATOR,
                                    .atEOF = true,
                                    .lastDocId = 0,
                                    .current = NULL,
                                    .Revalidate = Default_Revalidate,
    };

QueryIterator *NewEmptyIterator(void) {
  return &eofIterator;
}
