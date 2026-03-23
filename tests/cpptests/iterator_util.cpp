/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "iterator_util.h"

IteratorStatus MockIterator_Read(QueryIterator *base) {
    return reinterpret_cast<MockIterator *>(base)->Read();
}
IteratorStatus MockIterator_SkipTo(QueryIterator *base, t_docId docId) {
    return reinterpret_cast<MockIterator *>(base)->SkipTo(docId);
}
size_t MockIterator_NumEstimated(const QueryIterator *base) {
    return reinterpret_cast<const MockIterator *>(base)->NumEstimated();
}
void MockIterator_Rewind(QueryIterator *base) {
    reinterpret_cast<MockIterator *>(base)->Rewind();
}
void MockIterator_Free(QueryIterator *base) {
    delete reinterpret_cast<MockIterator *>(base);
}
ValidateStatus MockIterator_Revalidate(QueryIterator *base) {
    return reinterpret_cast<MockIterator *>(base)->Revalidate();
}
