#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "rtdoc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct QueryIterator;

void QIter_Free(struct QueryIterator *iter) NOEXCEPT;
struct RTDoc *QIter_Next(struct QueryIterator *iter);
NODISCARD size_t QIter_Remaining(struct QueryIterator const *iter);
void QIter_Sort(struct QueryIterator *iter);

#ifdef __cplusplus
}
#endif
