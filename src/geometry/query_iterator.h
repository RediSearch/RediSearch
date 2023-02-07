#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "rtdoc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct GeometryQueryIterator;

void QIter_Free(struct GeometryQueryIterator *iter) NOEXCEPT;
struct RTDoc *QIter_Next(struct GeometryQueryIterator *iter);
NODISCARD size_t QIter_Remaining(struct GeometryQueryIterator const *iter);
void QIter_Sort(struct GeometryQueryIterator *iter);

#ifdef __cplusplus
}
#endif
