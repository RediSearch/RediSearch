#ifndef RESULT_TERMOFFSETS_H
#define RESULT_TERMOFFSETS_H

#include "util/array.h"
#include "redisearch.h"
#include "byte_offsets.h"
#include "fragmenter.h"

typedef struct {
  Array terms;
  Array posOffsets;
  const RSByteOffsets *byteOffsets;
  FragmentOffsets expandedOffsets;
} ResultTermOffsets;

/**
 * Call to initialize the result with the matched term. This will extract offsets
 * as necessary.
 */
void ResultTermOffsets_Init(ResultTermOffsets *res, const RSIndexResult *ixRes);

void ResultTermOffsets_SetByteOffsets(ResultTermOffsets *res, const RSByteOffsets *offsets);

int ResultTermOffsets_Fragmentize(ResultTermOffsets *res, FragmentList *fragList, uint32_t fieldId,
                                  const char *doc);

void ResultTermOffsets_Free(ResultTermOffsets *res);

#endif