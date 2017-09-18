#include "result_termoffsets.h"
#include "fragmenter.h"

static void extractResultFromTerm(ResultTermOffsets *res, const RSIndexResult *ixRes) {
  switch (ixRes->type) {
    // Aggregate types
    case RSResultType_Intersection:
    case RSResultType_Union:
      for (size_t ii = 0; ii < ixRes->agg.numChildren; ++ii) {
        extractResultFromTerm(res, ixRes->agg.children[ii]);
      }
      break;

    // String
    case RSResultType_Term: {
      for (size_t ii = 0; ii < ARRAY_GETSIZE_AS(&res->terms, FragmentTerm); ++ii) {
        const FragmentTerm *fTerm = ARRAY_GETARRAY_AS(&res->terms, const FragmentTerm *) + ii;
        if (fTerm->len == ixRes->term.term->len &&
            !strncmp(fTerm->tok, ixRes->term.term->str, ixRes->term.term->len)) {
          return;
        }
      }

      // FragmentTerm *newTerm = Buffer_AddToSize(termList, sizeof(*newTerm));
      FragmentTerm *newTerm = ARRAY_ADD_AS(&res->terms, FragmentTerm);
      const RSQueryTerm *qTerm = ixRes->term.term;
      newTerm->len = qTerm->len;
      newTerm->score = qTerm->idf;
      newTerm->tok = strndup(qTerm->str, qTerm->len);

      RSOffsetVector *offInfo = ARRAY_ADD_AS(&res->posOffsets, RSOffsetVector);

      if (ixRes->term.offsets.data) {
        offInfo->data = malloc(ixRes->term.offsets.len);
        offInfo->len = ixRes->term.offsets.len;
        memcpy(offInfo->data, ixRes->term.offsets.data, ixRes->term.offsets.len);
      } else {
        offInfo->len = 0;
      }

      break;
    }
    default:
      break;
  }
}

void ResultTermOffsets_Init(ResultTermOffsets *res, const RSIndexResult *ixRes) {
  ResultTermOffsets_Free(res);
  Array_Init(&res->posOffsets);
  Array_Init(&res->terms);

  extractResultFromTerm(res, ixRes);
}

void ResultTermOffsets_SetByteOffsets(ResultTermOffsets *res, const RSByteOffsets *offsets) {
  res->byteOffsets = offsets;
  FragmentOffsets_Init(&res->expandedOffsets);

  for (size_t ii = 0; ii < ARRAY_GETSIZE_AS(&res->posOffsets, RSOffsetVector); ++ii) {
    RSOffsetVector *compressed = ARRAY_GETITEM_AS(&res->posOffsets, ii, RSOffsetVector *);
    RSOffsetIterator iter = RSOffsetVector_Iterate(compressed);
    FragmentOffsets_AddOffsets(&res->expandedOffsets, ii, &iter);
  }
}

void ResultTermOffsets_Free(ResultTermOffsets *res) {
  // TODO: Might use block allocator if these small allocations impact performance
  FragmentOffsets_Free(&res->expandedOffsets);
  res->byteOffsets = NULL;

  for (size_t ii = 0; ii < ARRAY_GETSIZE_AS(&res->terms, FragmentTerm); ++ii) {
    FragmentTerm *termInfo = ARRAY_GETITEM_AS(&res->terms, ii, FragmentTerm *);
    free((char *)termInfo->tok);
  }

  Array_Free(&res->terms);
  for (size_t ii = 0; ii < ARRAY_GETSIZE_AS(&res->posOffsets, RSOffsetVector); ++ii) {
    RSOffsetVector *offset = ARRAY_GETITEM_AS(&res->posOffsets, ii, RSOffsetVector *);
    if (offset->data) {
      free(offset->data);
    }
  }
  Array_Free(&res->posOffsets);
}

int ResultTermOffsets_Fragmentize(ResultTermOffsets *res, FragmentList *fragList, uint32_t fieldId,
                                  const char *doc) {
  const RSByteOffsetField *offField = NULL;
  const RSByteOffsets *byteOffsets = res->byteOffsets;

  if (!byteOffsets) {
    return 0;
  }

  for (size_t ii = 0; ii < byteOffsets->numFields; ++ii) {
    if (byteOffsets->fields[ii].fieldId == fieldId) {
      offField = byteOffsets->fields + ii;
      break;
    }
  }
  if (!offField) {
    return 0;
  }
  RSOffsetIterator iter = RSOffsetVector_Iterate(&byteOffsets->offsets);

  // printf("Have %lu fields in offset\n", result->byteOffsets->numFields);
  // printf("Have First Token=%lu, Last Token=%lu\n", offField->firstTokPos, offField->lastTokPos);

  // Seek the iterator to its correct position.
  size_t curPos = 1;
  while (curPos < offField->firstTokPos && iter.Next(iter.ctx) != RS_OFFSETVECTOR_EOF) {
    // printf("Advancing POS....\n");
    curPos++;
  }

  FragmentList_FragmentizeFromOffsets(fragList, doc, &res->expandedOffsets, &iter,
                                      offField->firstTokPos, offField->lastTokPos + 1);
  return 1;
}

// static void maybeAddSumTerm(const Query *q, heapResult *h, const RSIndexResult *res) {
//   // If no terms are required, leave!
//   if (!q->keepTerms) {
//     return;
//   }

//   if (!Buffer_Offset(&h->termList)) {
//     Buffer_Init(&h->termList, sizeof(FragmentTerm));
//   }
//   extractResultFromTerm(&h->termList, res);
//   return;
// }
