/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "filter_summary.h"

#include "doc_table.h"
#include "forward_index.h"
#include "indexer.h"
#include "rmalloc.h"
#include "spec.h"

#include <string.h>

typedef struct {
  uint16_t fieldIndex;
  FilterQuickCheckNumericSummary bounds;
} NumericFieldSummary;

struct FilterSummary {
  FilterQuickCheckSignature terms;
  uint16_t schemaFieldCount;
  uint16_t numericCount;
  NumericFieldSummary numeric[];
};

size_t FilterSummary_MemoryUsage(const FilterSummary* summary) {
  return summary ? sizeof(*summary) + summary->numericCount * sizeof(*summary->numeric) : 0;
}

void FilterSummary_Free(FilterSummary* summary) {
  rm_free(summary);
}

static FilterSummary* buildSummary(const RSAddDocumentCtx* aCtx) {
  const IndexSpec* spec = aCtx->spec;
  size_t numericCount = 0;
  for (size_t i = 0; i < spec->numFields; ++i) {
    const FieldSpec* fs = &spec->fields[i];
    numericCount += FieldSpec_IsIndexable(fs) && FIELD_IS(fs, INDEXFLD_T_NUMERIC);
  }
  FilterSummary* summary =
      rm_calloc(1, sizeof(*summary) + numericCount * sizeof(*summary->numeric));
  if (!summary) return NULL;
  summary->schemaFieldCount = spec->numFields;
  summary->numericCount = numericCount;
  size_t numericSlot = 0;
  for (size_t i = 0; i < spec->numFields; ++i) {
    const FieldSpec* fs = &spec->fields[i];
    if (FieldSpec_IsIndexable(fs) && FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
      summary->numeric[numericSlot].fieldIndex = fs->index;
      FilterQuickCheckNumericSummary_Init(&summary->numeric[numericSlot++].bounds);
    }
  }

  if (aCtx->fwIdx) {
    ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
    for (ForwardIndexEntry* entry = ForwardIndexIterator_Next(&it); entry;
         entry = ForwardIndexIterator_Next(&it)) {
      for (size_t i = 0; i < spec->numFields; ++i) {
        const FieldSpec* fs = &spec->fields[i];
        if (FieldSpec_IsIndexableText(fs) && (entry->fieldMask & FIELD_BIT(fs))) {
          FilterQuickCheckTerm term = {FilterQuickCheck_TEXT, fs->index, entry->term, entry->len};
          FilterQuickCheckSignature_Add(&summary->terms, &term);
        }
      }
    }
  }

  for (size_t i = 0; i < aCtx->doc->numFields; ++i) {
    const FieldSpec* fs = &aCtx->fspecs[i];
    const FieldIndexerData* data = &aCtx->fdatas[i];
    if (!fs->fieldName || !FieldSpec_IsIndexable(fs) || data->isNull) continue;
    FieldType indexedAs = aCtx->doc->fields[i].indexAs;
    if (indexedAs & INDEXFLD_T_TAG) {
      for (size_t j = 0; j < array_len(data->tags); ++j) {
        if (!data->tags[j]) continue;
        FilterQuickCheckTerm term = {FilterQuickCheck_TAG, fs->index, data->tags[j],
                                     strlen(data->tags[j])};
        FilterQuickCheckSignature_Add(&summary->terms, &term);
      }
    }
    if (indexedAs & INDEXFLD_T_NUMERIC) {
      for (size_t j = 0; j < numericCount; ++j) {
        if (summary->numeric[j].fieldIndex != fs->index) continue;
        FilterQuickCheckNumericSummary* bounds = &summary->numeric[j].bounds;
        if (data->isMulti) {
          for (size_t k = 0; k < array_len(data->arrNumeric); ++k) {
            FilterQuickCheckNumericSummary_Add(bounds, data->arrNumeric[k]);
          }
        } else {
          FilterQuickCheckNumericSummary_Add(bounds, data->numeric);
        }
        break;
      }
    }
  }
  return summary;
}

void FilterSummary_IndexDocument(const RSAddDocumentCtx* aCtx) {
  DocTable* table = &aCtx->spec->docs;
  RSDocumentMetadata* md = (RSDocumentMetadata*)DocTable_Borrow(table, aCtx->doc->docId);
  if (!md) return;
  table->memsize -= FilterSummary_MemoryUsage(md->filterSummary);
  FilterSummary_Free(md->filterSummary);
  md->filterSummary = NULL;
  /* Reusing an existing ID can retain older postings absent from this update's input. */
  if (!(aCtx->stateFlags & ACTX_F_ERRORED) && aCtx->doc->docId != aCtx->oldDocId) {
    md->filterSummary = buildSummary(aCtx);
    table->memsize += FilterSummary_MemoryUsage(md->filterSummary);
  }
  DMD_Return(md);
}

static bool lookupTerms(void* context, uint64_t docId, FilterQuickCheckSignature* out) {
  const IndexSpec* spec = context;
  const RSDocumentMetadata* md = DocTable_Borrow(&spec->docs, docId);
  bool available =
      md && md->filterSummary && md->filterSummary->schemaFieldCount == spec->numFields;
  if (available) *out = md->filterSummary->terms;
  DMD_Return(md);
  return available;
}

static bool lookupNumeric(void* context, uint64_t docId, uint16_t fieldIndex,
                          FilterQuickCheckNumericSummary* out) {
  const IndexSpec* spec = context;
  const RSDocumentMetadata* md = DocTable_Borrow(&spec->docs, docId);
  bool found = false;
  if (md && md->filterSummary && md->filterSummary->schemaFieldCount == spec->numFields) {
    const FilterSummary* summary = md->filterSummary;
    for (size_t i = 0; i < summary->numericCount; ++i) {
      if (summary->numeric[i].fieldIndex == fieldIndex) {
        *out = summary->numeric[i].bounds;
        found = true;
        break;
      }
    }
  }
  DMD_Return(md);
  return found;
}

FilterQuickCheckContext FilterSummary_CreateContext(IndexSpec* spec,
                                                    const FilterQuickCheck* check) {
  return (FilterQuickCheckContext){.check = check,
                                   .lookup = spec ? lookupTerms : NULL,
                                   .lookupContext = spec,
                                   .numericLookup = spec ? lookupNumeric : NULL,
                                   .numericLookupContext = spec};
}
