/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "tag_index_snapshot.h"

#include "document.h"
#include "doc_table.h"
#include "fulltext_indexed_terms.h"
#include "inverted_index.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "search_ctx.h"
#include "spec.h"
#include "tag_index.h"
#include "util/arr.h"

#include <string.h>

struct RSIndexedTagField {
  t_fieldIndex fieldIx;
  RSFulltextIndexedTerms *terms;
  struct RSIndexedTagField *next;
};

void RSIndexedTagField_FreeList(RSIndexedTagField *head) {
  while (head) {
    RSIndexedTagField *next = head->next;
    RSFulltextIndexedTerms_Free(head->terms);
    rm_free(head);
    head = next;
  }
}

void RSIndexedTagFields_DecrementLive(RedisSearchCtx *sctx, RSIndexedTagField *head) {
  if (head == NULL || sctx == NULL || sctx->spec == NULL) {
    return;
  }
  IndexSpec *spec = sctx->spec;
  if (spec->diskSpec || spec->keysDict == NULL) {
    return;
  }
  for (RSIndexedTagField *cur = head; cur; cur = cur->next) {
    if (cur->fieldIx >= spec->numFields) {
      continue;
    }
    FieldSpec *fs = spec->fields + cur->fieldIx;
    if (fs->types != INDEXFLD_T_TAG) {
      continue;
    }
    TagIndex *tidx = TagIndex_Open(fs);
    if (!tidx || tidx->diskSpec) {
      continue;
    }
    RSFulltextIndexedTerms *terms = cur->terms;
    if (!terms) {
      continue;
    }
    const char *p = RSFulltextIndexedTerms_PackedData(terms);
    const char *end = p + RSFulltextIndexedTerms_PackedSize(terms);
    if ((size_t)(end - p) < sizeof(uint32_t)) {
      continue;
    }
    uint32_t n = 0;
    memcpy(&n, p, sizeof(n));
    p += sizeof(n);
    for (uint32_t i = 0; i < n && p < end; i++) {
      if ((size_t)(end - p) < sizeof(uint32_t)) {
        break;
      }
      uint32_t len = 0;
      memcpy(&len, p, sizeof(len));
      p += sizeof(len);
      if ((size_t)(end - p) < len) {
        break;
      }
      size_t sz = 0;
      InvertedIndex *iv = TagIndex_OpenIndex(tidx, p, len, 0, &sz);
      if (iv) {
        InvertedIndex_DecrementLiveUniqueDocs(iv, 1);
      }
      p += len;
    }
  }
}

static RSIndexedTagField *build_snapshot_for_ctx(RSAddDocumentCtx *aCtx, IndexSpec *spec) {
  if (spec->diskSpec || spec->keysDict == NULL) {
    return NULL;
  }
  RSIndexedTagField *head = NULL;
  RSIndexedTagField **tail = &head;
  for (size_t ii = 0; ii < aCtx->doc->numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    if (fs->types != INDEXFLD_T_TAG || !FieldSpec_IsIndexable(fs)) {
      continue;
    }
    FieldIndexerData *fdata = aCtx->fdatas + ii;
    if (fdata->isNull || !fdata->tags || array_len(fdata->tags) == 0) {
      continue;
    }
    RSFulltextIndexedTerms *blob = RSFulltextIndexedTerms_CreateFromDedupedTagStrings(
        (const char **)fdata->tags, array_len(fdata->tags));
    if (!blob) {
      continue;
    }
    RSIndexedTagField *node = rm_calloc(1, sizeof(*node));
    node->fieldIx = fs->index;
    node->terms = blob;
    *tail = node;
    tail = &node->next;
  }
  return head;
}

void RSIndexedTagFields_SnapshotBulk(RSAddDocumentCtx *aCtx, IndexSpec *spec) {
  for (RSAddDocumentCtx *cur = aCtx; cur && cur->doc->docId; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }
    RSIndexedTagField *snap = build_snapshot_for_ctx(cur, spec);
    RSDocumentMetadata *md = DocTable_BorrowMutable(&spec->docs, cur->doc->docId);
    if (md) {
      DMD_SetIndexedTagFields(md, snap);
      DMD_Return(md);
    } else {
      RSIndexedTagField_FreeList(snap);
    }
  }
}

void DMD_SetIndexedTagFields(RSDocumentMetadata *dmd, RSIndexedTagField *fields) {
  if (dmd == NULL) {
    RSIndexedTagField_FreeList(fields);
    return;
  }
  if (dmd->indexedTagFields) {
    RSIndexedTagField_FreeList(dmd->indexedTagFields);
  }
  dmd->indexedTagFields = fields;
}