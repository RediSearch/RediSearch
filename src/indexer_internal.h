/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

/**
 * Indexing-pipeline internals shared between the mode-specific implementations,
 * [indexer.c](indexer.c) (memory) and [disk_indexer.c](disk_indexer.c) (disk).
 *
 * Not part of the indexer's public surface — that is [indexer.h](indexer.h).
 * Nothing outside those two translation units should include this.
 */

#include "spec.h"
#include "forward_index.h"
#include "redisearch.h"
#include "phonetic_manager.h"
#include "stemmer.h"
#include "synonym_map.h"
#include "util/arr/arr.h"

#include <stdbool.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Drop the replaced document's VecSim and Geometry entries.
 *
 * These two index types live in memory in both memory mode and disk mode (the
 * inverted-index / tag / doc-table cleanup is handled by `SearchDisk_PutDocument`
 * in disk mode and by `DocTable_DeleteById` in memory mode — neither covers VecSim or
 * Geometry, hence this dedicated step). Memory mode calls this inline from
 * `makeDocumentId` before the new DMD is allocated; disk mode calls it from
 * `applyDocTable` after the disk batch commits.
 *
 * `VecSimIndex_DeleteVector` and `GeometryIndex_RemoveId` no-op on unknown
 * doc-ids, so this is safe even if the replaced doc had no vector / geometry
 * data, and safe to call defensively on stale key-meta in disk mode.
 */
void Indexer_RemoveReplacedDocVectorAndGeometry(IndexSpec *spec, t_docId oldDocId);

/**
 * Remove the old document's contributions from the spec's scoring stats on
 * REPLACE. Paired with `Indexer_AddNewDocStats`. Memory mode passes `dmd->docLen`
 * from the popped DMD; disk mode passes `aCtx->disk.oldDocLen` captured by
 * `SearchDisk_PutDocument`.
 */
void Indexer_RemoveOldDocStats(IndexSpec *spec, uint32_t oldDocLen);

/**
 * Add the new document's contributions to the spec's scoring stats. Paired
 * with `Indexer_RemoveOldDocStats`. Both flows pass `fwIdx->totalFreq` as the new
 * doc's length.
 */
void Indexer_AddNewDocStats(IndexSpec *spec, uint32_t newDocLen);

// Returns true on terms that should be indexed in the suffix trie.
static inline bool entryWantsSuffixTrie(const IndexSpec *spec, const ForwardIndexEntry *entry) {
  return (spec->suffixMask & entry->fieldMask)
      && entry->term[0] != STEM_PREFIX
      && entry->term[0] != PHONETIC_PREFIX
      && entry->term[0] != SYNONYM_PREFIX_CHAR
      && strlen(entry->term) != 0;
}

/**
 * Attribute `duration` to every TEXT field set in `fieldMask` for the given
 * indexing phase. `fieldMask` is in `ftId` bit space (see `FIELD_BIT`), which
 * spans the full width of `t_fieldMask` — 128 bits on 64-bit builds — so this
 * walks the mask 64 bits at a time and uses `__builtin_ctzll` within each word
 * to jump straight to each set bit; a plain `ctz`-family call on the whole
 * mask would only examine the low 64 bits and misattribute higher `ftId`s.
 *
 * Memory mode calls this once per forward-index entry from `indexText`
 * (`FIELD_INDEXING_INDEX`); disk mode calls it from the matching points in
 * `stageText` (`FIELD_INDEXING_INDEX`) and `applyTextIndex` (`FIELD_INDEXING_APPLY`).
 */
static inline void Indexer_RecordTextFieldTiming(IndexSpec *spec, t_fieldMask fieldMask,
                                                  FieldIndexingPhase phase,
                                                  rs_wall_clock_ns_t duration) {
  const int numWords = (int)(sizeof(t_fieldMask) * 8 / 64);
  for (int word = 0; word < numWords; ++word) {
    uint64_t bits = (uint64_t)(fieldMask >> (word * 64));
    while (bits) {
      t_fieldId fieldId = (t_fieldId)(word * 64 + __builtin_ctzll(bits));
      bits &= bits - 1; // clear the lowest set bit
      if (fieldId >= array_len(spec->fieldIdToIndex)) {
        continue;
      }
      t_fieldIndex fieldIndex = spec->fieldIdToIndex[fieldId];
      if (fieldIndex < spec->numFields) {
        FieldSpec_AddIndexingTime(&spec->fields[fieldIndex], phase, duration);
      }
    }
  }
}

#ifdef __cplusplus
}
#endif
