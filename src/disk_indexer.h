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
 * Disk-mode (Flex) half of the per-document indexing pipeline. The shared
 * entry point and the memory-mode half live in [indexer.c](indexer.c), which
 * dispatches here whenever `SearchDisk_IsEnabled()`.
 *
 * Both functions below assume disk mode is enabled and `spec->diskSpec` is set.
 */

#include "document.h"
#include "search_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Disk-mode doc-id assignment for a single document, called from `doAssignIds`.
 *
 * Opens a fresh per-document write batch, stages the doc-table write onto it,
 * and assigns the new doc-id synchronously. The matching in-memory updates
 * (`DocIdMeta_Set`, scoring stats, GC notification) are deferred to the apply
 * step in `DiskIndexer_IndexDocument`, which runs once the batch has committed.
 */
void DiskIndexer_StageDocument(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx);

/**
 * Disk-mode per-document pipeline. Three steps with a commit fence between
 * the durable writes and the in-memory bookkeeping that pairs with them:
 *
 *   - Stage: write the doc-table / inverted-index / tag-index entries onto
 *     `aCtx->disk.batch` (`stageText`, `bulkStageFields`).
 *   - Commit fence: `commitDocument` aborts on error or commits the batch;
 *     returns false iff the batch did not become durable.
 *   - Apply: only runs on a successful commit. Updates the RAM-side state
 *     that paired with the now-durable disk writes (`applyDocTable`,
 *     `applyTextIndex`, `bulkApplyFields`, `applyVectorInserts`).
 *
 * On commit failure, the apply step is skipped — no in-memory state was
 * mutated, so there is nothing to roll back.
 *
 * Wildcard (`index_all`) and `INDEXMISSING` indexes are not supported on disk
 * specs, so the matching memory-mode hooks (`writeExistingDocs`,
 * `writeMissingFieldDocs`) are not called here.
 */
void DiskIndexer_IndexDocument(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx);

#ifdef __cplusplus
}
#endif
