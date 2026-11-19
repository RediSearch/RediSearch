/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Rebuilds search for the slots of an SST (physical) slot-migration import, on disk.
 *
 * An SST import installs the source's keys with the source's DocIdMeta, and search keeps out of
 * their slots while the RAM overlay replays (disk_asm_import_window.h). One coordinator per import
 * attempt (`task_id`) then rebuilds them:
 *
 * - IMPORT_SST_BASE_READY: takes the import gate, which holds back the source's later writes
 *   (the live tail) until search releases it. An attempt whose gate is refused is failed.
 * - IMPORT_RAM_OVERLAY_RESTORE_END: freezes the participating indexes, the disk indexes that
 *   exist now, and starts a single AsyncScan sweep of the slots. For every key it clears the
 *   DocIdMeta, skips the key if it expired, and indexes it into each participating index whose
 *   rule selects it. An index created later is never indexed into, even under a reused name.
 * - The sweep completed: the slots leave the import window and the gate is released, so the live
 *   tail is indexed by its notifications, and its deletes find the sweep's own docIds. A sweep
 *   that ends any other way fails the attempt, which keeps the gate and is never re-swept: a
 *   second sweep would clear DocIdMeta the first one wrote.
 * - A failed rebuild, including a refused gate, aborts the attempt
 *   (RedisModule_ClusterAbortSlotMigration).
 * - IMPORT_COMPLETED / IMPORT_FAILED: tears the coordinator down, stopping its sweep.
 * - After IMPORT_FAILED, the slots stay in the import window until Flex trims them with
 *   TRIM_BACKGROUND, whose handler deletes their documents.
 *
 * Every function runs on the main thread or under the GIL.
 */

// Handlers of the import's stage events. Callbacks naming an unknown `task_id` are ignored.
void DiskASM_ImportRebuild_OnBaseReady(RedisModuleCtx *ctx,
                                       const RedisModuleClusterSlotMigrationInfo *info);
void DiskASM_ImportRebuild_OnOverlayEnd(RedisModuleCtx *ctx,
                                        const RedisModuleClusterSlotMigrationInfo *info);
// Returns whether the slots stay in the import window until their trim.
bool DiskASM_ImportRebuild_OnImportEnded(const RedisModuleClusterSlotMigrationInfo *info,
                                         bool completed);

#ifdef ENABLE_ASSERT
// Whether `key`'s slot is being rebuilt. Flex promises that no write and no delete reaches such a
// slot until the gate is released, which debug builds assert. `key` may be NULL.
bool DiskASM_ImportRebuild_IsRebuildingKey(const RedisModuleString *key);
#endif

// INFO fields of the latest import attempt, active or ended.
void DiskASM_ImportRebuild_AddInfo(RedisModuleInfoCtx *ctx);

#ifdef __cplusplus
}
#endif
