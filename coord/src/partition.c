/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "partition.h"

int GetSlotByPartition(PartitionCtx *ctx, size_t partition){
  size_t step = ctx->tableSize / ctx->size;
  return ((partition + 1) * step - 1) % ctx->tableSize;
}

void PartitionCtx_Init(PartitionCtx *ctx, size_t numPartitions, const char **table,
                       size_t tableSize) {
  ctx->size = numPartitions;
  ctx->table = table;
  ctx->tableSize = tableSize;
}

void PartitionCtx_SetSlotTable(PartitionCtx *ctx, const char **table, size_t tableSize) {
  ctx->table = table;
  ctx->tableSize = tableSize;
}

/* Set the number of partitions in this partition context */
void PartitionCtx_SetSize(PartitionCtx *ctx, size_t size) {
  ctx->size = size;
}
