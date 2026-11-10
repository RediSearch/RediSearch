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
#include <stddef.h>
#include <stdint.h>

#include "field_spec.h"
#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Maps the name a document uses for a value to the schema fields that read it, so a document
 * can be loaded by walking its own fields instead of probing every schema field.
 *
 * Several schema fields may share a path (`v AS vv`, `v AS txt`); they form a chain in schema
 * order. The key is the whole `fieldPath`, which is the field name of a Hash.
 *
 * Positions are 1-based so that 0 ends a chain. The table is immutable: rebuild it when the
 * schema changes.
 */
typedef struct FieldPathTable FieldPathTable;

FieldPathTable *FieldPathTable_New(const FieldSpec *fields, size_t n);
void FieldPathTable_Free(FieldPathTable *table);

/** First field (1-based) whose key is exactly `key[0, len)`, or 0. */
t_fieldIndex FieldPathTable_Find(const FieldPathTable *table, const char *key, size_t len);
/** The field after `pos` (1-based) on its chain, or 0. */
t_fieldIndex FieldPathTable_Next(const FieldPathTable *table, t_fieldIndex pos);

#ifdef __cplusplus
}
#endif
