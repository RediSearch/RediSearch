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
 * order. For a Hash the key is the whole `fieldPath`. For JSON it is the root object key a path
 * descends into (`$.a.b`, `$["a"][0]`); a path that is not of that shape (wildcards, `..`,
 * filters, ...) can match a document whatever its keys are, and is kept on a separate chain.
 *
 * Positions are 1-based so that 0 ends a chain. The table is immutable: rebuild it when the
 * schema changes.
 */
typedef struct FieldPathTable FieldPathTable;

FieldPathTable *FieldPathTable_New(const FieldSpec *fields, size_t n, DocumentType type);
void FieldPathTable_Free(FieldPathTable *table);

/** First field (1-based) whose key is exactly `key[0, len)`, or 0. */
t_fieldIndex FieldPathTable_Find(const FieldPathTable *table, const char *key, size_t len);
/** First JSON field (1-based) whose path can match whatever the root keys are, or 0. */
t_fieldIndex FieldPathTable_FirstGeneric(const FieldPathTable *table);
/** The field after `pos` (1-based) on its chain, or 0. */
t_fieldIndex FieldPathTable_Next(const FieldPathTable *table, t_fieldIndex pos);

/**
 * Extracts the root object key of a JSON path of the shape `$.name...`, `$['name']...` or
 * `$["name"]...`, where `name` is limited to letters, digits and `_` (dot form) or free of quotes
 * and backslashes (bracket form). Returns false for any other path, which the caller must treat
 * as matching every document.
 */
bool FieldPathTable_JsonRootKey(const char *path, const char **key, size_t *keyLen);

#ifdef __cplusplus
}
#endif
