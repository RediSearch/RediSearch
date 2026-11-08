/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "field_path_table.h"

#include <string.h>

#include "obfuscation/hidden.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "util/dict/dict.h"

typedef struct {
  const char *key;
  size_t keyLen;
  t_fieldIndex head;  // 1-based, 0 marks an empty slot
  t_fieldIndex tail;
} PathEntry;

struct FieldPathTable {
  PathEntry *entries;
  uint32_t cap;           // power of two
  t_fieldIndex *next;     // next[i] is the chain successor of field i (1-based, 0 ends)
  t_fieldIndex generic;   // head of the chain of paths with no root key
  t_fieldIndex genericTail;
};

static bool isDotKeyChar(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

bool FieldPathTable_JsonRootKey(const char *path, const char **key, size_t *keyLen) {
  if (path[0] != '$') return false;
  if (path[1] == '.') {
    const char *start = path + 2;
    const char *end = start;
    while (isDotKeyChar(*end)) end++;
    // `$..x` is recursive descent and `$.*` a wildcard: neither names a root key.
    if (end == start || (*end != '\0' && *end != '.' && *end != '[')) return false;
    if (*end == '.' && end[1] == '.') return false;
    *key = start;
    *keyLen = end - start;
    return true;
  }
  if (path[1] == '[' && (path[2] == '\'' || path[2] == '"')) {
    const char quote = path[2];
    const char *start = path + 3;
    const char *end = start;
    while (*end && *end != quote && *end != '\\' && *end != '\'' && *end != '"') end++;
    if (end == start || *end != quote || end[1] != ']') return false;
    *key = start;
    *keyLen = end - start;
    return true;
  }
  return false;
}

static PathEntry *findSlot(const FieldPathTable *table, const char *key, size_t len) {
  const uint32_t mask = table->cap - 1;
  for (uint32_t pos = (uint32_t)RS_dictGenHashFunction(key, len) & mask;; pos = (pos + 1) & mask) {
    PathEntry *e = &table->entries[pos];
    if (e->head == 0 || (e->keyLen == len && !memcmp(e->key, key, len))) return e;
  }
}

FieldPathTable *FieldPathTable_New(const FieldSpec *fields, size_t n, DocumentType type) {
  FieldPathTable *table = rm_calloc(1, sizeof(*table));
  table->cap = 64;
  // Keep the load factor at or below one half.
  while (table->cap < 2 * n) table->cap <<= 1;
  table->entries = rm_calloc(table->cap, sizeof(*table->entries));
  table->next = rm_calloc(n + 1, sizeof(*table->next));

  for (size_t i = 0; i < n; i++) {
    const char *path = HiddenString_GetUnsafe(fields[i].fieldPath, NULL);
    const t_fieldIndex pos = (t_fieldIndex)(i + 1);
    const char *key = path;
    size_t keyLen = strlen(path);
    if (type == DocumentType_Json && !FieldPathTable_JsonRootKey(path, &key, &keyLen)) {
      if (table->genericTail) table->next[table->genericTail] = pos;
      else table->generic = pos;
      table->genericTail = pos;
      continue;
    }
    PathEntry *e = findSlot(table, key, keyLen);
    if (e->head == 0) {
      *e = (PathEntry){.key = key, .keyLen = keyLen, .head = pos, .tail = pos};
    } else {
      table->next[e->tail] = pos;
      e->tail = pos;
    }
  }
  return table;
}

void FieldPathTable_Free(FieldPathTable *table) {
  if (!table) return;
  rm_free(table->entries);
  rm_free(table->next);
  rm_free(table);
}

t_fieldIndex FieldPathTable_Find(const FieldPathTable *table, const char *key, size_t len) {
  return findSlot(table, key, len)->head;
}

t_fieldIndex FieldPathTable_FirstGeneric(const FieldPathTable *table) {
  return table->generic;
}

t_fieldIndex FieldPathTable_Next(const FieldPathTable *table, t_fieldIndex pos) {
  return table->next[pos];
}
