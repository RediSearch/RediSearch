#ifndef RS_VALUE_H_
#define RS_VALUE_H_

#include <string.h>
#include <sys/param.h>

#include "redisearch.h"
#include "sortable.h"

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////

// Enumeration of possible value types
typedef enum {
  RSValue_Number,
  RSValue_String,
  RSValue_Null,
} RSValueType;

// Variant value union
typedef struct {
  union {
    double numval;
    struct {
      char *str;
      uint32_t len;
    } strval;
  };
  RSValueType t;
} RSValue;

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if the
 * value needs to be detached */
static inline RSValue RS_StringVal(char *str, uint32_t len) {
  return (RSValue){.t = RSValue_String, .strval = {.str = str, .len = len}};
}

/* Wrap a string with length into a value object, assuming the string is a null terminated C string
 */
static inline RSValue RS_CStringVal(char *str) {
  return RS_StringVal(str, strlen(str));
}

/* Wrap a number into a value object */
static inline RSValue RS_NumVal(double n) {
  return (RSValue){
      .t = RSValue_Number, .numval = n,
  };
}

/* Create a new NULL RSValue */
static inline RSValue RS_NullVal() {
  return (RSValue){
      .t = RSValue_Null,
  };
}

/* Based on the value type, serialize the value into redis client response */
static int RSValue_SendReply(RedisModuleCtx *ctx, RSValue *v) {
  if (!v) {
    return RedisModule_ReplyWithNull(ctx);
  }
  switch (v->t) {
    case RSValue_String:
      return RedisModule_ReplyWithStringBuffer(ctx, v->strval.str, v->strval.len);
    case RSValue_Number:
      return RedisModule_ReplyWithDouble(ctx, v->numval);
    case RSValue_Null:
      return RedisModule_ReplyWithNull(ctx);
  }
  return REDISMODULE_OK;
}

/* A result field is a key/value pair of variant type, used inside a value map */
typedef struct {
  const char *key;
  RSValue val;
} RSField;

/* Create new KV field */
static RSField RS_NewField(const char *k, RSValue val) {
  return (RSField){.key = k, .val = val};
}

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
static void RSValue_Free(RSValue *v) {
  if (v->t == RSValue_String) {
    free(v->strval.str);
  }
}

/* A "map" of fields for results and documents. */
typedef struct {
  uint16_t len;
  uint16_t cap;
  RSField fields[];
} RSFieldMap;

/* The byte size of the field map */
static inline size_t RSFieldMap_SizeOf(uint16_t cap) {
  return sizeof(RSFieldMap) + cap * sizeof(RSField);
}

/* Make sure the fieldmap has enough capacity to add elements */
static void RSFieldMap_EnsureCap(RSFieldMap **m) {
  if ((*m)->len + 1 >= (*m)->cap) {
    (*m)->cap = MAX((*m)->cap * 2, UINT16_MAX);
    *m = realloc(*m, RSFieldMap_SizeOf((*m)->cap));
  }
}

/* Create a new field map with a given initial capacity */
static RSFieldMap *RS_NewFieldMap(uint16_t cap) {
  if (!cap) cap = 1;
  RSFieldMap *m = malloc(RSFieldMap_SizeOf(cap));
  *m = (RSFieldMap){.len = 0, .cap = cap};
  return m;
}

#define FIELDMAP_FIELD(m, i) (m)->fields[i]

/* Get an item by index */
static inline RSValue *RSFieldMap_Item(RSFieldMap *m, uint16_t pos) {
  return &m->fields[pos].val;
}

/* Find an item by name. */
static inline RSValue *RSFielfMap_Get(RSFieldMap *m, const char *k) {
  for (uint16_t i = 0; i < m->len; i++) {
    if (!strcmp(FIELDMAP_FIELD(m, i).key, k)) {
      return &FIELDMAP_FIELD(m, i).val;
    }
  }
  return NULL;
}

/* Add a filed to the map WITHOUT checking for duplications */
static void RSFieldMap_Add(RSFieldMap **m, const char *key, RSValue val) {
  RSFieldMap_EnsureCap(m);
  FIELDMAP_FIELD(*m, (*m)->len++) = RS_NewField(key, val);
}

/* Set a value in the map for a given key, checking for duplicates and replacing the existing value
 * if needed, and appending a new one if needed */
static void RSFieldMap_Set(RSFieldMap **m, const char *key, RSValue val) {
  for (uint16_t i = 0; i < (*m)->len; i++) {
    if (!strcmp(FIELDMAP_FIELD(*m, i).key, key)) {

      // avoid memory leaks...
      RSValue_Free(&FIELDMAP_FIELD(*m, i).val);
      // assign the new field
      FIELDMAP_FIELD(*m, i).val = val;
      return;
    }
  }
  // not found - append a new field
  RSFieldMap_EnsureCap(m);
  FIELDMAP_FIELD(*m, (*m)->len++) = RS_NewField(key, val);
}

#endif