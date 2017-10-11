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

static inline RSValue RS_StringVal(char *str, uint32_t len) {
  return (RSValue){.t = RSValue_String, .strval = {.str = str, .len = len}};
}

static inline RSValue RS_CStringVal(char *str) {
  return (RSValue){.t = RSValue_String,
                   .strval = {
                       .str = str, .len = strlen(str),
                   }};
}

static inline RSValue RS_NumVal(double n) {
  return (RSValue){
      .t = RSValue_Number, .numval = n,
  };
}

static inline RSValue RS_NullVal() {
  return (RSValue){
      .t = RSValue_Null,
  };
}

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
// A result field is a key/value pair of variant type
typedef struct {
  const char *key;
  RSValue val;
} RSField;

static RSField RS_NewField(const char *k, RSValue val) {
  return (RSField){.key = k, .val = val};
}

// A "map" of fields for results and documents - should have getters/setters etc
typedef struct {
  uint16_t len;
  uint16_t cap;
  RSField fields[];
} RSFieldMap;

static inline size_t RSFieldMap_SizeOf(uint16_t cap) {
  return sizeof(RSFieldMap) + cap * sizeof(RSField);
}

static void RSFieldMap_EnsureCap(RSFieldMap **m) {
  if ((*m)->len + 1 >= (*m)->cap) {
    (*m)->cap = MAX((*m)->cap * 2, UINT16_MAX);
    *m = realloc(*m, RSFieldMap_SizeOf((*m)->cap));
  }
}

static RSFieldMap *RS_NewFieldMap(uint16_t cap) {
  if (!cap) cap = 1;
  RSFieldMap *m = malloc(RSFieldMap_SizeOf(cap));
  *m = (RSFieldMap){.len = 0, .cap = cap};
  return m;
}

static RSValue *RSFieldMap_Item(RSFieldMap *m, uint16_t pos) {
  return &m->fields[pos].val;
}

static void RSFieldMap_Set(RSFieldMap *m, const char *key, RSValue val) {
  // TODO: override existing field
  RSFieldMap_EnsureCap(&m);
  m->fields[m->len++] = RS_NewField(key, val);
}

#endif