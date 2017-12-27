#ifndef RS_VALUE_H_
#define RS_VALUE_H_

#include <string.h>
#include <sys/param.h>
#include <stdarg.h>
#include <assert.h>
#include "redisearch.h"
#include "sortable.h"
#include "util/fnv.h"
#include "rmutil/cmdparse.h"

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////

// Enumeration of possible value types
typedef enum {
  RSValue_Number = 0x01,
  RSValue_String = 0x02,
  RSValue_Null = 0x04,
  RSValue_RedisString = 0x08,
  // An array of values, that can be of any type
  RSValue_Array = 0x10,

} RSValueType;

// Variant value union
typedef struct rsvalue {
  union {
    // numeric value
    double numval;

    // string value
    struct {
      char *str;
      uint32_t len;
    } strval;

    // array value
    struct {
      struct rsvalue *vals;
      uint32_t len;
    } arrval;

    // redis string value
    struct RedisModuleString *rstrval;
  };
  RSValueType t : 31;
  int shouldFree : 1;
} RSValue;

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
static void RSValue_Free(RSValue *v) {
  if (v->t == RSValue_String && v->shouldFree) {
    free(v->strval.str);
  } else if (v->t == RSValue_Array && v->shouldFree) {
    for (uint32_t i = 0; i < v->arrval.len; i++) {
      RSValue_Free(&v->arrval.vals[i]);
    }
    free(v->arrval.vals);
  }
}

/* Shallow Copy returns a copy of the original value, while marking the underlying string or array
 * as not needing free, as they are held by another "master" value. This means that you can safely
 * call RSValue_Free on shallow copies without it having any effect */
static inline void RSValue_ShallowCopy(RSValue *dst, RSValue *src) {

  *dst = *src;
  dst->shouldFree = 0;
}

/* Deep copy an object duplicate strings and array, and duplicate sub values recursively on arrays.
 * On numeric values it's no slower than shallow copy. Redis strings ar not recreated
 */
static void RSValue_DeepCopy(RSValue *dst, RSValue *src) {

  *dst = *src;
  if (src->t == RSValue_String) {
    dst->strval.str = strndup(src->strval.str, src->strval.len);
    dst->shouldFree = 1;

  } else if (src->t == RSValue_Array) {
    dst->arrval.vals = calloc(src->arrval.len, sizeof(RSValue));
    for (uint32_t i = 0; i < src->arrval.len; i++) {
      RSValue_DeepCopy(&dst->arrval.vals[i], &src->arrval.vals[i]);
    }
    dst->shouldFree = 1;
  }
}

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
static inline RSValue RS_StringVal(char *str, uint32_t len) {
  return (RSValue){.t = RSValue_String, .shouldFree = 1, .strval = {.str = str, .len = len}};
}

static inline RSValue RS_StringValStatic(char *str, uint32_t len) {
  return (RSValue){.t = RSValue_String, .shouldFree = 0, .strval = {.str = str, .len = len}};
}

/* Wrap a string with length into a value object, assuming the string is a null terminated C string
 */
static inline RSValue RS_CStringVal(char *str) {
  return RS_StringVal(str, strlen(str));
}

static inline RSValue RS_CStringValStatic(char *str) {
  return RS_StringValStatic(str, strlen(str));
}

/* Wrap a redis string value */
static inline RSValue RS_RedisStringVal(RedisModuleString *str) {
  return (RSValue){.t = RSValue_RedisString, .rstrval = str};
}

// Returns true if the value contains a string
static inline int RSValue_IsString(const RSValue *value) {
  return value->t == RSValue_String || value->t == RSValue_RedisString;
}

static RSValue RSValue_ToString(RSValue *v) {
  switch (v->t) {
    case RSValue_String:
      return RS_StringValStatic(v->strval.str, v->strval.len);
    case RSValue_RedisString: {
      size_t sz;
      const char *str = RedisModule_StringPtrLen(v->rstrval, &sz);
      return RS_StringValStatic((char *)str, sz);
    }
    case RSValue_Number: {
      char *str;
      asprintf(&str, "%f", v->numval);
      return RS_CStringVal(str);
    }
    case RSValue_Null:
    default:
      return RS_StringValStatic("", 0);
  }
}

/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
static inline uint64_t RSValue_Hash(RSValue *v, uint64_t hval) {
  switch (v->t) {
    case RSValue_Number:
      return fnv_64a_buf(&v->numval, sizeof(double), hval);
    case RSValue_String:
      return fnv_64a_buf(v->strval.str, v->strval.len, hval);
    case RSValue_RedisString: {
      size_t sz;
      const char *c = RedisModule_StringPtrLen(v->rstrval, &sz);
      return fnv_64a_buf((void *)c, sz, hval);
    }
    case RSValue_Null:
      return fnv_64a_buf("__NULL__", 8, hval);
    case RSValue_Array: {
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        hval = RSValue_Hash(&v->arrval.vals[i], hval);
      }
      return hval;
    }
  }
}

// Gets the string pointer and length from the value
static inline const char *RSValue_StringPtrLen(const RSValue *value, size_t *lenp) {
  if (value->t == RSValue_String) {
    if (lenp) {
      *lenp = value->strval.len;
    }
    return value->strval.str;
  } else {
    return RedisModule_StringPtrLen(value->rstrval, lenp);
  }
}

/* Wrap a number into a value object */
static inline RSValue RS_NumVal(double n) {
  return (RSValue){
      .t = RSValue_Number,
      .numval = n,
  };
}

/* Wrap an array of RSValue objects into an RSValue array object */
static inline RSValue RS_ArrVal(RSValue *vals, uint32_t len) {
  return (RSValue){
      .t = RSValue_Array,
      .arrval = {.vals = vals, .len = len},
      .shouldFree = 1,
  };
}

static inline RSValue RS_VStringArray(uint32_t sz, ...) {
  RSValue *arr = calloc(sz, sizeof(RSValue));
  va_list ap;
  va_start(ap, sz);
  for (uint32_t i = 0; i < sz; i++) {
    char *p = va_arg(ap, char *);
    arr[i] = RS_CStringVal(p);
  }
  va_end(ap);
  return RS_ArrVal(arr, sz);
}

/* Wrap an array of NULL terminated C strings into an RSValue array */
static inline RSValue RS_StringArray(char **strs, uint32_t sz) {
  RSValue *arr = calloc(sz, sizeof(RSValue));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_CStringVal(strs[i]);
  }
  return RS_ArrVal(arr, sz);
}

/* Create a new NULL RSValue */
static inline RSValue RS_NullVal() {
  return (RSValue){
      .t = RSValue_Null,
  };
}

static RSValue RS_NewValueFromCmdArg(CmdArg *arg) {
  switch (arg->type) {
    case CmdArg_Double:
      return RS_NumVal(CMDARG_DOUBLE(arg));
    case CmdArg_Integer:
      return RS_NumVal((double)CMDARG_INT(arg));
    case CmdArg_String:
      return RS_StringValStatic(CMDARG_STRPTR(arg), CMDARG_STRLEN(arg));
    case CmdArg_Flag:
      return RS_NumVal((double)CMDARG_BOOL(arg));
    case CmdArg_Array: {
      RSValue *vals = calloc(CMDARG_ARRLEN(arg), sizeof(RSValue));
      for (size_t i = 0; i < CMDARG_ARRLEN(arg); ++i) {
        vals[i] = RS_NewValueFromCmdArg(CMDARG_ARRELEM(arg, i));
      }
      return RS_ArrVal(vals, CMDARG_ARRLEN(arg));
    }
    default:
      return RS_NullVal();
  }
}
static inline int cmp_strings(const char *s1, const char *s2, size_t l1, size_t l2) {
  int cmp = strncmp(s1, s2, MIN(l1, l2));
  if (l1 == l2) {
    // if the strings are the same length, just return the result of strcmp
    return cmp;
  } else {  // if the lengths arent identical
    // if the strings are identical but the lengths aren't, return the longer string
    if (cmp == 0) {
      return l1 > l2 ? 1 : -1;
    } else {  // the strings are lexically different, just return that
      return cmp;
    }
  }
}
static int RSValue_Cmp(RSValue *v1, RSValue *v2) {
  assert(v1);
  assert(v2);
  if (v1->t == v2->t) {
    switch (v1->t) {
      case RSValue_Number:

        return v1->numval > v2->numval ? v1->numval : (v1->numval < v2->numval ? -1 : 0);
      case RSValue_String:
        return cmp_strings(v1->strval.str, v2->strval.str, v1->strval.len, v2->strval.len);
      case RSValue_RedisString: {
        size_t l1, l2;
        const char *s1 = RedisModule_StringPtrLen(v1->rstrval, &l1);
        const char *s2 = RedisModule_StringPtrLen(v2->rstrval, &l2);
        return cmp_strings(s1, s2, l1, l2);
      }
      case RSValue_Null:
        return 0;
      case RSValue_Array:  // can't compare arrays ATM
        return 0;
    }
  }

  // default strategy: convert both to strings and compare strings
  RSValue s1 = RSValue_ToString(v1);

  RSValue s2 = RSValue_ToString(v2);
  int cmp = RSValue_Cmp(&s1, &s1);
  RSValue_Free(&s1);
  RSValue_Free(&s2);
  return cmp;
}

static inline RSValue *RSValue_ArrayItem(RSValue *arr, uint32_t index) {
  return &arr->arrval.vals[index];
}

/* Based on the value type, serialize the value into redis client response */
static int RSValue_SendReply(RedisModuleCtx *ctx, RSValue *v) {
  if (!v) {
    return RedisModule_ReplyWithNull(ctx);
  }
  switch (v->t) {
    case RSValue_String:
      return RedisModule_ReplyWithStringBuffer(ctx, v->strval.str, v->strval.len);
    case RSValue_RedisString:
      return RedisModule_ReplyWithString(ctx, v->rstrval);
    case RSValue_Number:
      return RedisModule_ReplyWithDouble(ctx, v->numval);
    case RSValue_Null:
      return RedisModule_ReplyWithNull(ctx);
    case RSValue_Array:
      RedisModule_ReplyWithArray(ctx, v->arrval.len);
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        RSValue_SendReply(ctx, &v->arrval.vals[i]);
      }
  }
  return REDISMODULE_OK;
}

static void RSValue_Print(RSValue *v) {
  if (!v) {
    printf("nil");
  }
  switch (v->t) {
    case RSValue_String:
      printf("%.*s", v->strval.len, v->strval.str);
      break;
    case RSValue_RedisString:
      printf("%s", RedisModule_StringPtrLen(v->rstrval, NULL));
      break;
    case RSValue_Number:
      printf("%f", v->numval);
      break;
    case RSValue_Null:
      printf("NULL");
      break;
    case RSValue_Array:
      printf("[");
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        RSValue_Print(&v->arrval.vals[i]);
        printf(", ");
      }
      printf("]");
      break;
  }
}

typedef struct {
  size_t len;
  const char *keys[];
} RSMultiKey;

static RSMultiKey *RS_NewMultiKey(size_t len) {
  RSMultiKey *ret = calloc(1, sizeof(RSMultiKey) + len * sizeof(const char *));
  ret->len = len;
  return ret;
}

/* Create a multi-key from a string array */
static RSMultiKey *RS_NewMultiKeyFromArgs(CmdArray *arr) {
  RSMultiKey *ret = RS_NewMultiKey(arr->len);
  for (size_t i = 0; i < arr->len; i++) {
    assert(CMDARRAY_ELEMENT(arr, i)->type == CmdArg_String);
    ret->keys[i] = CMDARG_STRPTR(CMDARRAY_ELEMENT(arr, i));
  }
  return ret;
}

static void RSMultiKey_Free(RSMultiKey *k) {
  free(k);
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

/* Create a new field map with a given initial capacity */
static RSFieldMap *RS_NewFieldMap(uint16_t cap) {
  if (!cap) cap = 1;
  RSFieldMap *m = malloc(RSFieldMap_SizeOf(cap));
  *m = (RSFieldMap){.len = 0, .cap = cap};
  return m;
}

/* Make sure the fieldmap has enough capacity to add elements */
static void RSFieldMap_EnsureCap(RSFieldMap **m) {
  if (!*m) {
    *m = RS_NewFieldMap(2);
    return;
  }
  if ((*m)->len + 1 >= (*m)->cap) {
    (*m)->cap = MIN((*m)->cap * 2, UINT16_MAX);
    *m = realloc(*m, RSFieldMap_SizeOf((*m)->cap));
  }
}

#define FIELDMAP_FIELD(m, i) (m)->fields[i]

/* Get an item by index */
static inline RSValue *RSFieldMap_Item(RSFieldMap *m, uint16_t pos) {
  return &m->fields[pos].val;
}

/* Find an item by name. */
static inline RSValue *RSFieldMap_Get(RSFieldMap *m, const char *k) {
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
  RSFieldMap_EnsureCap(m);

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
  FIELDMAP_FIELD(*m, (*m)->len++) = RS_NewField(key, val);
}

/* Free the field map. If freeKeys is set to 1 we also free the keys */
static void RSFieldMap_Free(RSFieldMap *m, int freeKeys) {
  for (uint16_t i = 0; i < m->len; i++) {
    RSValue_Free(&m->fields[i].val);

    if (freeKeys) free((void *)m->fields[i].key);
  }
  free(m);
}

static void RSFieldMap_Print(RSFieldMap *m) {
  for (uint16_t i = 0; i < m->len; i++) {
    printf("%s: ", m->fields[i].key);
    RSValue_Print(&m->fields[i].val);
    printf("\n");
  }
}
#endif