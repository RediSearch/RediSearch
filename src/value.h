#ifndef RS_VALUE_H_
#define RS_VALUE_H_

#include <string.h>
#include <sys/param.h>
#include <stdarg.h>
#include <errno.h>
#include <math.h>
#include <assert.h>
#include <rmutil/sds.h>
#include "redisearch.h"
#include "util/fnv.h"
#include "rmutil/cmdparse.h"

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////

// Enumeration of possible value types
typedef enum {
  RSValue_Number = 1,
  RSValue_String = 3,
  RSValue_Null = 4,
  RSValue_RedisString = 5,
  // NULL terminated C string that should not be freed with the value
  RSValue_ConstString = 7,
  RSValue_SDS = 9,

  // An array of values, that can be of any type
  RSValue_Array = 6,
  // Reference to another value
  RSValue_Reference = 8,

} RSValueType;

#define RSVALUE_STATIC ((RSValue){.allocated = 0})

// Variant value union
typedef struct rsvalue {
  RSValueType t : 8;
  int refcount : 23;
  uint8_t allocated : 1;
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
      struct rsvalue **vals;
      uint32_t len;
    } arrval;

    // redis string value
    struct RedisModuleString *rstrval;

    // reference to another value
    struct rsvalue *ref;
  };

} RSValue;

static void RSValue_Print(RSValue *v);
/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
static inline void RSValue_Free(RSValue *v) {
  if (!v) return;
  --v->refcount;
  // RSValue_Print(v);
  if (v->refcount <= 0) {

    switch (v->t) {
      case RSValue_String:
        free(v->strval.str);
        break;
      case RSValue_Array:
        for (uint32_t i = 0; i < v->arrval.len; i++) {
          RSValue_Free(v->arrval.vals[i]);
        }
        if (v->allocated) free(v->arrval.vals);
        break;
      case RSValue_Reference:
        RSValue_Free(v->ref);
        break;
      case RSValue_SDS:
        sdsfree(v->strval.str);
        break;
      default:  // no free
        break;
    }

    if (v->allocated) {
      free(v);
    }
  }
}

static inline RSValue *RSValue_IncrRef(RSValue *v) {
  ++v->refcount;
  return v;
}

static inline RSValue *RSValue_DecrRef(RSValue *v) {
  --v->refcount;
  return v;
}

/* Deep copy an object duplicate strings and array, and duplicate sub values recursively on
 * arrays. On numeric values it's no slower than shallow copy. Redis strings ar not recreated
 */
#define RSValue_Copy RSValue_IncrRef

static inline RSValue *RS_NewValue(RSValueType t) {
  RSValue *v = malloc(sizeof(*v));
  v->t = t;
  v->refcount = 0;
  v->allocated = 1;
  return v;
}

static RSValue RS_StaticValue(RSValueType t) {
  RSValue v = (RSValue){
      .t = t,
      .refcount = 1,
      .allocated = 0,
  };
  return v;
}

static inline void RSValue_SetNumber(RSValue *v, double n) {
  v->t = RSValue_Number;
  v->numval = n;
}

static inline void RSValue_SetString(RSValue *v, char *str, size_t len) {
  v->t = RSValue_String;
  v->strval.len = len;
  v->strval.str = str;
}

static inline void RSValue_SetSDS(RSValue *v, sds s) {
  v->t = RSValue_SDS;
  v->strval.len = sdslen(s);
  v->strval.str = s;
}
static inline void RSValue_SetConstString(RSValue *v, const char *str, size_t len) {
  v->t = RSValue_ConstString;
  v->strval.len = len;
  v->strval.str = (char *)str;
}

static inline void RSValue_MakeReference(RSValue *dst, RSValue *src) {
  *dst = (RSValue){
      .t = RSValue_Reference,
      .refcount = 1,
      .allocated = 0,
      .ref = RSValue_IncrRef(src),
  };
}
/* Create a special reference value to a value, but incrementing its ref count and returning a
 * stack allocated object that just holds a pointer to the other value */
static inline RSValue RSValue_StaticReference(RSValue *v) {
  RSValue ret;
  RSValue_MakeReference(&ret, v);
  return ret;
}

/* Return the value itself or its referred value */
static inline RSValue *RSValue_Dereference(RSValue *v) {
  return v->t == RSValue_Reference ? v->ref : v;
}

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
static inline RSValue *RS_StringVal(char *str, uint32_t len) {
  RSValue *v = RS_NewValue(RSValue_String);
  v->strval.str = str;
  v->strval.len = len;
  return v;
}

static inline RSValue *RS_ConstStringVal(char *str, uint32_t len) {
  RSValue *v = RS_NewValue(RSValue_ConstString);
  v->strval.str = str;
  v->strval.len = len;
  return v;
}

/* Wrap a string with length into a value object, assuming the string is a null terminated C
 * string
 */
static inline RSValue *RS_StringValC(char *str) {
  return RS_StringVal(str, strlen(str));
}

static inline RSValue *RS_ConstStringValC(char *str) {
  return RS_ConstStringVal(str, strlen(str));
}

/* Wrap a redis string value */
static inline RSValue *RS_RedisStringVal(RedisModuleString *str) {
  RSValue *v = RS_NewValue(RSValue_RedisString);
  v->rstrval = str;
  return v;
}

// Returns true if the value contains a string
static inline int RSValue_IsString(const RSValue *value) {
  return value && (value->t == RSValue_String || value->t == RSValue_RedisString ||
                   value->t == RSValue_ConstString || value->t == RSValue_SDS);
}

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
static void RSValue_ToString(RSValue *dst, RSValue *v) {
  switch (v->t) {
    case RSValue_String:
    case RSValue_ConstString:
    case RSValue_SDS:
      RSValue_MakeReference(dst, v);
      break;
    case RSValue_RedisString: {
      size_t sz;
      const char *str = RedisModule_StringPtrLen(v->rstrval, &sz);
      RSValue_SetConstString(dst, str, sz);
      break;
    }
    case RSValue_Number: {
      char *str;
      asprintf(&str, "%.12g", v->numval);
      RSValue_SetString(dst, str, strlen(str));
      break;
    }
    case RSValue_Reference:
      return RSValue_ToString(dst, v->ref);

    case RSValue_Null:
    default:
      return RSValue_SetConstString(dst, "", 0);
  }
}

static int RSValue_ParseNumber(const char *p, size_t l, RSValue *v) {
  v = RSValue_Dereference(v);
  char *e;
  errno = 0;
  double d = strtod(p, &e);
  if ((errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL)) || (errno != 0 && d == 0) ||
      *e != '\0') {
    return 0;
  }
  v->t = RSValue_Number;
  v->numval = d;

  return 1;
}

/* Convert a value to a number, either returning the actual numeric values or by parsing a string
into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
not. If possible, we put the actual value into teh double pointer */
static inline int RSValue_ToNumber(RSValue *v, double *d) {
  if (!v) return 0;
  v = RSValue_Dereference(v);

  const char *p = NULL;
  size_t l = 0;
  switch (v->t) {
    // for numerics - just set the value and return
    case RSValue_Number:
      *d = v->numval;
      return 1;

    case RSValue_String:
    case RSValue_ConstString:
    case RSValue_SDS:

      // C strings - take the ptr and len
      p = v->strval.str;
      l = v->strval.len;
      break;
    case RSValue_RedisString:
      // Redis strings - take the number and len
      p = RedisModule_StringPtrLen(v->rstrval, &l);
      break;

    case RSValue_Null:
    case RSValue_Array:
    default:
      return 0;
  }
  // If we have a string - try to parse it
  if (p) {
    char *e;
    errno = 0;
    *d = strtod(p, &e);
    if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
        *e != '\0') {
      return 0;
    }

    return 1;
  }

  return 0;
}

/**
 * Returns the value as a simple opaque buffer
 */
static inline const void *RSValue_ToBuffer(RSValue *value, size_t *outlen) {
  value = RSValue_Dereference(value);

  switch (value->t) {
    case RSValue_Number:
      *outlen = sizeof(value->numval);
      return &value->numval;
    case RSValue_String:
    case RSValue_ConstString:
    case RSValue_SDS:

      *outlen = value->strval.len;
      return value->strval.str;
    case RSValue_RedisString:
      return RedisModule_StringPtrLen(value->rstrval, outlen);
    case RSValue_Array:
    case RSValue_Null:
    default:
      *outlen = 0;
      return "";
  }
}

/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
static inline uint64_t RSValue_Hash(RSValue *v, uint64_t hval) {

  switch (v->t) {
    case RSValue_String:
    case RSValue_ConstString:
    case RSValue_SDS:

      return fnv_64a_buf(v->strval.str, v->strval.len, hval);
    case RSValue_Number:
      return fnv_64a_buf(&v->numval, sizeof(double), hval);

    case RSValue_RedisString: {
      size_t sz;
      const char *c = RedisModule_StringPtrLen(v->rstrval, &sz);
      return fnv_64a_buf((void *)c, sz, hval);
    }
    case RSValue_Null:
      return fnv_64a_buf("__NULL__", 8, hval);
    case RSValue_Reference:
      return RSValue_Hash(v->ref, hval);
    case RSValue_Array: {
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        hval = RSValue_Hash(v->arrval.vals[i], hval);
      }
      return hval;
    }
  }

  return 0;
}

// Gets the string pointer and length from the value
static inline const char *RSValue_StringPtrLen(RSValue *value, size_t *lenp) {
  value = RSValue_Dereference(value);

  if (value->t == RSValue_String || value->t == RSValue_ConstString || value->t == RSValue_SDS) {
    if (lenp) {
      *lenp = value->strval.len;
    }
    return value->strval.str;
  } else if (value->t == RSValue_RedisString) {
    return RedisModule_StringPtrLen(value->rstrval, lenp);
  } else {
    return NULL;
  }
}

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
static inline const char *RSValue_ConvertStringPtrLen(RSValue *value, size_t *lenp, char *buf,
                                                      size_t buflen) {
  value = RSValue_Dereference(value);

  if (RSValue_IsString(value)) {
    return RSValue_StringPtrLen(value, lenp);
  } else if (value->t == RSValue_Number) {
    size_t n = snprintf(buf, buflen, "%f", value->numval);
    if (n >= buflen) {
      *lenp = 0;
      return "";
    }
    *lenp = n;
    return buf;
  } else {
    // Array, Null, other types
    *lenp = 0;
    return "";
  }
}

/* Wrap a number into a value object */
static inline RSValue *RS_NumVal(double n) {
  RSValue *v = RS_NewValue(RSValue_Number);
  v->numval = n;
  return v;
}

/* Wrap an array of RSValue objects into an RSValue array object */
static inline RSValue *RS_ArrVal(RSValue **vals, uint32_t len) {

  RSValue *v = RS_NewValue(RSValue_Array);
  v->arrval.vals = vals;
  v->arrval.len = len;
  return v;
}

static inline RSValue *RS_VStringArray(uint32_t sz, ...) {
  RSValue **arr = calloc(sz, sizeof(*arr));
  va_list ap;
  va_start(ap, sz);
  for (uint32_t i = 0; i < sz; i++) {
    char *p = va_arg(ap, char *);
    arr[i] = RS_StringValC(p);
  }
  va_end(ap);
  return RS_ArrVal(arr, sz);
}

/* Wrap an array of NULL terminated C strings into an RSValue array */
static inline RSValue *RS_StringArray(char **strs, uint32_t sz) {
  RSValue **arr = calloc(sz, sizeof(RSValue *));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValC(strs[i]);
  }
  return RS_ArrVal(arr, sz);
}

static RSValue RS_NULL = {.t = RSValue_Null, .refcount = 1, .allocated = 0};
/* Create a new NULL RSValue */
static inline RSValue *RS_NullVal() {
  return &RS_NULL;
}

static RSValue *RS_NewValueFromCmdArg(CmdArg *arg) {
  switch (arg->type) {
    case CmdArg_Double:
      return RS_NumVal(CMDARG_DOUBLE(arg));
    case CmdArg_Integer:
      return RS_NumVal((double)CMDARG_INT(arg));
    case CmdArg_String:
      return RS_ConstStringVal(CMDARG_STRPTR(arg), CMDARG_STRLEN(arg));
    case CmdArg_Flag:
      return RS_NumVal((double)CMDARG_BOOL(arg));
    case CmdArg_Array: {
      RSValue **vals = calloc(CMDARG_ARRLEN(arg), sizeof(*vals));
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
  v1 = RSValue_Dereference(v1);
  v2 = RSValue_Dereference(v2);

  if (v1->t == v2->t) {
    switch (v1->t) {
      case RSValue_Number:

        return v1->numval > v2->numval ? v1->numval : (v1->numval < v2->numval ? -1 : 0);
      case RSValue_String:
      case RSValue_ConstString:
      case RSValue_SDS:

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
      default:
        return 0;
    }
  }

  static char buf1[100], buf2[100];

  size_t l1, l2;
  const char *s1 = RSValue_ConvertStringPtrLen(v1, &l1, buf1, sizeof(buf1));
  const char *s2 = RSValue_ConvertStringPtrLen(v2, &l2, buf2, sizeof(buf2));
  return cmp_strings(s1, s2, l1, l2);
}

static inline RSValue *RSValue_ArrayItem(RSValue *arr, uint32_t index) {
  return arr->arrval.vals[index];
}

/* Based on the value type, serialize the value into redis client response */
static int RSValue_SendReply(RedisModuleCtx *ctx, RSValue *v) {
  if (!v) {
    return RedisModule_ReplyWithNull(ctx);
  }
  v = RSValue_Dereference(v);

  switch (v->t) {
    case RSValue_String:
    case RSValue_ConstString:
    case RSValue_SDS:

      return RedisModule_ReplyWithStringBuffer(ctx, v->strval.str, v->strval.len);
    case RSValue_RedisString:
      return RedisModule_ReplyWithString(ctx, v->rstrval);
    case RSValue_Number: {
      static char buf[128];
      snprintf(buf, sizeof(buf), "%.12g", v->numval);
      return RedisModule_ReplyWithStringBuffer(ctx, buf, strlen(buf));
    }
    case RSValue_Null:
      return RedisModule_ReplyWithNull(ctx);
    case RSValue_Array:
      RedisModule_ReplyWithArray(ctx, v->arrval.len);
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        RSValue_SendReply(ctx, v->arrval.vals[i]);
      }
      return REDISMODULE_OK;
    default:
      RedisModule_ReplyWithNull(ctx);
  }
  return REDISMODULE_OK;
}

static void RSValue_Print(RSValue *v) {
  if (!v) {
    printf("nil");
  }

  switch (v->t) {
    case RSValue_String:
    case RSValue_ConstString:
    case RSValue_SDS:

      printf("%.*s", v->strval.len, v->strval.str);
      break;
    case RSValue_RedisString:
      printf("%s", RedisModule_StringPtrLen(v->rstrval, NULL));
      break;
    case RSValue_Number:
      printf("%.12g", v->numval);
      break;
    case RSValue_Null:
      printf("NULL");
      break;
    case RSValue_Array:
      printf("[");
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        RSValue_Print(v->arrval.vals[i]);
        printf(", ");
      }
      printf("]");
      break;
    case RSValue_Reference:
      RSValue_Print(v->ref);
      break;
  }
}

// Convert a property key from '@property_name' format as used in queries to 'property_name'
#define RSKEY(s) ((s && *s == '@') ? s + 1 : s)

#define RSKEY_NOTFOUND -1
#define RSKEY_NOCACHE -2
#define RSKEY_UNCACHED -3
#define RSKEY_ISVALIDIDX(i) (i >= 0)
typedef struct {
  const char *key;
  int cachedIdx;
} RSKey;

#define RS_KEY(s)                  \
  ((RSKey){                        \
      .key = s,                    \
      .cachedIdx = RSKEY_UNCACHED, \
  })

typedef struct {
  uint16_t len;
  RSKey keys[];
} RSMultiKey;

static RSMultiKey *RS_NewMultiKey(uint16_t len) {
  RSMultiKey *ret = calloc(1, sizeof(RSMultiKey) + len * sizeof(RSKey));
  ret->len = len;
  return ret;
}

static RSMultiKey *RS_NewMultiKeyVariadic(int len, ...) {
  RSMultiKey *ret = calloc(1, sizeof(RSMultiKey) + len * sizeof(RSKey));
  ret->len = len;
  va_list ap;
  va_start(ap, len);
  for (int i = 0; i < len; i++) {
    const char *arg = va_arg(ap, const char *);
    ret->keys[i] = RS_KEY(RSKEY(arg));
  }
  va_end(ap);
  return ret;
}

/* Create a multi-key from a string array */
static RSMultiKey *RS_NewMultiKeyFromArgs(CmdArray *arr, int allowCaching) {
  RSMultiKey *ret = RS_NewMultiKey(arr->len);
  for (size_t i = 0; i < arr->len; i++) {
    assert(CMDARRAY_ELEMENT(arr, i)->type == CmdArg_String);
    ret->keys[i] = RS_KEY(RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(arr, i))));
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
static inline RSField RS_NewField(const char *k, RSValue *val) {
  static RSField ret;
  ret.key = (RSKEY(k));
  RSValue_MakeReference(&ret.val, val);

  return ret;
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
  RSFieldMap *m = calloc(1, RSFieldMap_SizeOf(cap));
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
  return RSValue_Dereference(&m->fields[pos].val);
}

/* Find an item by name. */
static inline RSValue *RSFieldMap_Get(RSFieldMap *m, const char *k) {
  k = RSKEY(k);
  for (uint16_t i = 0; i < m->len; i++) {
    if (!strcmp(FIELDMAP_FIELD(m, i).key, k)) {
      return RSValue_Dereference(&FIELDMAP_FIELD(m, i).val);
    }
  }
  return NULL;
}

static inline RSValue *RSFieldMap_GetByKey(RSFieldMap *m, RSKey *k) {
  if (RSKEY_ISVALIDIDX(k->cachedIdx)) {
    return RSValue_Dereference(&FIELDMAP_FIELD(m, k->cachedIdx).val);
  }

  for (uint16_t i = 0; i < m->len; i++) {
    if (!strcmp(FIELDMAP_FIELD(m, i).key, (k->key))) {
      if (k->cachedIdx != RSKEY_NOCACHE) {
        k->cachedIdx = i;
      }
      return RSValue_Dereference(&FIELDMAP_FIELD(m, i).val);
    }
  }
  if (k->cachedIdx != RSKEY_NOCACHE) {
    k->cachedIdx = RSKEY_NOTFOUND;
  }
  return RS_NullVal();
}

/* Add a filed to the map WITHOUT checking for duplications */
static void RSFieldMap_Add(RSFieldMap **m, const char *key, RSValue *val) {
  RSFieldMap_EnsureCap(m);
  // Creating the field will create a static reference and increase the ref count on val
  FIELDMAP_FIELD(*m, (*m)->len++) = RS_NewField(key, val);
}

/* Set a value in the map for a given key, checking for duplicates and replacing the existing
 * value if needed, and appending a new one if needed */
static void RSFieldMap_Set(RSFieldMap **m, const char *key, RSValue *val) {
  key = RSKEY(key);
  if (*m) {
    for (uint16_t i = 0; i < (*m)->len; i++) {
      if (!strcmp(FIELDMAP_FIELD(*m, i).key, (key))) {

        // avoid memory leaks...
        RSValue_Free(&FIELDMAP_FIELD(*m, i).val);
        // assign the new field
        RSValue_MakeReference(&FIELDMAP_FIELD(*m, i).val, val);
        return;
      }
    }
  }
  RSFieldMap_EnsureCap(m);

  // not found - append a new field
  FIELDMAP_FIELD(*m, (*m)->len++) = RS_NewField(key, val);
}

static void setStaticValuevp(RSValue *v, RSValueType t, va_list ap) {
  v->t = t;
  v->allocated = 0;
  v->refcount = 1;
  switch (t) {
    case RSValue_String:
    case RSValue_ConstString:
    case RSValue_SDS:
      v->strval.str = va_arg(ap, char *);
      v->strval.len = va_arg(ap, size_t);

      break;
    case RSValue_RedisString:
      v->rstrval = va_arg(ap, RedisModuleString *);
      break;
    case RSValue_Number:
      v->numval = va_arg(ap, double);
      break;
    default:
      break;
  }
}

static void RSFieldMap_SetRawValue(RSFieldMap **m, const char *key, RSValueType t, ...) {
  key = RSKEY(key);
  va_list ap;
  va_start(ap, t);
  if (*m) {
    for (uint16_t i = 0; i < (*m)->len; i++) {
      if (!strcmp(FIELDMAP_FIELD(*m, i).key, (key))) {

        // avoid memory leaks...
        RSValue_Free(&FIELDMAP_FIELD(*m, i).val);
        setStaticValuevp(&FIELDMAP_FIELD(*m, i).val, t, ap);
        goto end;
      }
    }
  }
  RSFieldMap_EnsureCap(m);

  // not found - append a new field
  setStaticValuevp(&FIELDMAP_FIELD(*m, (*m)->len).val, t, ap);
  FIELDMAP_FIELD(*m, (*m)->len).key = key;
  (*m)->len++;
end:
  va_end(ap);
}

static void RSFieldMap_SetStatic(RSFieldMap **m, const char *key, RSValue *in) {
  key = RSKEY(key);
  if (*m) {
    for (uint16_t i = 0; i < (*m)->len; i++) {
      if (!strcmp(FIELDMAP_FIELD(*m, i).key, (key))) {

        // avoid memory leaks...
        RSValue_Free(&FIELDMAP_FIELD(*m, i).val);
        // assign the new field
        FIELDMAP_FIELD(*m, i).val = *in;
        FIELDMAP_FIELD(*m, i).val.allocated = 0;
        FIELDMAP_FIELD(*m, i).val.refcount = 1;
        return;
      }
    }
  }
  RSFieldMap_EnsureCap(m);

  // not found - append a new field
  FIELDMAP_FIELD(*m, (*m)->len) = (RSField){.key = key, .val = *in};
  FIELDMAP_FIELD(*m, (*m)->len).val.allocated = 0;
  FIELDMAP_FIELD(*m, (*m)->len).val.refcount = 1;
  (*m)->len++;
}

static void RSFieldMap_SetNumber(RSFieldMap **m, const char *key, double d) {
  RSFieldMap_SetRawValue(m, key, RSValue_Number, d);
}

static inline void RSFieldMap_Reset(RSFieldMap *m) {
  if (m) {
    for (size_t i = 0; i < m->len; i++) {
      RSValue_Free(&m->fields[i].val);
    }
    m->len = 0;
  }
}
/* Free the field map. If freeKeys is set to 1 we also free the keys */
static void RSFieldMap_Free(RSFieldMap *m, int freeKeys) {
  if (!m) return;
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