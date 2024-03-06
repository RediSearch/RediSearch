/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <pthread.h>

#include "value.h"
#include "util/mempool.h"
#include "module.h"
#include "query_error.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////
static size_t RSValue_NumToString(double dd, char *buf) {
  long long ll = dd;
  if (ll == dd) {
    return sprintf(buf, "%lld", ll);
  } else {
    return sprintf(buf, "%.12g", dd);
  }
}

typedef struct {
  mempool_t *values;
  mempool_t *fieldmaps;
} mempoolThreadPool;

static void mempoolThreadPoolDtor(void *p) {
  mempoolThreadPool *tp = p;
  if (tp->values) {
    mempool_destroy(tp->values);
  }
  if (tp->fieldmaps) {
    mempool_destroy(tp->fieldmaps);
  }
  rm_free(tp);
}

pthread_key_t mempoolKey_g;

static void *_valueAlloc() {
  return rm_malloc(sizeof(RSValue));
}

static void _valueFree(void *p) {
  rm_free(p);
}

static void __attribute__((constructor)) initKey() {
  pthread_key_create(&mempoolKey_g, mempoolThreadPoolDtor);
}

static inline mempoolThreadPool *getPoolInfo() {
  mempoolThreadPool *tp = pthread_getspecific(mempoolKey_g);
  if (tp == NULL) {
    tp = rm_calloc(1, sizeof(*tp));
    mempool_options opts = {
        .initialCap = 0, .maxCap = 1000, .alloc = _valueAlloc, .free = _valueFree};
    tp->values = mempool_new(&opts);
    pthread_setspecific(mempoolKey_g, tp);
  }
  return tp;
}

RSValue *RS_NewValue(RSValueType t) {
  RSValue *v = mempool_get(getPoolInfo()->values);
  v->t = t;
  v->refcount = 1;
  v->allocated = 1;
  return v;
}

void RSValue_Clear(RSValue *v) {
  switch (v->t) {
    case RSValue_String:
      // free strings by allocation strategy
      switch (v->strval.stype) {
        case RSString_Malloc:
          rm_free(v->strval.str);
          break;
        case RSString_RMAlloc:
          rm_free(v->strval.str);
          break;
        case RSString_SDS:
          sdsfree(v->strval.str);
          break;
        case RSString_Const:
        case RSString_Volatile:
          break;
      }
      break;
    case RSValue_Reference:
      RSValue_Decref(v->ref);
      break;
    case RSValue_OwnRstring:
      RedisModule_FreeString(RSDummyContext, v->rstrval);
      break;
    case RSValue_Null:
      return;  // prevent changing global RS_NULL to RSValue_Undef
    case RSValue_Duo:
      RSValue_Decref(RS_DUOVAL_VAL(*v));
      RSValue_Decref(RS_DUOVAL_OTHERVAL(*v));
      RSValue_Decref(RS_DUOVAL_OTHER2VAL(*v));
      rm_free(v->duoval.vals);
      break;
    case RSValue_Array:
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        RSValue_Decref(v->arrval.vals[i]);
      }
      if (!v->arrval.staticarray) {
        rm_free(v->arrval.vals);
      }
      break;
    case RSValue_Map:
      for (uint32_t i = 0; i < v->mapval.len; i++) {
        RSValue_Decref(v->mapval.pairs[RSVALUE_MAP_KEYPOS(i)]);
        RSValue_Decref(v->mapval.pairs[RSVALUE_MAP_VALUEPOS(i)]);
      }
      rm_free(v->mapval.pairs);
      break;
    default:   // no free
      break;
  }

  v->ref = NULL;
  v->t = RSValue_Undef;
}

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
void RSValue_Free(RSValue *v) {
  RSValue_Clear(v);
  if (v->allocated) {
    mempool_release(getPoolInfo()->values, v);
  }
}

RSValue RS_Value(RSValueType t) {
  RSValue v = (RSValue){
      .t = t,
      .refcount = 1,
      .allocated = 0,
  };
  return v;
}

inline void RSValue_SetNumber(RSValue *v, double n) {
  v->t = RSValue_Number;
  v->numval = n;
}

inline void RSValue_SetString(RSValue *v, char *str, size_t len) {
  v->t = RSValue_String;
  v->strval.len = len;
  v->strval.str = str;
  v->strval.stype = RSString_Malloc;
}

RSValue *RS_NewCopiedString(const char *s, size_t n) {
  RSValue *v = RS_NewValue(RSValue_String);
  char *cp = rm_malloc(n + 1);
  cp[n] = 0;
  memcpy(cp, s, n);
  RSValue_SetString(v, cp, n);
  return v;
}

inline void RSValue_SetSDS(RSValue *v, sds s) {
  v->t = RSValue_String;
  v->strval.len = sdslen(s);
  v->strval.str = s;
  v->strval.stype = RSString_SDS;
}
inline void RSValue_SetConstString(RSValue *v, const char *str, size_t len) {
  v->t = RSValue_String;
  v->strval.len = len;
  v->strval.str = (char *)str;
  v->strval.stype = RSString_Const;
}

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
inline RSValue *RS_StringVal(char *str, uint32_t len) {
  RS_LOG_ASSERT(len <= (UINT32_MAX >> 4), "string length exceeds limit");
  RSValue *v = RS_NewValue(RSValue_String);
  v->strval.str = str;
  v->strval.len = len;
  v->strval.stype = RSString_Malloc;
  return v;
}

/* Same as RS_StringVal but with explicit string type */
inline RSValue *RS_StringValT(char *str, uint32_t len, RSStringType t) {
  RSValue *v = RS_NewValue(RSValue_String);
  v->strval.str = str;
  v->strval.len = len;
  v->strval.stype = t;
  return v;
}

RSValue *RS_StringValFmt(const char *fmt, ...) {
  char *buf;
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&buf, fmt, ap);
  va_end(ap);
  return RS_StringVal(buf, strlen(buf));
}

/* Wrap a redis string value */
RSValue *RS_RedisStringVal(RedisModuleString *str) {
  RSValue *v = RS_NewValue(RSValue_RedisString);
  v->rstrval = str;
  return v;
}

RSValue *RS_OwnRedisStringVal(RedisModuleString *str) {
  RSValue *r = RS_RedisStringVal(str);
  RSValue_MakeRStringOwner(r);
  return r;
}

// TODO : NORMALLY
RSValue *RS_StealRedisStringVal(RedisModuleString *str) {
  RSValue *ret = RS_RedisStringVal(str);
  ret->t = RSValue_OwnRstring;
  return ret;
}

void RSValue_MakeRStringOwner(RSValue *v) {
  RS_LOG_ASSERT(v->t == RSValue_RedisString, "RSvalue type should be string");
  v->t = RSValue_OwnRstring;
  RedisModule_RetainString(RSDummyContext, v->rstrval);
}

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
void RSValue_ToString(RSValue *dst, RSValue *v) {
  switch (v->t) {
    case RSValue_String:
      RSValue_MakeReference(dst, v);
      break;
    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t sz;
      const char *str = RedisModule_StringPtrLen(v->rstrval, &sz);
      RSValue_SetConstString(dst, str, sz);
      break;
    }
    case RSValue_Number: {
      char tmpbuf[128] = {0};
      RSValue_NumToString(v->numval, tmpbuf);
      char *buf = rm_strdup(tmpbuf);
      RSValue_SetString(dst, buf, strlen(buf));
      break;
    }
    case RSValue_Reference:
      return RSValue_ToString(dst, v->ref);

    case RSValue_Duo:
      return RSValue_ToString(dst, RS_DUOVAL_VAL(*v));

    case RSValue_Null:
    default:
      return RSValue_SetConstString(dst, "", 0);
  }
}

RSValue *RSValue_ParseNumber(const char *p, size_t l) {

  char *e;
  errno = 0;
  double d = strtod(p, &e);
  if ((errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL)) || (errno != 0 && d == 0) ||
      *e != '\0') {
    return NULL;
  }
  return RS_NumVal(d);
}

/* Convert a value to a number, either returning the actual numeric values or by parsing a string
into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
not. If possible, we put the actual value into teh double pointer */
int RSValue_ToNumber(const RSValue *v, double *d) {
  if (RSValue_IsNull(v)) return 0;
  v = RSValue_Dereference(v);

  const char *p = NULL;
  size_t l = 0;
  switch (v->t) {
    // for numerics - just set the value and return
    case RSValue_Number:
      *d = v->numval;
      return 1;

    case RSValue_String:
      // C strings - take the ptr and len
      p = v->strval.str;
      l = v->strval.len;
      break;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      // Redis strings - take the number and len
      p = RedisModule_StringPtrLen(v->rstrval, &l);
      break;

    case RSValue_Duo:
      return RSValue_ToNumber(RS_DUOVAL_VAL(*v), d);

    case RSValue_Null:
    case RSValue_Array:
    case RSValue_Map:
    case RSValue_Undef:
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

// Gets the string pointer and length from the value
const char *RSValue_StringPtrLen(const RSValue *value, size_t *lenp) {
  value = RSValue_Dereference(value);

  switch (value->t) {
    case RSValue_String:
      if (lenp) {
        *lenp = value->strval.len;
      }
      return value->strval.str;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      return RedisModule_StringPtrLen(value->rstrval, lenp);
    case RSValue_Duo:
      return RSValue_StringPtrLen(RS_DUOVAL_VAL(*value), lenp);
    default:
      return NULL;
  }
}

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
const char *RSValue_ConvertStringPtrLen(const RSValue *value, size_t *lenp, char *buf,
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
RSValue *RS_NumVal(double n) {
  RSValue *v = RS_NewValue(RSValue_Number);
  v->numval = n;
  return v;
}

RSValue *RS_Int64Val(int64_t dd) {
  RSValue *v = RS_NewValue(RSValue_Number);
  v->numval = dd;
  return v;
}

RSValue *RSValue_NewArray(RSValue **vals, uint32_t len) {
  RSValue *arr = RS_NewValue(RSValue_Array);
  arr->arrval.vals = vals;
  arr->arrval.len = len;
  arr->arrval.staticarray = 0;
  return arr;
}

RSValue *RSValue_NewArrayEx(RSValue **vals, size_t n, int options) {
  RSValue *arr = RS_NewValue(RSValue_Array);
  RSValue **list;
  if (options & RSVAL_ARRAY_ALLOC) {
    list = vals;
  } else {
    list = rm_malloc(sizeof(*list) * n);
  }

  arr->arrval.vals = list;

  if (options & RSVAL_ARRAY_STATIC) {
    arr->arrval.staticarray = 1;
  } else {
    arr->arrval.staticarray = 0;
  }

  if (!vals) {
    arr->arrval.len = 0;
  } else {
    arr->arrval.len = n;
    for (size_t ii = 0; ii < n; ++ii) {
      RSValue *v = vals[ii];
      list[ii] = v;
      if (!v) {
        continue;
      }
      if (!(options & RSVAL_ARRAY_NOINCREF)) {
        RSValue_IncrRef(v);
      }
    }
  }

  return arr;
}

RSValue *RSValue_NewMap(RSValue **pairs, uint32_t numPairs) {
  RSValue *map = RS_NewValue(RSValue_Map);
  map->mapval.pairs = pairs;
  map->mapval.len = numPairs;
  return map;
}

RSValue *RS_VStringArray(uint32_t sz, ...) {
  RSValue **arr = rm_calloc(sz, sizeof(*arr));
  va_list ap;
  va_start(ap, sz);
  for (uint32_t i = 0; i < sz; i++) {
    char *p = va_arg(ap, char *);
    arr[i] = RS_StringValC(p);
  }
  va_end(ap);
  return RSValue_NewArrayEx(arr, sz, RSVAL_ARRAY_NOINCREF | RSVAL_ARRAY_ALLOC);
}

/* Wrap an array of NULL terminated C strings into an RSValue array */
RSValue *RS_StringArray(char **strs, uint32_t sz) {
  RSValue **arr = rm_calloc(sz, sizeof(RSValue *));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValC(strs[i]);
  }
  return RSValue_NewArrayEx(arr, sz, RSVAL_ARRAY_NOINCREF | RSVAL_ARRAY_ALLOC);
}

RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st) {
  RSValue **arr = rm_calloc(sz, sizeof(RSValue *));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValT(strs[i], strlen(strs[i]), st);
  }
  return RSValue_NewArrayEx(arr, sz, RSVAL_ARRAY_NOINCREF | RSVAL_ARRAY_ALLOC);
}

RSValue RS_NULL = {.t = RSValue_Null, .refcount = 1, .allocated = 0};
/* Create a new NULL RSValue */
inline RSValue *RS_NullVal() {
  return &RS_NULL;
}

RSValue *RS_DuoVal(RSValue *val, RSValue *otherval, RSValue *other2val) {
  RSValue *duo = RS_NewValue(RSValue_Duo);
  duo->duoval.vals = rm_calloc(3, sizeof(*duo->duoval.vals));
  duo->duoval.vals[0] = val;
  duo->duoval.vals[1] = otherval;
  duo->duoval.vals[2] = other2val;
  return duo;
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

static inline int compare_arrays_first(const RSValue *arr1, const RSValue *arr2, QueryError *qerr) {
  uint32_t len1 = arr1->arrval.len;
  uint32_t len2 = arr2->arrval.len;

  uint32_t len = MIN(len1, len2);
  if (len) {
    // Compare only the first entry
    return RSValue_Cmp(arr1->arrval.vals[0], arr2->arrval.vals[0], qerr);
  }
  return len1 - len2;
}

// TODO: Use when SORTABLE is not looking only at the first array element
static inline int compare_arrays(const RSValue *arr1, const RSValue *arr2, QueryError *qerr) {
  uint32_t len1 = arr1->arrval.len;
  uint32_t len2 = arr2->arrval.len;

  uint32_t len = MIN(len1, len2);
  for (uint32_t i = 0; i < len; i++) {
    int cmp = RSValue_Cmp(arr1->arrval.vals[i], arr2->arrval.vals[i], qerr);
    if (cmp != 0) {
      return cmp;
    }
  }
  return len1 - len2;
}



static inline int cmp_numbers(const RSValue *v1, const RSValue *v2) {
  return v1->numval > v2->numval ? 1 : (v1->numval < v2->numval ? -1 : 0);
}

static inline int convert_to_number(const RSValue *v, RSValue *vn, QueryError *qerr) {
  double d;
  if (!RSValue_ToNumber(v, &d)) {
    if (!qerr) return 0;

    const char *s = RSValue_StringPtrLen(v, NULL);
    QueryError_SetErrorFmt(qerr, QUERY_ENOTNUMERIC, "Error converting string '%s' to number", s);
    return 0;
  }

  RSValue_SetNumber(vn, d);
  return 1;
}

static int RSValue_CmpNC(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  switch (v1->t) {
    case RSValue_Number:
      return cmp_numbers(v1, v2);
    case RSValue_String:
      return cmp_strings(v1->strval.str, v2->strval.str, v1->strval.len, v2->strval.len);
    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t l1, l2;
      const char *s1 = RedisModule_StringPtrLen(v1->rstrval, &l1);
      const char *s2 = RedisModule_StringPtrLen(v2->rstrval, &l2);
      return cmp_strings(s1, s2, l1, l2);
    }
    case RSValue_Duo:
      return RSValue_Cmp(RS_DUOVAL_VAL(*v1), RS_DUOVAL_VAL(*v2), qerr);
    case RSValue_Null:
      return 0;
    case RSValue_Array:
      return compare_arrays_first(v1, v2, qerr);

    case RSValue_Map:   // can't compare maps ATM
    default:
      return 0;
  }
}

int RSValue_Cmp(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  RS_LOG_ASSERT(v1 && v2, "missing RSvalue");
  if (v1->t == v2->t) {
    return RSValue_CmpNC(v1, v2, qerr);
  }

  // if one of the values is null, the other wins
  if (v1 == RS_NullVal()) {
    return -1;
  } else if (v2 == RS_NullVal()) {
    return 1;
  }

  // if either of the arguments is a number, convert the other one to a number
  // if, however, error handling is not available, fallback to string comparison
  do {
    if (v1->t == RSValue_Number) {
      RSValue v2n;
      if (!convert_to_number(v2, &v2n, qerr)) {
        // if it is possible to indicate an error, return
        if (qerr) return 0;
        // otherwise, fallback to string comparison
        break;
      }
      return cmp_numbers(v1, &v2n);
    } else if (v2->t == RSValue_Number) {
      RSValue v1n;
      if (!convert_to_number(v1, &v1n, qerr)) {
        // if it is possible to indicate an error, return
        if (qerr) return 0;
        // otherwise, fallback to string comparison
        break;
      }
      // otherwise, fallback to string comparison
      return cmp_numbers(&v1n, v2);
    }
  } while (0);

  // cast to strings and compare as strings
  char buf1[100], buf2[100];

  size_t l1, l2;
  const char *s1 = RSValue_ConvertStringPtrLen(v1, &l1, buf1, sizeof(buf1));
  const char *s2 = RSValue_ConvertStringPtrLen(v2, &l2, buf2, sizeof(buf2));
  return cmp_strings(s1, s2, l1, l2);
}

int RSValue_Equal(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  RS_LOG_ASSERT(v1 && v2, "missing RSvalue");

  if (v1->t == v2->t) {
    return RSValue_CmpNC(v1, v2, qerr) == 0;
  }

  if (v1 == RS_NullVal() || v2 == RS_NullVal()) {
    return 0;
  }

  // if either of the arguments is a number, convert the other one to a number
  RSValue vn;
  if (v1->t == RSValue_Number) {
    if (!convert_to_number(v2, &vn, NULL)) return 0;
    return cmp_numbers(v1, &vn) == 0;
  } else if (v2->t == RSValue_Number) {
    if (!convert_to_number(v1, &vn, NULL)) return 0;
    return cmp_numbers(&vn, v2) == 0;
  }

  // cast to strings and compare as strings
  char buf1[100], buf2[100];

  size_t l1, l2;
  const char *s1 = RSValue_ConvertStringPtrLen(v1, &l1, buf1, sizeof(buf1));
  const char *s2 = RSValue_ConvertStringPtrLen(v2, &l2, buf2, sizeof(buf2));
  return cmp_strings(s1, s2, l1, l2) == 0;
}

#if 0
static int RSValue_SendReply_Collection(RedisModule_Reply *reply, const RSValue *v, SendReplyFlags flags) {
  typedef struct {
    const RSValue *v; // collection: either array or map
    int k; // position in collection
  }  Item;

  if (v->t != RSValue_Array && v->t != RSValue_Map) {
    return RSValue_SendReply(reply, v, flags);
  }

  arrayof(Item) stack = array_new(Item, 64);

  bool next(const RSValue *w, int k, const RSValue *v) {
    if (v->t != RSValue_Array && v->t != RSValue_Map) {
      RSValue_SendReply(reply, v, flags);
      return false;
    }
    stack = array_append(stack, ((Item){.v = w, .k = k + 1}));
    stack = array_append(stack, ((Item){.v = v, 0}));
    return true;
  }

  stack = array_append(stack, ((Item){.v = v, .k = 0}));
  while (array_len(stack) > 0) {
    Item item = array_pop(stack);
    const RSValue *w = item.v;
    int k = item.k;
    int n;

    if (w->t == RSValue_Array) {
      if (!k) {
        RedisModule_Reply_Array(reply);
      }
      n = w->arrval.len;
      for (; k < n; ++k) {
        RSValue *v = w->arrval.vals[k];
        if (next(w, k, v)) {
          break;
        }
      }
    } else { // map
      if (!k) {
        RedisModule_Reply_Map(reply);
      }
      n = w->mapval.len;
      for (; k < n; ++k) {
        RSValue **pair = &w->mapval.pairs[2 * k];
        RSValue_SendReply(reply, pair[0], flags);
        RSValue *v = pair[1];
        if (next(w, k, v)) {
          break;
        }
      }
    }
    if (k == n) {
      if (w->t == RSValue_Array) {
        RedisModule_Reply_ArrayEnd(reply);
      } else {
        RedisModule_Reply_MapEnd(reply);
      }
    }
  }
  array_free(stack);
  return REDISMODULE_OK;
}
#endif //0

/* Based on the value type, serialize the value into redis client response */
int RSValue_SendReply(RedisModule_Reply *reply, const RSValue *v, SendReplyFlags flags) {
  v = RSValue_Dereference(v);

  switch (v->t) {
    case RSValue_String:
      return RedisModule_Reply_StringBuffer(reply, v->strval.str, v->strval.len);

    case RSValue_RedisString:
    case RSValue_OwnRstring:
      return RedisModule_Reply_String(reply, v->rstrval);

    case RSValue_Number: {
      if (!(flags & SENDREPLY_FLAG_EXPAND)) {
        char buf[128] = {0};
        RSValue_NumToString(v->numval, buf);

        if (flags & SENDREPLY_FLAG_TYPED) {
          return RedisModule_Reply_Error(reply, buf);
        } else {
          return RedisModule_Reply_StringBuffer(reply, buf, strlen(buf));
        }
      } else {
        long long ll = v->numval;
        if (ll == v->numval) {
          return RedisModule_Reply_LongLong(reply, ll);
        } else {
          return RedisModule_Reply_Double(reply, v->numval);
        }
      }
    }

    case RSValue_Null:
      return RedisModule_Reply_Null(reply);

    case RSValue_Duo: {
      return RSValue_SendReply(reply, RS_DUOVAL_OTHERVAL(*v), flags);
    }

#if 1
    case RSValue_Array:
      RedisModule_Reply_Array(reply);
        for (uint32_t i = 0; i < v->arrval.len; i++) {
          RSValue_SendReply(reply, v->arrval.vals[i], flags);
        }
      RedisModule_Reply_ArrayEnd(reply);
      return REDISMODULE_OK;

    case RSValue_Map:
      // If Map value is used, assume Map api exists (RedisModule_HasMap)
      RedisModule_Reply_Map(reply);
      for (uint32_t i = 0; i < v->mapval.len; i++) {
          RSValue_SendReply(reply, v->mapval.pairs[RSVALUE_MAP_KEYPOS(i)], flags);
          RSValue_SendReply(reply, v->mapval.pairs[RSVALUE_MAP_VALUEPOS(i)], flags);
      }
      RedisModule_Reply_MapEnd(reply);
      break;
#else // non-recursive
    case RSValue_Array:
    case RSValue_Map:
      return RSValue_SendReply_Collection(reply, v, flags);
#endif

    default:
      RedisModule_Reply_Null(reply);
  }
  return REDISMODULE_OK;
}

void RSValue_Print(const RSValue *v) {
  FILE *fp = stderr;
  if (!v) {
    fprintf(fp, "nil");
  }
  switch (v->t) {
    case RSValue_String:
      fprintf(fp, "\"%.*s\"", v->strval.len, v->strval.str);
      break;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      fprintf(fp, "\"%s\"", RedisModule_StringPtrLen(v->rstrval, NULL));
      break;
    case RSValue_Number: {
      char tmp[128] = {0};
      RSValue_NumToString(v->numval, tmp);
      fprintf(fp, "%s", tmp);
      break;
    }
    case RSValue_Null:
      fprintf(fp, "NULL");
      break;
    case RSValue_Undef:
      fprintf(fp, "<Undefined>");
    case RSValue_Array:
      fprintf(fp, "[");
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        if (i > 0)
          fprintf(fp, ", ");
        RSValue_Print(v->arrval.vals[i]);
      }
      fprintf(fp, "]");
      break;
    case RSValue_Map:
      fprintf(fp, "{");
      for (uint32_t i = 0; i < v->mapval.len; i++) {
        if (i > 0)
          fprintf(fp, ", ");
        RSValue_Print(v->mapval.pairs[RSVALUE_MAP_KEYPOS(i)]);
        fprintf(fp, ": ");
        RSValue_Print(v->mapval.pairs[RSVALUE_MAP_VALUEPOS(i)]);
      }
      fprintf(fp, "}");
      break;
    case RSValue_Reference:
      RSValue_Print(v->ref);
      break;

    case RSValue_Duo:
      RSValue_Print(RS_DUOVAL_VAL(*v));
      break;
  }
}

#ifdef _DEBUG
void print_rsvalue(RSValue *v) {
  RSValue_Print(v);
  fputs("\n", stderr);
}
#endif // _DEBUG

/*
 *  - s: will be parsed as a string
 *  - l: Will be parsed as a long integer
 *  - d: Will be parsed as a double
 *  - !: will be skipped
 *  - ?: means evrything after is optional
 */

int RSValue_ArrayAssign(RSValue **args, int argc, const char *fmt, ...) {

  va_list ap;
  va_start(ap, fmt);
  const char *p = fmt;
  size_t i = 0;
  int optional = 0;
  while (i < argc && *p) {
    switch (*p) {
      case 's': {
        char **ptr = va_arg(ap, char **);
        if (!RSValue_IsString(args[i])) {
          goto err;
        }
        *ptr = (char *)RSValue_StringPtrLen(args[i], NULL);
        break;
      }
      case 'l': {
        long long *lp = va_arg(ap, long long *);
        double d;
        if (!RSValue_ToNumber(args[i], &d)) {
          goto err;
        }
        *lp = (long long)d;
        break;
      }
      case 'd': {
        double *dp = va_arg(ap, double *);
        if (!RSValue_ToNumber(args[i], dp)) {
          goto err;
        }
        break;
      }
      case '!':
        // do nothing...
        break;
      case '?':
        optional = 1;
        // reduce i because it will be incremented soon
        i -= 1;
        break;
      default:
        goto err;
    }
    ++i;
    ++p;
  }
  // if we have stuff left to read in the format but we haven't gotten to the optional part -fail
  if (*p && !optional && i < argc) {
    goto err;
  }
  // if we don't have anything left to read from the format but we haven't gotten to the array's
  // end, fail
  if (*p == 0 && i < argc) {
    goto err;
  }

  va_end(ap);
  return 1;
err:
  va_end(ap);
  return 0;
}

const char *RSValue_TypeName(RSValueType t) {
  switch (t) {
    case RSValue_Array:
      return "array";
    case RSValue_Map:
      return "map";
    case RSValue_Number:
      return "number";
    case RSValue_String:
      return "string";
    case RSValue_Null:
      return "(null)";
    case RSValue_OwnRstring:
    case RSValue_RedisString:
      return "redis-string";
    case RSValue_Reference:
      return "reference";
    case RSValue_Duo:
      return "duo";
    default:
      return "!!UNKNOWN TYPE!!";
  }
}
