/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <pthread.h>

#include "value.h"
#include "util/mempool.h"
#include "module.h"
#include "query_error.h"
#include "rmutil/rm_assert.h"
#include "fast_float/fast_float_strtod.h"
#include "obfuscation/obfuscation_api.h"

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////

pthread_key_t mempoolKey_g;

static void *_valueAlloc() {
  return rm_malloc(sizeof(RSValue));
}

static void __attribute__((constructor)) initKey() {
  pthread_key_create(&mempoolKey_g, (void (*)(void *))mempool_destroy);
}

static inline mempool_t *getPool() {
  mempool_t *tp = pthread_getspecific(mempoolKey_g);
  if (tp == NULL) {
    const mempool_options opts = {
        .initialCap = 0, .maxCap = 1000, .alloc = _valueAlloc, .free = rm_free};
    tp = mempool_new(&opts);
    pthread_setspecific(mempoolKey_g, tp);
  }
  return tp;
}

RSValue RSValue_Undefined_Static() {
  RSValue v;
  v._t = RSValue_Undef;
  v._allocated = 0;
  v._refcount = 1;
  return v;
}

RSValue *RS_NewValue(RSValueType t) {
  RSValue *v = mempool_get(getPool());
  v->_t = t;
  v->_refcount = 1;
  v->_allocated = 1;
  return v;
}

RSValue RSValue_NewStatic_Number(double n) {
  RSValue v = {0};

  v._t = RSValue_Number;
  v._refcount = 1;
  v._allocated = 0;
  v._numval = n;

  return v;
}

RSValue RSValue_NewStatic_String_Malloc(char *str, uint32_t len) {
  RSValue v = {0};
  v._allocated = 0;
  v._refcount = 1;
  v._t = RSValue_String;
  v._strval.str = str;
  v._strval.len = len;
  v._strval.stype = RSString_Malloc;
  return v;
}

bool RSValue_IsTrio(const RSValue *v) {
  return (v && v->_t == RSValue_Trio);
}

RSValue *RSValue_Trio_GetLeft(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValue_Trio);
  return v->_trioval.vals[0];
}

RSValue *RSValue_Trio_GetMiddle(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValue_Trio);
  return v->_trioval.vals[1];
}

RSValue *RSValue_Trio_GetRight(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValue_Trio);
  return v->_trioval.vals[2];
}

RSValueType RSValue_Type(const RSValue *v) {
  RS_ASSERT(v);
  return v->_t;
}

bool RSValue_IsReference(const RSValue *v) {
  return (v && v->_t == RSValue_Reference);
}

bool RSValue_IsNumber(const RSValue *v) {
  return (v && v->_t == RSValue_Number);
}

bool RSValue_IsString(const RSValue *v) {
  return (v && v->_t == RSValue_String);
}

bool RSValue_IsArray(const RSValue *v) {
  return (v && v->_t == RSValue_Array);
}

bool RSValue_IsRedisString(const RSValue *v) {
  return (v && v->_t == RSValue_RedisString);
}

bool RSValue_IsOwnRString(const RSValue *v) {
  return (v && v->_t == RSValue_OwnRstring);
}

double RSValue_Number_Get(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValue_Number);
  return v->_numval;
}

char *RSValue_String_Get(const RSValue *v, uint32_t *lenp) {
  RS_ASSERT(v && v->_t == RSValue_String);
  if(lenp) {
    *lenp = v->_strval.len;
  }
  return v->_strval.str;
}

char *RSValue_String_GetPtr(const RSValue *v) {
  return RSValue_String_Get(v, NULL);
}

RedisModuleString *RSValue_RedisString_Get(const RSValue *v) {
  RS_ASSERT(v && (v->_t == RSValue_RedisString || v->_t == RSValue_OwnRstring));
  return v->_rstrval;
}

uint32_t RSValue_MapLen(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValue_Map);
  return v->_mapval.len;
}

void RSValue_MapEntry(const RSValue *map, uint32_t i, RSValue **key, RSValue **val) {
  RS_ASSERT(i < RSValue_MapLen(map));
  *key = map->_mapval.pairs[RSVALUE_MAP_KEYPOS(i)];
  *val = map->_mapval.pairs[RSVALUE_MAP_VALUEPOS(i)];
}

void RSValue_IntoUndefined(RSValue *v) {
  RS_ASSERT(v);
  v->_t = RSValue_Undef;
}

void RSValue_IntoNumber(RSValue *v, double n) {
  RS_ASSERT(v);
  v->_t = RSValue_Number;
  v->_numval = n;
}

void RSValue_IntoNull(RSValue *v) {
  RS_ASSERT(v);
  v->_t = RSValue_Null;
}

uint16_t RSValue_Refcount(const RSValue *v) {
  RS_ASSERT(v);
  return v->_refcount;
}

void RSValue_Clear(RSValue *v) {
  switch (v->_t) {
    case RSValue_String:
      // free strings by allocation strategy
      switch (v->_strval.stype) {
        case RSString_Malloc:
        case RSString_RMAlloc:
          rm_free(v->_strval.str);
          break;
        case RSString_SDS:
          sdsfree(v->_strval.str);
          break;
        case RSString_Const:
          break;
      }
      break;
    case RSValue_Reference:
      RSValue_Decref(v->_ref);
      break;
    case RSValue_OwnRstring:
      RedisModule_FreeString(RSDummyContext, v->_rstrval);
      break;
    case RSValue_Null:
      return;  // prevent changing global RS_NULL to RSValue_Undef
    case RSValue_Trio:
      RSValue_Decref(RSValue_Trio_GetLeft(v));
      RSValue_Decref(RSValue_Trio_GetMiddle(v));
      RSValue_Decref(RSValue_Trio_GetRight(v));
      rm_free(v->_trioval.vals);
      break;
    case RSValue_Array:
      for (uint32_t i = 0; i < v->_arrval.len; i++) {
        RSValue_Decref(v->_arrval.vals[i]);
      }
      rm_free(v->_arrval.vals);
      break;
    case RSValue_Map:
      for (uint32_t i = 0; i < v->_mapval.len; i++) {
        RSValue_Decref(v->_mapval.pairs[RSVALUE_MAP_KEYPOS(i)]);
        RSValue_Decref(v->_mapval.pairs[RSVALUE_MAP_VALUEPOS(i)]);
      }
      rm_free(v->_mapval.pairs);
      break;
    default:   // no free
      break;
  }

  v->_ref = NULL;
  v->_t = RSValue_Undef;
}


RSValue* RSValue_IncrRef(RSValue* v) {
  __atomic_fetch_add(&v->_refcount, 1, __ATOMIC_RELAXED);
  return v;
}

void RSValue_Decref(RSValue* v) {
  if (__atomic_sub_fetch(&(v)->_refcount, 1, __ATOMIC_RELAXED) == 0) {
    RSValue_Free(v);
  }
}

int RSValue_IsNull(const RSValue *value) {
  if (!value || value == RS_NullVal()) return 1;
  if (value->_t == RSValue_Reference) return RSValue_IsNull(value->_ref);
  return 0;
}

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
void RSValue_Free(RSValue *v) {
  RSValue_Clear(v);
  if (v->_allocated) {
    mempool_release(getPool(), v);
  }
}

RSValue RS_Value(RSValueType t) {
  RSValue v = (RSValue){
      ._t = t,
      ._refcount = 1,
      ._allocated = 0,
  };
  return v;
}

inline void RSValue_SetNumber(RSValue *v, double n) {
  v->_t = RSValue_Number;
  v->_numval = n;
}

inline void RSValue_SetString(RSValue *v, char *str, size_t len) {
  v->_t = RSValue_String;
  v->_strval.len = len;
  v->_strval.str = str;
  v->_strval.stype = RSString_Malloc;
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
  v->_t = RSValue_String;
  v->_strval.len = sdslen(s);
  v->_strval.str = s;
  v->_strval.stype = RSString_SDS;
}
inline void RSValue_SetConstString(RSValue *v, const char *str, size_t len) {
  v->_t = RSValue_String;
  v->_strval.len = len;
  v->_strval.str = (char *)str;
  v->_strval.stype = RSString_Const;
}

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
inline RSValue *RS_StringVal(char *str, uint32_t len) {
  RS_LOG_ASSERT(len <= (UINT32_MAX >> 4), "string length exceeds limit");
  RSValue *v = RS_NewValue(RSValue_String);
  v->_strval.str = str;
  v->_strval.len = len;
  v->_strval.stype = RSString_Malloc;
  return v;
}

/* Same as RS_StringVal but with explicit string type */
inline RSValue *RS_StringValT(char *str, uint32_t len, RSStringType t) {
  RSValue *v = RS_NewValue(RSValue_String);
  v->_strval.str = str;
  v->_strval.len = len;
  v->_strval.stype = t;
  return v;
}

/* Wrap a redis string value */
RSValue *RS_RedisStringVal(RedisModuleString *str) {
  RSValue *v = RS_NewValue(RSValue_RedisString);
  v->_rstrval = str;
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
  ret->_t = RSValue_OwnRstring;
  return ret;
}

void RSValue_MakeRStringOwner(RSValue *v) {
  RS_LOG_ASSERT(v->_t == RSValue_RedisString, "RSvalue type should be string");
  v->_t = RSValue_OwnRstring;
  RedisModule_RetainString(RSDummyContext, v->_rstrval);
}

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
void RSValue_ToString(RSValue *dst, RSValue *v) {
  switch (v->_t) {
    case RSValue_String:
      RSValue_MakeReference(dst, v);
      break;
    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t sz;
      const char *str = RedisModule_StringPtrLen(v->_rstrval, &sz);
      RSValue_SetConstString(dst, str, sz);
      break;
    }
    case RSValue_Number: {
      char tmpbuf[128];
      size_t len = RSValue_NumToString(v, tmpbuf);
      char *buf = rm_strdup(tmpbuf);
      RSValue_SetString(dst, buf, len);
      break;
    }
    case RSValue_Reference:
      return RSValue_ToString(dst, v->_ref);

    case RSValue_Trio:
      return RSValue_ToString(dst, RSValue_Trio_GetLeft(v));

    case RSValue_Null:
    default:
      return RSValue_SetConstString(dst, "", 0);
  }
}

RSValue *RSValue_ParseNumber(const char *p, size_t l) {

  char *e;
  errno = 0;
  double d = fast_float_strtod(p, &e);
  if ((errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL)) || (errno != 0 && d == 0) ||
      *e != '\0') {
    return NULL;
  }
  return RS_NumVal(d);
}

/* Convert a value to a number, either returning the actual numeric values or by parsing a string
into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
not. If possible, we put the actual value into the double pointer */
int RSValue_ToNumber(const RSValue *v, double *d) {
  if (RSValue_IsNull(v)) return 0;
  v = RSValue_Dereference(v);

  const char *p = NULL;
  size_t l = 0;
  switch (v->_t) {
    // for numerics - just set the value and return
    case RSValue_Number:
      *d = v->_numval;
      return 1;

    case RSValue_String:
      // C strings - take the ptr and len
      p = v->_strval.str;
      l = v->_strval.len;
      break;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      // Redis strings - take the number and len
      p = RedisModule_StringPtrLen(v->_rstrval, &l);
      break;

    case RSValue_Trio:
      return RSValue_ToNumber(RSValue_Trio_GetLeft(v), d);

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
    *d = fast_float_strtod(p, &e);
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

  switch (value->_t) {
    case RSValue_String:
      if (lenp) {
        *lenp = value->_strval.len;
      }
      return value->_strval.str;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      return RedisModule_StringPtrLen(value->_rstrval, lenp);
    case RSValue_Trio:
      return RSValue_StringPtrLen(RSValue_Trio_GetLeft(value), lenp);
    default:
      return NULL;
  }
}

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
const char *RSValue_ConvertStringPtrLen(const RSValue *value, size_t *lenp, char *buf,
                                        size_t buflen) {
  value = RSValue_Dereference(value);

  if (RSValue_IsStringVariant(value)) {
    return RSValue_StringPtrLen(value, lenp);
  } else if (value->_t == RSValue_Number) {
    size_t n = snprintf(buf, buflen, "%f", value->_numval);
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
  v->_numval = n;
  return v;
}

RSValue *RS_Int64Val(int64_t dd) {
  RSValue *v = RS_NewValue(RSValue_Number);
  v->_numval = dd;
  return v;
}

RSValue *RSValue_NewArray(RSValue **vals, uint32_t len) {
  RSValue *arr = RS_NewValue(RSValue_Array);
  arr->_arrval.vals = vals;
  arr->_arrval.len = len;
  return arr;
}

RSValue *RSValue_NewMap(RSValue **pairs, uint32_t numPairs) {
  RSValue *map = RS_NewValue(RSValue_Map);
  map->_mapval.pairs = pairs;
  map->_mapval.len = numPairs;
  return map;
}

RSValue *RS_VStringArray(uint32_t sz, ...) {
  RSValue **arr = RSValue_AllocateArray(sz);
  va_list ap;
  va_start(ap, sz);
  for (uint32_t i = 0; i < sz; i++) {
    char *p = va_arg(ap, char *);
    arr[i] = RS_StringValC(p);
  }
  va_end(ap);
  return RSValue_NewArray(arr, sz);
}

/* Wrap an array of NULL terminated C strings into an RSValue array */
RSValue *RS_StringArray(char **strs, uint32_t sz) {
  RSValue **arr = RSValue_AllocateArray(sz);

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValC(strs[i]);
  }
  return RSValue_NewArray(arr, sz);
}

RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st) {
  RSValue **arr = RSValue_AllocateArray(sz);

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValT(strs[i], strlen(strs[i]), st);
  }
  return RSValue_NewArray(arr, sz);
}

RSValue RS_NULL = {._t = RSValue_Null, ._refcount = 1, ._allocated = 0};
/* Returns a pointer to the NULL RSValue */
RSValue *RS_NullVal() {
  return &RS_NULL;
}

RSValue *RS_TrioVal(RSValue *val, RSValue *otherval, RSValue *other2val) {
  RSValue *trio = RS_NewValue(RSValue_Trio);
  trio->_trioval.vals = rm_calloc(3, sizeof(*trio->_trioval.vals));
  trio->_trioval.vals[0] = val;
  trio->_trioval.vals[1] = otherval;
  trio->_trioval.vals[2] = other2val;
  return trio;
}

static inline int cmp_strings(const char *s1, const char *s2, size_t l1, size_t l2) {
  int cmp = strncmp(s1, s2, MIN(l1, l2));
  if (l1 == l2) {
    // if the strings are the same length, just return the result of strcmp
    return cmp;
  } else {  // if the lengths aren't identical
    // if the strings are identical but the lengths aren't, return the longer string
    if (cmp == 0) {
      return l1 > l2 ? 1 : -1;
    } else {  // the strings are lexically different, just return that
      return cmp;
    }
  }
}

static inline int compare_arrays_first(const RSValue *arr1, const RSValue *arr2, QueryError *qerr) {
  uint32_t len1 = arr1->_arrval.len;
  uint32_t len2 = arr2->_arrval.len;

  uint32_t len = MIN(len1, len2);
  if (len) {
    // Compare only the first entry
    return RSValue_Cmp(arr1->_arrval.vals[0], arr2->_arrval.vals[0], qerr);
  }
  return len1 - len2;
}

// TODO: Use when SORTABLE is not looking only at the first array element
static inline int compare_arrays(const RSValue *arr1, const RSValue *arr2, QueryError *qerr) {
  uint32_t len1 = arr1->_arrval.len;
  uint32_t len2 = arr2->_arrval.len;

  uint32_t len = MIN(len1, len2);
  for (uint32_t i = 0; i < len; i++) {
    int cmp = RSValue_Cmp(arr1->_arrval.vals[i], arr2->_arrval.vals[i], qerr);
    if (cmp != 0) {
      return cmp;
    }
  }
  return len1 - len2;
}



static inline int cmp_numbers(const RSValue *v1, const RSValue *v2) {
  return v1->_numval > v2->_numval ? 1 : (v1->_numval < v2->_numval ? -1 : 0);
}

static inline int convert_to_number(const RSValue *v, RSValue *vn, QueryError *qerr) {
  double d;
  if (!RSValue_ToNumber(v, &d)) {
    if (!qerr) return 0;

    const char *s = RSValue_StringPtrLen(v, NULL);
    QueryError_SetWithUserDataFmt(qerr, QUERY_ENOTNUMERIC, "Error converting string", " '%s' to number", s);
    return 0;
  }

  RSValue_SetNumber(vn, d);
  return 1;
}

static int RSValue_CmpNC(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  switch (v1->_t) {
    case RSValue_Number:
      return cmp_numbers(v1, v2);
    case RSValue_String:
      return cmp_strings(v1->_strval.str, v2->_strval.str, v1->_strval.len, v2->_strval.len);
    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t l1, l2;
      const char *s1 = RedisModule_StringPtrLen(v1->_rstrval, &l1);
      const char *s2 = RedisModule_StringPtrLen(v2->_rstrval, &l2);
      return cmp_strings(s1, s2, l1, l2);
    }
    case RSValue_Trio:
      return RSValue_Cmp(RSValue_Trio_GetLeft(v1), RSValue_Trio_GetLeft(v2), qerr);
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
  if (v1->_t == v2->_t) {
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
    if (v1->_t == RSValue_Number) {
      RSValue v2n;
      if (!convert_to_number(v2, &v2n, qerr)) {
        // if it is possible to indicate an error, return
        if (qerr) return 0;
        // otherwise, fallback to string comparison
        break;
      }
      return cmp_numbers(v1, &v2n);
    } else if (v2->_t == RSValue_Number) {
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

  if (v1->_t == v2->_t) {
    return RSValue_CmpNC(v1, v2, qerr) == 0;
  }

  if (v1 == RS_NullVal() || v2 == RS_NullVal()) {
    return 0;
  }

  // if either of the arguments is a number, convert the other one to a number
  RSValue vn;
  if (v1->_t == RSValue_Number) {
    if (!convert_to_number(v2, &vn, NULL)) return 0;
    return cmp_numbers(v1, &vn) == 0;
  } else if (v2->_t == RSValue_Number) {
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

sds RSValue_DumpSds(const RSValue *v, sds s, bool obfuscate) {
  if (!v) {
    return sdscat(s, "nil");
  }
  switch (v->_t) {
    case RSValue_String:
      if (obfuscate) {
        const char *obfuscated = Obfuscate_Text(v->_strval.str);
        return sdscatfmt(s, "\"%s\"", obfuscated);
      } else {
        s = sdscat(s, "\"");
        s = sdscatlen(s, v->_strval.str, v->_strval.len);
        s = sdscat(s, "\"");
        return s;
      }
      break;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      if (obfuscate) {
        size_t len;
        const char *obfuscated = Obfuscate_Text(RedisModule_StringPtrLen(v->_rstrval, &len));
        return sdscatfmt(s, "\"%s\"", obfuscated);
      } else {
        size_t len;
        const char *str = RedisModule_StringPtrLen(v->_rstrval, &len);
        s = sdscat(s, "\"");
        s = sdscatlen(s, str, len);
        s = sdscat(s, "\"");
        return s;
      }
      break;
    case RSValue_Number: {
      if (obfuscate) {
        return sdscat(s, Obfuscate_Number(v->_numval));
      } else {
        char buf[128];
        size_t len = RSValue_NumToString(v, buf);
        return sdscatlen(s, buf, len);
      }
      break;
    }
    case RSValue_Null:
      return sdscat(s, "NULL");
      break;
    case RSValue_Undef:
      return sdscat(s, "<Undefined>");
    case RSValue_Array:
      s = sdscat(s, "[");
      for (uint32_t i = 0; i < v->_arrval.len; i++) {
        if (i > 0)
          s = sdscat(s, ", ");
        s = RSValue_DumpSds(v->_arrval.vals[i], s, obfuscate);
      }
      return sdscat(s, "]");
      break;
    case RSValue_Map:
      s = sdscat(s, "{");
      for (uint32_t i = 0; i < v->_mapval.len; i++) {
        if (i > 0)
          s = sdscat(s, ", ");
        s = RSValue_DumpSds(v->_mapval.pairs[RSVALUE_MAP_KEYPOS(i)], s, obfuscate);
        s = sdscat(s, ": ");
        s = RSValue_DumpSds(v->_mapval.pairs[RSVALUE_MAP_VALUEPOS(i)], s, obfuscate);
      }
      s = sdscat(s, "}");
      break;
    case RSValue_Reference:
      return RSValue_DumpSds(v->_ref, s, obfuscate);
      break;

    case RSValue_Trio:
      return RSValue_DumpSds(RSValue_Trio_GetLeft(v), s, obfuscate);
      break;
  }
}
