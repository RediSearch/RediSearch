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
#include "rmalloc.h"
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

///////////////////////////////////////////////////////////////
// Helper functions used throughout (no RSValue dependencies)
///////////////////////////////////////////////////////////////

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

static inline int cmp_numbers(const RSValue *v1, const RSValue *v2) {
  return v1->_numval > v2->_numval ? 1 : (v1->_numval < v2->_numval ? -1 : 0);
}

// Trio getters (needed by RSValue_Clear)
RSValue *RSValue_Trio_GetLeft(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValueType_Trio);
  return v->_trioval.vals[0];
}

RSValue *RSValue_Trio_GetMiddle(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValueType_Trio);
  return v->_trioval.vals[1];
}

RSValue *RSValue_Trio_GetRight(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValueType_Trio);
  return v->_trioval.vals[2];
}

///////////////////////////////////////////////////////////////
// Setters (needed by some constructors)
///////////////////////////////////////////////////////////////

// Forward declarations needed for RSValue_Clear and other functions
void RSValue_Free(RSValue *v);
void RSValue_DecrRef(RSValue* v);
RSValue* RSValue_IncrRef(RSValue* v);

inline void RSValue_SetNumber(RSValue *v, double n) {
  v->_t = RSValueType_Number;
  v->_numval = n;
}

inline void RSValue_SetString(RSValue *v, char *str, size_t len) {
  v->_t = RSValueType_String;
  v->_strval.len = len;
  v->_strval.str = str;
  v->_strval.stype = RSStringType_RMAlloc;
}


inline void RSValue_SetConstString(RSValue *v, const char *str, size_t len) {
  v->_t = RSValueType_String;
  v->_strval.len = len;
  v->_strval.str = (char *)str;
  v->_strval.stype = RSStringType_Const;
}

void RSValue_MakeRStringOwner(RSValue *v) {
  RS_LOG_ASSERT(v->_t == RSValueType_RedisString, "RSvalue type should be string");
  v->_t = RSValueType_OwnRstring;
  RedisModule_RetainString(RSDummyContext, v->_rstrval);
}

///////////////////////////////////////////////////////////////
// Constructors
///////////////////////////////////////////////////////////////

RSValue *RSValue_NewWithType(RSValueType t) {
  RSValue *v = mempool_get(getPool());
  v->_t = t;
  v->_refcount = 1;
  v->_allocated = 1;
  return v;
}

RSValue RSValue_Undefined() {
  RSValue v;
  v._t = RSValueType_Undef;
  v._allocated = 0;
  v._refcount = 1;
  return v;
}

RSValue RSValue_Number(double n) {
  RSValue v = {0};

  v._t = RSValueType_Number;
  v._refcount = 1;
  v._allocated = 0;
  v._numval = n;

  return v;
}

RSValue RSValue_String(char *str, uint32_t len) {
  RSValue v = {0};
  v._allocated = 0;
  v._refcount = 1;
  v._t = RSValueType_String;
  v._strval.str = str;
  v._strval.len = len;
  v._strval.stype = RSStringType_RMAlloc;
  return v;
}

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
inline RSValue *RSValue_NewString(char *str, uint32_t len) {
  RS_LOG_ASSERT(len <= (UINT32_MAX >> 4), "string length exceeds limit");
  RSValue *v = RSValue_NewWithType(RSValueType_String);
  v->_strval.str = str;
  v->_strval.len = len;
  v->_strval.stype = RSStringType_RMAlloc;
  return v;
}

/* Same as RSValue_NewString but for const strings */
RSValue *RSValue_NewConstString(const char *str, uint32_t len) {
  RSValue *v = RSValue_NewWithType(RSValueType_String);
  v->_strval.str = (char *) str;
  v->_strval.len = len;
  v->_strval.stype = RSStringType_Const;
  return v;
}

/* Wrap a redis string value */
RSValue *RSValue_NewBorrowedRedisString(RedisModuleString *str) {
  RSValue *v = RSValue_NewWithType(RSValueType_RedisString);
  v->_rstrval = str;
  return v;
}

RSValue *RSValue_NewOwnedRedisString(RedisModuleString *str) {
  RSValue *r = RSValue_NewBorrowedRedisString(str);
  RSValue_MakeRStringOwner(r);
  return r;
}

// TODO : NORMALLY
RSValue *RSValue_NewStolenRedisString(RedisModuleString *str) {
  RSValue *ret = RSValue_NewBorrowedRedisString(str);
  ret->_t = RSValueType_OwnRstring;
  return ret;
}

RSValue RS_NULL = {._t = RSValueType_Null, ._refcount = 1, ._allocated = 0};
/* Returns a pointer to the NULL RSValue */
RSValue *RSValue_NullStatic() {
  return &RS_NULL;
}

RSValue *RSValue_NewCopiedString(const char *s, size_t n) {
  RSValue *v = RSValue_NewWithType(RSValueType_String);
  char *cp = rm_malloc(n + 1);
  cp[n] = 0;
  memcpy(cp, s, n);
  RSValue_SetString(v, cp, n);
  return v;
}

RSValue *RSValue_NewParsedNumber(const char *p, size_t l) {

  char *e;
  errno = 0;
  double d = fast_float_strtod(p, &e);
  if ((errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL)) || (errno != 0 && d == 0) ||
      *e != '\0') {
    return NULL;
  }
  return RSValue_NewNumber(d);
}

/* Wrap a number into a value object */
RSValue *RSValue_NewNumber(double n) {
  RSValue *v = RSValue_NewWithType(RSValueType_Number);
  v->_numval = n;
  return v;
}

RSValue *RSValue_NewNumberFromInt64(int64_t dd) {
  RSValue *v = RSValue_NewWithType(RSValueType_Number);
  v->_numval = dd;
  return v;
}

RSValue *RSValue_NewArray(RSValue **vals, uint32_t len) {
  RSValue *arr = RSValue_NewWithType(RSValueType_Array);
  arr->_arrval.vals = vals;
  arr->_arrval.len = len;
  return arr;
}

RSValueMap RSValueMap_AllocUninit(uint32_t len) {
  RSValueMapEntry *entries =
    (len > 0) ? (RSValueMapEntry*) rm_malloc(len * sizeof(RSValueMapEntry)) : NULL;
  RSValueMap map = {
    .len = len,
    .entries = entries,
  };

  return map;
}

RSValue *RSValue_NewMap(RSValueMap map) {
  RSValue *v = RSValue_NewWithType(RSValueType_Map);
  v->_mapval = map;
  return v;
}

RSValue *RSValue_NewVStringArray(uint32_t sz, ...) {
  RSValue **arr = RSValue_AllocateArray(sz);
  va_list ap;
  va_start(ap, sz);
  for (uint32_t i = 0; i < sz; i++) {
    char *p = va_arg(ap, char *);
    arr[i] = RSValue_NewCString(p);
  }
  va_end(ap);
  return RSValue_NewArray(arr, sz);
}

/* Wrap an array of NULL terminated C strings into an RSValue array */
RSValue *RSValue_NewStringArray(char **strs, uint32_t sz) {
  RSValue **arr = RSValue_AllocateArray(sz);

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RSValue_NewCString(strs[i]);
  }
  return RSValue_NewArray(arr, sz);
}

RSValue *RSValue_NewConstStringArray(char **strs, uint32_t sz) {
  RSValue **arr = RSValue_AllocateArray(sz);

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RSValue_NewConstString(strs[i], strlen(strs[i]));
  }
  return RSValue_NewArray(arr, sz);
}

RSValue *RSValue_NewTrio(RSValue *val, RSValue *otherval, RSValue *other2val) {
  RSValue *trio = RSValue_NewWithType(RSValueType_Trio);
  trio->_trioval.vals = rm_calloc(3, sizeof(*trio->_trioval.vals));
  trio->_trioval.vals[0] = val;
  trio->_trioval.vals[1] = otherval;
  trio->_trioval.vals[2] = other2val;
  return trio;
}

///////////////////////////////////////////////////////////////
// Getters and Setters (grouped by field)
///////////////////////////////////////////////////////////////

// Type getters
RSValueType RSValue_Type(const RSValue *v) {
  RS_ASSERT(v);
  return v->_t;
}

bool RSValue_IsReference(const RSValue *v) {
  return (v && v->_t == RSValueType_Reference);
}

bool RSValue_IsNumber(const RSValue *v) {
  return (v && v->_t == RSValueType_Number);
}

bool RSValue_IsString(const RSValue *v) {
  return (v && v->_t == RSValueType_String);
}

bool RSValue_IsArray(const RSValue *v) {
  return (v && v->_t == RSValueType_Array);
}

bool RSValue_IsRedisString(const RSValue *v) {
  return (v && v->_t == RSValueType_RedisString);
}

bool RSValue_IsOwnRString(const RSValue *v) {
  return (v && v->_t == RSValueType_OwnRstring);
}

bool RSValue_IsTrio(const RSValue *v) {
  return (v && v->_t == RSValueType_Trio);
}

int RSValue_IsNull(const RSValue *value) {
  if (!value || value == RSValue_NullStatic()) return 1;
  if (value->_t == RSValueType_Reference) return RSValue_IsNull(value->_ref);
  return 0;
}

// Number getters/setters
double RSValue_Number_Get(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValueType_Number);
  return v->_numval;
}

void RSValue_IntoNumber(RSValue *v, double n) {
  RS_ASSERT(v);
  v->_t = RSValueType_Number;
  v->_numval = n;
}

// String getters/setters
char *RSValue_String_Get(const RSValue *v, uint32_t *lenp) {
  RS_ASSERT(v && v->_t == RSValueType_String);
  if(lenp) {
    *lenp = v->_strval.len;
  }
  return v->_strval.str;
}

char *RSValue_String_GetPtr(const RSValue *v) {
  return RSValue_String_Get(v, NULL);
}

RedisModuleString *RSValue_RedisString_Get(const RSValue *v) {
  RS_ASSERT(v && (v->_t == RSValueType_RedisString || v->_t == RSValueType_OwnRstring));
  return v->_rstrval;
}

// Gets the string pointer and length from the value
const char *RSValue_StringPtrLen(const RSValue *value, size_t *lenp) {
  value = RSValue_Dereference(value);

  switch (value->_t) {
    case RSValueType_String:
      if (lenp) {
        *lenp = value->_strval.len;
      }
      return value->_strval.str;
    case RSValueType_RedisString:
    case RSValueType_OwnRstring:
      return RedisModule_StringPtrLen(value->_rstrval, lenp);
    case RSValueType_Trio:
      return RSValue_StringPtrLen(RSValue_Trio_GetLeft(value), lenp);
    default:
      return NULL;
  }
}

// Map getters/setters
uint32_t RSValue_Map_Len(const RSValue *v) {
  RS_ASSERT(v && v->_t == RSValueType_Map);
  return v->_mapval.len;
}

void RSValue_Map_GetEntry(const RSValue *map, uint32_t i, RSValue **key, RSValue **val) {
  RS_ASSERT(i < RSValue_Map_Len(map));

  *key = map->_mapval.entries[i].key;
  *val = map->_mapval.entries[i].value;
}

void RSValueMap_SetEntry(RSValueMap *map, size_t i, RSValue *key, RSValue *value) {
  RS_ASSERT(i < map->len);
  map->entries[i].key = key;
  map->entries[i].value = value;
}

// Reference getters/setters
void RSValue_Clear(RSValue *v) {
  switch (v->_t) {
    case RSValueType_String:
      // free strings by allocation strategy
      switch (v->_strval.stype) {
        case RSStringType_RMAlloc:
          rm_free(v->_strval.str);
          break;
        case RSStringType_Const:
          break;
      }
      break;
    case RSValueType_Reference:
      RSValue_DecrRef(v->_ref);
      break;
    case RSValueType_OwnRstring:
      RedisModule_FreeString(RSDummyContext, v->_rstrval);
      break;
    case RSValueType_Null:
      return;  // prevent changing global RS_NULL to RSValue_Undef
    case RSValueType_Trio:
      RSValue_DecrRef(RSValue_Trio_GetLeft(v));
      RSValue_DecrRef(RSValue_Trio_GetMiddle(v));
      RSValue_DecrRef(RSValue_Trio_GetRight(v));
      rm_free(v->_trioval.vals);
      break;
    case RSValueType_Array:
      for (uint32_t i = 0; i < v->_arrval.len; i++) {
        RSValue_DecrRef(v->_arrval.vals[i]);
      }
      rm_free(v->_arrval.vals);
      break;
    case RSValueType_Map:
      for (uint32_t i = 0; i < v->_mapval.len; i++) {
        RSValue_DecrRef(v->_mapval.entries[i].key);
        RSValue_DecrRef(v->_mapval.entries[i].value);
      }
      if (v->_mapval.len > 0) {
        rm_free(v->_mapval.entries);
      }
      break;
    default:   // no free
      break;
  }

  v->_ref = NULL;
  v->_t = RSValueType_Undef;
}

RSValue* RSValue_IncrRef(RSValue* v) {
  __atomic_fetch_add(&v->_refcount, 1, __ATOMIC_RELAXED);
  return v;
}

void RSValue_DecrRef(RSValue* v) {
  if (__atomic_sub_fetch(&(v)->_refcount, 1, __ATOMIC_RELAXED) == 0) {
    RSValue_Free(v);
  }
}

// Type conversion setters
void RSValue_IntoUndefined(RSValue *v) {
  RS_ASSERT(v);
  v->_t = RSValueType_Undef;
}

void RSValue_IntoNull(RSValue *v) {
  RS_ASSERT(v);
  v->_t = RSValueType_Null;
}

// Refcount getter
uint16_t RSValue_Refcount(const RSValue *v) {
  RS_ASSERT(v);
  return v->_refcount;
}

///////////////////////////////////////////////////////////////
// Other Functions (utility, comparison, conversion, etc.)
///////////////////////////////////////////////////////////////

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
void RSValue_Free(RSValue *v) {
  RSValue_Clear(v);
  if (v->_allocated) {
    mempool_release(getPool(), v);
  }
}

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
void RSValue_ToString(RSValue *dst, RSValue *v) {
  switch (v->_t) {
    case RSValueType_String:
      RSValue_MakeReference(dst, v);
      break;
    case RSValueType_RedisString:
    case RSValueType_OwnRstring: {
      size_t sz;
      const char *str = RedisModule_StringPtrLen(v->_rstrval, &sz);
      RSValue_SetConstString(dst, str, sz);
      break;
    }
    case RSValueType_Number: {
      char tmpbuf[128];
      size_t len = RSValue_NumToString(v, tmpbuf, sizeof(tmpbuf));
      char *buf = rm_strdup(tmpbuf);
      RSValue_SetString(dst, buf, len);
      break;
    }
    case RSValueType_Reference:
      return RSValue_ToString(dst, v->_ref);

    case RSValueType_Trio:
      return RSValue_ToString(dst, RSValue_Trio_GetLeft(v));

    case RSValueType_Null:
    default:
      return RSValue_SetConstString(dst, "", 0);
  }
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
    case RSValueType_Number:
      *d = v->_numval;
      return 1;

    case RSValueType_String:
      // C strings - take the ptr and len
      p = v->_strval.str;
      l = v->_strval.len;
      break;
    case RSValueType_RedisString:
    case RSValueType_OwnRstring:
      // Redis strings - take the number and len
      p = RedisModule_StringPtrLen(v->_rstrval, &l);
      break;

    case RSValueType_Trio:
      return RSValue_ToNumber(RSValue_Trio_GetLeft(v), d);

    case RSValueType_Null:
    case RSValueType_Array:
    case RSValueType_Map:
    case RSValueType_Undef:
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

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
const char *RSValue_ConvertStringPtrLen(const RSValue *value, size_t *lenp, char *buf,
                                        size_t buflen) {
  value = RSValue_Dereference(value);

  if (RSValue_IsAnyString(value)) {
    return RSValue_StringPtrLen(value, lenp);
  } else if (value->_t == RSValueType_Number) {
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

static inline bool convert_to_number(const RSValue *v, RSValue *vn, QueryError *qerr) {
  double d;
  if (!RSValue_ToNumber(v, &d)) {
    if (!qerr) return false;

    const char *s = RSValue_StringPtrLen(v, NULL);
    QueryError_SetWithUserDataFmt(qerr, QUERY_ERROR_CODE_NOT_NUMERIC, "Error converting string", " '%s' to number", s);
    return false;
  }

  RSValue_SetNumber(vn, d);
  return true;
}

// Forward declaration for recursive call
int RSValue_Cmp(const RSValue *v1, const RSValue *v2, QueryError *qerr);

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

static int RSValue_CmpNC(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  switch (v1->_t) {
    case RSValueType_Number:
      return cmp_numbers(v1, v2);
    case RSValueType_String:
      return cmp_strings(v1->_strval.str, v2->_strval.str, v1->_strval.len, v2->_strval.len);
    case RSValueType_RedisString:
    case RSValueType_OwnRstring: {
      size_t l1, l2;
      const char *s1 = RedisModule_StringPtrLen(v1->_rstrval, &l1);
      const char *s2 = RedisModule_StringPtrLen(v2->_rstrval, &l2);
      return cmp_strings(s1, s2, l1, l2);
    }
    case RSValueType_Trio:
      return RSValue_Cmp(RSValue_Trio_GetLeft(v1), RSValue_Trio_GetLeft(v2), qerr);
    case RSValueType_Null:
      return 0;
    case RSValueType_Array:
      return compare_arrays_first(v1, v2, qerr);

    case RSValueType_Map:   // can't compare maps ATM
    default:
      return 0;
  }
}

/* Compare 2 values for sorting */
int RSValue_Cmp(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  RS_LOG_ASSERT(v1 && v2, "missing RSvalue");
  if (v1->_t == v2->_t) {
    return RSValue_CmpNC(v1, v2, qerr);
  }

  // if one of the values is null, the other wins
  if (v1 == RSValue_NullStatic()) {
    return -1;
  } else if (v2 == RSValue_NullStatic()) {
    return 1;
  }

  // if either of the arguments is a number, convert the other one to a number
  // if, however, error handling is not available, fallback to string comparison
  do {
    if (v1->_t == RSValueType_Number) {
      RSValue v2n;
      if (!convert_to_number(v2, &v2n, qerr)) {
        // if it is possible to indicate an error, return
        if (qerr) return 0;
        // otherwise, fallback to string comparison
        break;
      }
      return cmp_numbers(v1, &v2n);
    } else if (v2->_t == RSValueType_Number) {
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

/* Return 1 if the two values are equal */
int RSValue_Equal(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  RS_LOG_ASSERT(v1 && v2, "missing RSvalue");

  if (v1->_t == v2->_t) {
    return RSValue_CmpNC(v1, v2, qerr) == 0;
  }

  if (v1 == RSValue_NullStatic() || v2 == RSValue_NullStatic()) {
    return 0;
  }

  // if either of the arguments is a number, convert the other one to a number
  RSValue vn;
  if (v1->_t == RSValueType_Number) {
    if (!convert_to_number(v2, &vn, NULL)) return 0;
    return cmp_numbers(v1, &vn) == 0;
  } else if (v2->_t == RSValueType_Number) {
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
    case RSValueType_String:
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
    case RSValueType_RedisString:
    case RSValueType_OwnRstring:
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
    case RSValueType_Number: {
      if (obfuscate) {
        return sdscat(s, Obfuscate_Number(v->_numval));
      } else {
        char buf[128];
        size_t len = RSValue_NumToString(v, buf, sizeof(buf));
        return sdscatlen(s, buf, len);
      }
      break;
    }
    case RSValueType_Null:
      return sdscat(s, "NULL");
      break;
    case RSValueType_Undef:
      return sdscat(s, "<Undefined>");
      break;
    case RSValueType_Array:
      s = sdscat(s, "[");
      for (uint32_t i = 0; i < v->_arrval.len; i++) {
        if (i > 0)
          s = sdscat(s, ", ");
        s = RSValue_DumpSds(v->_arrval.vals[i], s, obfuscate);
      }
      return sdscat(s, "]");
      break;
    case RSValueType_Map:
      s = sdscat(s, "{");
      for (uint32_t i = 0; i < v->_mapval.len; i++) {
        if (i > 0)
          s = sdscat(s, ", ");
        s = RSValue_DumpSds(v->_mapval.entries[i].key, s, obfuscate);
        s = sdscat(s, ": ");
        s = RSValue_DumpSds(v->_mapval.entries[i].value, s, obfuscate);
      }
      return sdscat(s, "}");
      break;
    case RSValueType_Reference:
      return RSValue_DumpSds(v->_ref, s, obfuscate);
      break;
    case RSValueType_Trio:
      return RSValue_DumpSds(RSValue_Trio_GetLeft(v), s, obfuscate);
      break;
  }
}

///////////////////////////////////////////////////////////////
// Extra functions not in header
///////////////////////////////////////////////////////////////

RSValue RS_Value(RSValueType t) {
  RSValue v = (RSValue){
      ._t = t,
      ._refcount = 1,
      ._allocated = 0,
  };
  return v;
}
