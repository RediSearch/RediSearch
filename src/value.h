/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef RS_VALUE_H_
#define RS_VALUE_H_

#include "redisearch.h"
#include "rmalloc.h"
#include "query_error.h"

#include "util/fnv.h"

#include "rmutil/rm_assert.h"
#include "hiredis/sds.h"

#include <string.h>
#include <sys/param.h>
#include <stdarg.h>
#include <errno.h>

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////

// Enumeration of possible value types
typedef enum {
  // This is the NULL/Empty value
  RSValue_Undef = 0,

  RSValue_Number = 1,
  RSValue_String = 3,
  RSValue_Null = 4,
  RSValue_RedisString = 5,
  // An array of values, that can be of any type
  RSValue_Array = 6,
  // A redis string, but we own a refcount to it; tied to RSDummy
  RSValue_OwnRstring = 7,
  // Reference to another value
  RSValue_Reference = 8,
  // Trio value
  RSValue_Trio = 9,
  // Map value
  RSValue_Map = 10,

} RSValueType;

/* Enumerate sub-types of C strings, basically the free() strategy */
typedef enum {
  RSString_Const = 0x00,
  RSString_Malloc = 0x01,
  RSString_RMAlloc = 0x02,
  RSString_SDS = 0x03,
} RSStringType;

/**
 * Represents a key-value pair entry in an RSValueMap.
 * Both key and value are RSValue pointers that are owned by the map.
 */
typedef struct RSValueMapEntry {
  struct RSValue *key;
  struct RSValue *value;
} RSValueMapEntry;

#pragma pack(4)
/**
 * Represents a map (dictionary/hash table) of RSValue key-value pairs.
 * The map owns all keys and values and will free them when the map is freed.
 */
typedef struct RSValueMap {
  uint32_t len;
  RSValueMapEntry *entries;
} RSValueMap;

// Variant value union
typedef struct RSValue {

  union {
    // numeric value
    double _numval;

    // int64_t intval;

    // string value
    struct {
      char *str;
      uint32_t len : 29;
      // sub type for string
      RSStringType stype : 3;
    } _strval;

    // array value
    struct {
      struct RSValue **vals;
      uint32_t len;
    } _arrval;

    // map value
    RSValueMap _mapval;

    struct {
      /**
       * Trio value
       *
       * Allows keeping a value, together with two additional values.
       *
       * For example, keeping a value and, in addition, a different value for serialization,
       * such as a JSON String representation, and one for its expansion.
       */

      // An array of 2 RSValue *'s
      // The first entry is the value, the second entry is the additional value
      struct RSValue **vals;
    } _trioval;

    // redis string value
    struct RedisModuleString *_rstrval;

    // reference to another value
    struct RSValue *_ref;
  };
  RSValueType _t : 7;
  uint8_t _allocated : 1;
  uint16_t _refcount;

#ifdef __cplusplus
  RSValue() {
  }
  RSValue(RSValueType t_) : _ref(NULL), _t(t_), _refcount(0), _allocated(0) {
  }

#endif
} RSValue;
#pragma pack()

#define APIVERSION_RETURN_MULTI_CMP_FIRST 3

/**
 * Clears the underlying storage of the value, and makes it
 * be a reference to the NULL value
 */
void RSValue_Clear(RSValue *v);

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
void RSValue_Free(RSValue *v);

RSValue *RSValue_IncrRef(RSValue *v);
void RSValue_Decref(RSValue *v);

RSValue *RS_NewValue(RSValueType t);

/**
 * Creates a static undefined RSValue.
 * The returned value is not allocated on the heap and should not be freed.
 * @return An undefined RSValue with static storage
 */
RSValue RSValue_Undefined_Static();

#ifndef __cplusplus
static RSValue RS_StaticValue(RSValueType t) {
  RSValue v = (RSValue){
      ._t = t,
      ._refcount = 1,
      ._allocated = 0,
  };
  return v;
}
#endif

void RSValue_SetNumber(RSValue *v, double n);
void RSValue_SetString(RSValue *v, char *str, size_t len);
void RSValue_SetSDS(RSValue *v, sds s);
void RSValue_SetConstString(RSValue *v, const char *str, size_t len);

#ifndef __cplusplus
static inline void RSValue_MakeReference(RSValue *dst, RSValue *src) {
  RS_LOG_ASSERT(src, "RSvalue is missing");
  RSValue_Clear(dst);
  dst->_t = RSValue_Reference;
  dst->_ref = RSValue_IncrRef(src);
}

static inline void RSValue_MakeOwnReference(RSValue *dst, RSValue *src) {
  RSValue_MakeReference(dst, src);
  RSValue_Decref(src);
}
#endif

/**
 * Creates a static RSValue containing a number.
 * The returned value is not allocated on the heap and should not be freed.
 * @param n The numeric value to wrap
 * @return A static RSValue of type RSValue_Number
 */
RSValue RSValue_NewStatic_Number(double n);

/**
 * Creates a static RSValue containing a malloc'd string.
 * The returned value itself is not heap-allocated, but takes ownership of the string.
 * @param str The malloc'd string to wrap (ownership is transferred)
 * @param len The length of the string
 * @return A static RSValue of type RSValue_String with RSString_Malloc subtype
 */
RSValue RSValue_NewStatic_String_Malloc(char *str, uint32_t len);

/* Return the value itself or its referred value */
static inline RSValue *RSValue_Dereference(const RSValue *v) {
  for (; v && v->_t == RSValue_Reference; v = v->_ref);
  return (RSValue *)v;
}

/**
 * Check whether the RSValue is of type RSValue_Trio.
 * @param v The value to check
 * @return true if the value is a Trio, false otherwise
 */
bool RSValue_IsTrio(const RSValue *v);

/**
 * Get the left value of a Trio value.
 * The passed RSValue must be of type RSValue_Trio.
 * @param v The Trio value to extract the left value from
 * @return The left value of the Trio
 */
RSValue *RSValue_Trio_GetLeft(const RSValue *v);

/**
 * Get the middle value of a Trio value.
 * The passed RSValue must be of type RSValue_Trio.
 * @param v The Trio value to extract the middle value from
 * @return The middle value of the Trio
 */
RSValue *RSValue_Trio_GetMiddle(const RSValue *v);

/**
 * Get the right value of a Trio value.
 * The passed RSValue must be of type RSValue_Trio.
 * @param v The Trio value to extract the right value from
 * @return The right value of the Trio
 */
RSValue *RSValue_Trio_GetRight(const RSValue *v);


/**
 * Get the type of an RSValue.
 * @param v The value to inspect
 * @return The RSValueType of the value
 */
RSValueType RSValue_Type(const RSValue *v);

/**
 * Check if the RSValue is a reference type.
 * @param v The value to check
 * @return true if the value is of type RSValue_Reference, false otherwise
 */
bool RSValue_IsReference(const RSValue *v);

/**
 * Check if the RSValue is a number type.
 * @param v The value to check
 * @return true if the value is of type RSValue_Number, false otherwise
 */
bool RSValue_IsNumber(const RSValue *v);

/**
 * Check if the RSValue is a string type.
 * @param v The value to check
 * @return true if the value is of type RSValue_String, false otherwise
 */
bool RSValue_IsString(const RSValue *v);

/**
 * Check if the RSValue is an array type.
 * @param v The value to check
 * @return true if the value is of type RSValue_Array, false otherwise
 */
bool RSValue_IsArray(const RSValue *v);

/**
 * Check if the RSValue is a Redis string type.
 * @param v The value to check
 * @return true if the value is of type RSValue_RedisString, false otherwise
 */
bool RSValue_IsRedisString(const RSValue *v);

/**
 * Check if the RSValue is an owned Redis string type.
 * @param v The value to check
 * @return true if the value is of type RSValue_OwnRstring, false otherwise
 */
bool RSValue_IsOwnRString(const RSValue *v);

/**
 * Get the numeric value from an RSValue.
 * The value must be of type RSValue_Number.
 * @param v The value to extract the number from
 * @return The double value stored in the RSValue
 */
double RSValue_Number_Get(const RSValue *v);

/**
 * Get the string value and length from an RSValue.
 * The value must be of type RSValue_String.
 * @param v The value to extract the string from
 * @param lenp Output parameter for the string length. Only used if not NULL
 * @return Pointer to the string data
 */
char *RSValue_String_Get(const RSValue *v, uint32_t *lenp);

/**
 * Get the string pointer from an RSValue without length.
 * The value must be of type RSValue_String.
 * @param v The value to extract the string from
 * @return Pointer to the string data
 */
char *RSValue_String_GetPtr(const RSValue *v);

/**
 * Get the RedisModuleString from an RSValue.
 * The value must be of type RSValue_RedisString or RSValue_OwnRstring.
 * @param v The value to extract the Redis string from
 * @return The RedisModuleString pointer
 */
RedisModuleString *RSValue_RedisString_Get(const RSValue *v);

/**
 * Get the number of key-value pairs in a map RSValue.
 * @param v The map value
 * @return The number of pairs in the map
 */
uint32_t RSValue_MapLen(const RSValue *v);

/**
 * Get a key-value pair from a map RSValue at a specific index.
 * @param map The map value
 * @param i The index of the pair to retrieve
 * @param key Output parameter for the key (can be NULL)
 * @param val Output parameter for the value (can be NULL)
 */
void RSValue_MapGetEntry(const RSValue *map, uint32_t i, RSValue **key, RSValue **val);

/**
 * Convert an RSValue to undefined type in-place.
 * This clears the existing value and sets it to RSValue_Undef.
 * @param v The value to modify
 */
void RSValue_IntoUndefined(RSValue *v);

/**
 * Convert an RSValue to a number type in-place.
 * This clears the existing value and sets it to RSValue_Number with the given value.
 * @param v The value to modify
 * @param n The numeric value to set
 */
void RSValue_IntoNumber(RSValue *v, double n);

/**
 * Convert an RSValue to null type in-place.
 * This clears the existing value and sets it to RSValue_Null.
 * @param v The value to modify
 */
void RSValue_IntoNull(RSValue *v);

/**
 * Get the reference count of an RSValue.
 * @param v The value to inspect
 * @return The current reference count
 */
uint16_t RSValue_Refcount(const RSValue *v);

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
RSValue *RS_StringVal(char *str, uint32_t len);

/* Same as RS_StringVal but with explicit string type */
RSValue *RS_StringValT(char *str, uint32_t len, RSStringType t);

/* Wrap a string with length into a value object, assuming the string is a null terminated C
 * string
 */
static inline RSValue *RS_StringValC(char *s) {
  return RS_StringVal(s, strlen(s));
}
static inline RSValue *RS_ConstStringVal(const char *s, size_t n) {
  return RS_StringValT((char *)s, n, RSString_Const);
}
#define RS_ConstStringValC(s) RS_ConstStringVal(s, strlen(s))

/* Wrap a redis string value */
RSValue *RS_RedisStringVal(RedisModuleString *str);

/**
 *  Create a new value object which increments and owns a reference to the string
 */
RSValue *RS_OwnRedisStringVal(RedisModuleString *str);

/**
 * Create a new value object which steals a reference to the string
 */
RSValue *RS_StealRedisStringVal(RedisModuleString *s);

void RSValue_MakeRStringOwner(RSValue *v);

// Returns true if the value contains a string
static inline int RSValue_IsStringVariant(const RSValue *value) {
  return value && (value->_t == RSValue_String || value->_t == RSValue_RedisString ||
                   value->_t == RSValue_OwnRstring);
}

/* Create a new NULL RSValue */
RSValue *RS_NullVal();

/* Return 1 if the value is NULL, RSValue_Null or a reference to RSValue_Null */
int RSValue_IsNull(const RSValue *value);

/**
 * Copies a string using the default mechanism. Returns the copied value.
 */
RSValue *RS_NewCopiedString(const char *s, size_t dst);

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
void RSValue_ToString(RSValue *dst, RSValue *v);

/* New value from string, trying to parse it as a number */
RSValue *RSValue_ParseNumber(const char *p, size_t l);

/* Convert a value to a number, either returning the actual numeric values or by parsing a string
into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
not. If possible, we put the actual value into the double pointer */
int RSValue_ToNumber(const RSValue *v, double *d);

/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
static inline uint64_t RSValue_Hash(const RSValue *v, uint64_t hval) {
  switch (v->_t) {
    case RSValue_Reference:
      return RSValue_Hash(v->_ref, hval);
    case RSValue_String:

      return fnv_64a_buf(v->_strval.str, v->_strval.len, hval);
    case RSValue_Number:
      return fnv_64a_buf(&v->_numval, sizeof(double), hval);

    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t sz;
      const char *c = RedisModule_StringPtrLen(v->_rstrval, &sz);
      return fnv_64a_buf((void *)c, sz, hval);
    }
    case RSValue_Null:
      return hval + 1;

    case RSValue_Array: {
      for (uint32_t i = 0; i < v->_arrval.len; i++) {
        hval = RSValue_Hash(v->_arrval.vals[i], hval);
      }
      return hval;
    }

    case RSValue_Map:
      for (uint32_t i = 0; i < v->_mapval.len; i++) {
        hval = RSValue_Hash(v->_mapval.entries[i].key, hval);
        hval = RSValue_Hash(v->_mapval.entries[i].value, hval);
      }
      return hval;

    case RSValue_Undef:
      return 0;

    case RSValue_Trio:
      return RSValue_Hash(RSValue_Trio_GetLeft(v), hval);
  }

  return 0;
}

// Gets the string pointer and length from the value
const char *RSValue_StringPtrLen(const RSValue *value, size_t *lenp);

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
const char *RSValue_ConvertStringPtrLen(const RSValue *value, size_t *lenp, char *buf,
                                        size_t buflen);

/* Wrap a number into a value object */
RSValue *RS_NumVal(double n);

RSValue *RS_Int64Val(int64_t ii);

/**
 * Create a new array from existing values
 * Take ownership of the values (values would be freed when array is freed)
 * @param vals the values array to use for the array
 * @param len number of values
 */
RSValue *RSValue_NewArray(RSValue **vals, uint32_t len);

/**
 * Helper function to allocate memory before passing it to RSValue_NewArray
 */
static inline RSValue **RSValue_AllocateArray(uint32_t len) {
  return (RSValue **)rm_malloc(len * sizeof(RSValue *));
}

/**
 * Create a new RSValueMap with space for the specified number of entries.
 * The map entries are uninitialized and must be set using RSValueMap_SetEntry.
 * @param len The number of entries to allocate space for
 * @return A new RSValueMap with allocated but uninitialized entries
 */
RSValueMap RSValueMap_Create_Uninit(uint32_t len);

/**
 * Set a key-value pair at a specific index in the map.
 * Takes ownership of both the key and value RSValues.
 * @param map The map to modify
 * @param i The index where to set the entry (must be < map->len)
 * @param key The key RSValue (ownership is transferred to the map)
 * @param value The value RSValue (ownership is transferred to the map)
 */
void RSValueMap_SetEntry(RSValueMap *map, size_t i, RSValue *key, RSValue *value);

/**
 * Create a new RSValue of type RSValue_Map from an RSValueMap.
 * Takes ownership of the map structure and all its entries.
 * @param map The RSValueMap to wrap (ownership is transferred)
 * @return A new RSValue of type RSValue_Map
 */
RSValue *RSValue_NewMap(RSValueMap map);

/** Accesses the array element at a given position as an l-value */
#define RSVALUE_ARRELEM(vv, pos) ((vv)->_arrval.vals[pos])
/** Accesses the array length as an lvalue */
#define RSVALUE_ARRLEN(vv) ((vv)->_arrval.len)

RSValue *RS_VStringArray(uint32_t sz, ...);

/* Wrap an array of NULL terminated C strings into an RSValue array */
RSValue *RS_StringArray(char **strs, uint32_t sz);

/* Initialize all strings in the array with a given string type */
RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st);

/* Wrap a trio of RSValue into an RSValue Trui */
RSValue *RS_TrioVal(RSValue *val, RSValue *otherval, RSValue *other2val);

/* Compare 2 values for sorting */
int RSValue_Cmp(const RSValue *v1, const RSValue *v2, QueryError *status);

/* Return 1 if the two values are equal */
int RSValue_Equal(const RSValue *v1, const RSValue *v2, QueryError *status);

/* "truth testing" for a value. for a number - not zero. For a string/array - not empty. null is
 * considered false */
static inline int RSValue_BoolTest(const RSValue *v) {
  if (RSValue_IsNull(v)) return 0;

  v = RSValue_Dereference(v);
  switch (v->_t) {
    case RSValue_Array:
      return v->_arrval.len != 0;
    case RSValue_Number:
      return v->_numval != 0;
    case RSValue_String:
      return v->_strval.len != 0;
    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t l = 0;
      const char *p = RedisModule_StringPtrLen(v->_rstrval, &l);
      return l != 0;
    }
    default:
      return 0;
  }
}

static inline RSValue *RSValue_ArrayItem(const RSValue *arr, uint32_t index) {
  RS_ASSERT(arr && arr->_t == RSValue_Array);
  RS_ASSERT(index < arr->_arrval.len);
  return arr->_arrval.vals[index];
}

static inline uint32_t RSValue_ArrayLen(const RSValue *arr) {
  return arr ? arr->_arrval.len : 0;
}

/**
 * Formats the passed numeric RSValue as a string.
 * The passed RSValue must be of type RSValue_Number.
 */
static size_t RSValue_NumToString(const RSValue *v, char *buf) {
  RS_ASSERT(v->_t == RSValue_Number);
  double dd = v->_numval;
  long long ll = dd;
  if (ll == dd) {
    return sprintf(buf, "%lld", ll);
  } else {
    return sprintf(buf, "%.12g", dd);
  }
}

// Formats the parsed expression object into a string, obfuscating the values if needed based on the
// obfuscate boolean The returned string must be freed by the caller using sdsfree
sds RSValue_DumpSds(const RSValue *v, sds s, bool obfuscate);

/**
 * This macro decrements the refcount of dst (as a pointer), and increments the
 * refcount of src, and finally assigns src to the variable dst
 */
#define RSVALUE_REPLACE(dstpp, src) \
  do {                              \
    RSValue_Decref(*dstpp);         \
    RSValue_IncrRef(src);           \
    *(dstpp) = src;                 \
  } while (0);

/**
 * This macro does three things:
 * (1) It checks if the value v is NULL, if it isn't then it:
 * (2) Decrements it
 * (3) Sets the variable to NULL, as it no longer owns it.
 */
#define RSVALUE_CLEARVAR(v) \
  if (v) {                  \
    RSValue_Decref(v);      \
  }

#ifdef __cplusplus
}
#endif
#endif
