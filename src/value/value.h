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
  RSValueType_Undef = 0,

  RSValueType_Number = 1,
  RSValueType_String = 3,
  RSValueType_Null = 4,
  RSValueType_RedisString = 5,
  // An array of values, that can be of any type
  RSValueType_Array = 6,
  // A redis string, but we own a refcount to it; tied to RSDummy
  RSValueType_OwnRstring = 7,
  // Reference to another value
  RSValueType_Reference = 8,
  // Trio value
  RSValueType_Trio = 9,
  // Map value
  RSValueType_Map = 10,

} RSValueType;

/* Enumerate sub-types of C strings, indicates whether or not it should be rm_free'd */
typedef enum {
  RSStringType_Const = 0x00,
  RSStringType_RMAlloc = 0x02,
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

    // trio value
    struct {
      // An array of 3 RSValue *'s
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

///////////////////////////////////////////////////////////////
// Constructors
///////////////////////////////////////////////////////////////

/**
 * Creates a heap-allocated RSValue of the specified type.
 * The returned value must be freed when no longer needed.
 * @param t The type of RSValue to create
 * @return A pointer to a heap-allocated RSValue
 */
RSValue *RSValue_NewWithType(RSValueType t);

/**
 * Creates a stack-allocated undefined RSValue.
 * The returned value is not allocated on the heap and should not be freed.
 * @return A stack-allocated RSValue of type RSValueType_Undef
 */
RSValue RSValue_Undefined();

#ifndef __cplusplus
/**
 * Creates a stack-allocated RSValue of the specified type.
 * The returned value is not heap-allocated and should not be freed.
 * This is a generic constructor for stack-allocated RSValues.
 * @param t The type of RSValue to create
 * @return A stack-allocated RSValue
 */
static RSValue RSValue_WithType(RSValueType t) {
  RSValue v = (RSValue){
      ._t = t,
      ._refcount = 1,
      ._allocated = 0,
  };
  return v;
}
#endif

/**
 * Creates a stack-allocated RSValue containing a number.
 * The returned value is not allocated on the heap and should not be freed.
 * @param n The numeric value to wrap
 * @return A stack-allocated RSValue of type RSValueType_Number
 */
RSValue RSValue_Number(double n);

/**
 * Creates a stack-allocated RSValue containing a malloc'd string.
 * The returned value itself is not heap-allocated, but takes ownership of the string.
 * @param str The malloc'd string to wrap (ownership is transferred)
 * @param len The length of the string
 * @return A stack-allocated RSValue of type RSValue_String with RSString_Malloc subtype
 */
RSValue RSValue_String(char *str, uint32_t len);

/**
 * Creates a heap-allocated RSValue wrapping a string.
 * Doesn't duplicate the string. Use strdup if the value needs to be detached.
 * @param str The string to wrap (ownership is transferred)
 * @param len The length of the string
 * @return A pointer to a heap-allocated RSValue
 */
RSValue *RSValue_NewString(char *str, uint32_t len);

/**
 * Creates a heap-allocated RSValue wrapping a null-terminated C string.
 * @param s The null-terminated string to wrap (ownership is transferred)
 * @return A pointer to a heap-allocated RSValue
 */
static inline RSValue *RSValue_NewCString(char *s) {
  return RSValue_NewString(s, strlen(s));
}
/**
 * Creates a heap-allocated RSValue wrapping a const null-terminated C string.
 * @param str The string to wrap (ownership is transferred)
 * @param len The length of the string
 * @return A pointer to a heap-allocated RSValue wrapping a constant C string
 */
RSValue *RSValue_NewConstString(const char *str, uint32_t len);
/**
 * Like RSValue_NewConstString, but uses strlen to determine
 * the length of the passed null-terminated C string.
 */
static inline RSValue *RSValue_NewConstCString(const char *s) {
  return RSValue_NewConstString(s, strlen(s));
}

/**
 * Creates a heap-allocated RSValue wrapping a RedisModuleString.
 * Does not increment the refcount of the Redis string.
 * The passed Redis string's refcount does not get decremented
 * upon freeing the returned RSValue.
 * @param str The RedisModuleString to wrap
 * @return A pointer to a heap-allocated RSValue
 */
RSValue *RSValue_NewBorrowedRedisString(RedisModuleString *str);

/**
 * Creates a heap-allocated RSValue which increments and owns a reference to the Redis string.
 * The RSValue will decrement the refcount when freed.
 * @param str The RedisModuleString to wrap (refcount is incremented)
 * @return A pointer to a heap-allocated RSValue
 */
RSValue *RSValue_NewOwnedRedisString(RedisModuleString *str);

/**
 * Creates a heap-allocated RSValue which steals a reference to the Redis string.
 * The caller's reference is transferred to the RSValue.
 * @param s The RedisModuleString to wrap (ownership is transferred)
 * @return A pointer to a heap-allocated RSValue
 */
RSValue *RSValue_NewStolenRedisString(RedisModuleString *s);

/**
 * Returns a pointer to a statically allocated NULL RSValue.
 * This is a singleton - the same pointer is always returned.
 * DO NOT free or modify this value.
 * @return A pointer to a static RSValue of type RSValueType_Null
 */
RSValue *RSValue_NullStatic();

/**
 * Creates a heap-allocated RSValue with a copied string.
 * The string is duplicated using rm_malloc.
 * @param s The string to copy
 * @param dst The length of the string to copy
 * @return A pointer to a heap-allocated RSValue owning the copied string
 */
RSValue *RSValue_NewCopiedString(const char *s, size_t dst);

/**
 * Creates a heap-allocated RSValue by parsing a string as a number.
 * Returns NULL if the string cannot be parsed as a valid number.
 * @param p The string to parse
 * @param l The length of the string
 * @return A pointer to a heap-allocated RSValue or NULL on parse failure
 */
RSValue *RSValue_NewParsedNumber(const char *p, size_t l);

/**
 * Creates a heap-allocated RSValue containing a number.
 * @param n The numeric value to wrap
 * @return A pointer to a heap-allocated RSValue of type RSValueType_Number
 */
RSValue *RSValue_NewNumber(double n);

/**
 * Creates a heap-allocated RSValue containing a number from an int64.
 * @param ii The int64 value to convert and wrap
 * @return A pointer to a heap-allocated RSValue of type RSValueType_Number
 */
RSValue *RSValue_NewNumberFromInt64(int64_t ii);

/**
 * Creates a heap-allocated RSValue array from existing values.
 * Takes ownership of the values (values will be freed when array is freed).
 * @param vals The values array to use for the array (ownership is transferred)
 * @param len Number of values
 * @return A pointer to a heap-allocated RSValue of type RSValueType_Array
 */
RSValue *RSValue_NewArray(RSValue **vals, uint32_t len);

/**
 * Creates an RSValueMap structure with heap-allocated space for entries.
 * The map entries are uninitialized and must be set using RSValueMap_SetEntry.
 * Note: This returns a struct by value, not a pointer, but the struct
 * points to the heap allocation.
 * @param len The number of entries to allocate space for
 * @return An RSValueMap struct with heap-allocated but uninitialized entries
 */
RSValueMap RSValueMap_AllocUninit(uint32_t len);

/**
 * Creates a heap-allocated RSValue of type RSValue_Map from an RSValueMap.
 * Takes ownership of the map structure and all its entries.
 * @param map The RSValueMap to wrap (ownership is transferred)
 * @return A pointer to a heap-allocated RSValue of type RSValueType_Map
 */
RSValue *RSValue_NewMap(RSValueMap map);

/**
 * Creates a heap-allocated RSValue array from variadic string arguments.
 * @param sz Number of strings to expect
 * @param ... Variadic list of char* strings
 * @return A pointer to a heap-allocated RSValue array
 */
RSValue *RSValue_NewVStringArray(uint32_t sz, ...);

/**
 * Creates a heap-allocated RSValue array from NULL terminated C strings.
 * @param strs Array of string pointers
 * @param sz Number of strings in the array
 * @return A pointer to a heap-allocated RSValue array
 */
RSValue *RSValue_NewStringArray(char **strs, uint32_t sz);

/**
 * Creates a heap-allocated RSValue array with strings of type `RSStringType_Const`.
 * @param strs Array of string pointers
 * @param sz Number of strings in the array
 * @return A pointer to a heap-allocated RSValue array
 */
RSValue *RSValue_NewConstStringArray(char **strs, uint32_t szx);

/**
 * Creates a heap-allocated RSValue Trio from three RSValues.
 * Takes ownership of all three values.
 * @param val The left value (ownership is transferred)
 * @param otherval The middle value (ownership is transferred)
 * @param other2val The right value (ownership is transferred)
 * @return A pointer to a heap-allocated RSValue of type RSValueType_Trio
 */
RSValue *RSValue_NewTrio(RSValue *val, RSValue *otherval, RSValue *other2val);

///////////////////////////////////////////////////////////////
// Getters and Setters (grouped by field)
///////////////////////////////////////////////////////////////

// Type getters
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
 * @return true if the value is of type RSValueType_Number, false otherwise
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
 * Check whether the RSValue is of type RSValue_Trio.
 * @param v The value to check
 * @return true if the value is a Trio, false otherwise
 */
bool RSValue_IsTrio(const RSValue *v);

// Returns true if the value contains any type of string
static inline int RSValue_IsAnyString(const RSValue *value) {
  return value && (value->_t == RSValueType_String || value->_t == RSValueType_RedisString ||
                   value->_t == RSValueType_OwnRstring);
}

/* Return 1 if the value is NULL, RSValue_Null or a reference to RSValue_Null */
int RSValue_IsNull(const RSValue *value);

// Number getters/setters
void RSValue_SetNumber(RSValue *v, double n);

/**
 * Get the numeric value from an RSValue.
 * The value must be of type RSValueType_Number.
 * @param v The value to extract the number from
 * @return The double value stored in the RSValue
 */
double RSValue_Number_Get(const RSValue *v);

/**
 * Convert an RSValue to a number type in-place.
 * This clears the existing value and sets it to RSValueType_Number with the given value.
 * @param v The value to modify
 * @param n The numeric value to set
 */
void RSValue_IntoNumber(RSValue *v, double n);

// String getters/setters
void RSValue_SetString(RSValue *v, char *str, size_t len);
void RSValue_SetConstString(RSValue *v, const char *str, size_t len);

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
 * Gets the string pointer and length from the value,
 * dereferencing in case `value` is a (chain of) RSValue
 * references. Works for all RSValue string types.
 *
 * If `value` if of type `RSValue_String`, does the same as
 * `RSValue_String_GetPtr()`
 */
const char *RSValue_StringPtrLen(const RSValue *value, size_t *lenp);

// Array getters/setters
static inline RSValue *RSValue_ArrayItem(const RSValue *arr, uint32_t index) {
  RS_ASSERT(arr && arr->_t == RSValueType_Array);
  RS_ASSERT(index < arr->_arrval.len);
  return arr->_arrval.vals[index];
}

static inline uint32_t RSValue_ArrayLen(const RSValue *arr) {
  return arr ? arr->_arrval.len : 0;
}

/** Accesses the array element at a given position as an l-value */
#define RSVALUE_ARRELEM(vv, pos) ((vv)->_arrval.vals[pos])
/** Accesses the array length as an lvalue */
#define RSVALUE_ARRLEN(vv) ((vv)->_arrval.len)

// Map getters/setters
/**
 * Get the number of key-value pairs in a map RSValue.
 * @param v The map value
 * @return The number of pairs in the map
 */
uint32_t RSValue_Map_Len(const RSValue *v);

/**
 * Get a key-value pair from a map RSValue at a specific index.
 * @param map The map value
 * @param i The index of the pair to retrieve
 * @param key Output parameter for the key (can be NULL)
 * @param val Output parameter for the value (can be NULL)
 */
void RSValue_Map_GetEntry(const RSValue *map, uint32_t i, RSValue **key, RSValue **val);

/**
 * Set a key-value pair at a specific index in the map.
 * Takes ownership of both the key and value RSValues.
 * @param map The map to modify
 * @param i The index where to set the entry (must be < map->len)
 * @param key The key RSValue (ownership is transferred to the map)
 * @param value The value RSValue (ownership is transferred to the map)
 */
void RSValueMap_SetEntry(RSValueMap *map, size_t i, RSValue *key, RSValue *value);

// Trio getters
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

// Reference getters/setters
/* Return the value itself or its referred value */
static inline RSValue *RSValue_Dereference(const RSValue *v) {
  for (; v && v->_t == RSValueType_Reference; v = v->_ref);
  return (RSValue *)v;
}

/**
 * Clears the underlying storage of the value, and makes it
 * be a reference to the NULL value
 */
void RSValue_Clear(RSValue *v);

RSValue *RSValue_IncrRef(RSValue *v);
void RSValue_DecrRef(RSValue *v);

#ifndef __cplusplus
static inline void RSValue_MakeReference(RSValue *dst, RSValue *src) {
  RS_LOG_ASSERT(src, "RSvalue is missing");
  RSValue_Clear(dst);
  dst->_t = RSValueType_Reference;
  dst->_ref = RSValue_IncrRef(src);
}

static inline void RSValue_MakeOwnReference(RSValue *dst, RSValue *src) {
  RSValue_MakeReference(dst, src);
  RSValue_DecrRef(src);
}
#endif

/**
 * This function decrements the refcount of dst (as a pointer), and increments the
 * refcount of src, and finally assigns src to the variable dst
 */
static inline void RSValue_Replace(RSValue **destpp, RSValue *src) {
    RSValue_DecrRef(*destpp);
    RSValue_IncrRef(src);
    *(destpp) = src;
}

/**
 * Convert an RSValue to undefined type in-place.
 * This clears the existing value and sets it to RSValue_Undef.
 * @param v The value to modify
 */
void RSValue_IntoUndefined(RSValue *v);

/**
 * Convert an RSValue to null type in-place.
 * This clears the existing value and sets its to RSValueType_Null.
 * @param v The value to modify
 */
void RSValue_IntoNull(RSValue *v);

/**
 * Get the reference count of an RSValue.
 * @param v The value to inspect
 * @return The current reference count
 */
uint16_t RSValue_Refcount(const RSValue *v);

///////////////////////////////////////////////////////////////
// Other Functions (utility, memory management, comparison, etc.)
///////////////////////////////////////////////////////////////

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
void RSValue_Free(RSValue *v);

void RSValue_MakeRStringOwner(RSValue *v);

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
void RSValue_ToString(RSValue *dst, RSValue *v);

/* Convert a value to a number, either returning the actual numeric values or by parsing a string
into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
not. If possible, we put the actual value into the double pointer */
int RSValue_ToNumber(const RSValue *v, double *d);

/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
static inline uint64_t RSValue_Hash(const RSValue *v, uint64_t hval) {
  switch (v->_t) {
    case RSValueType_Reference:
      return RSValue_Hash(v->_ref, hval);
    case RSValueType_String:

      return fnv_64a_buf(v->_strval.str, v->_strval.len, hval);
    case RSValueType_Number:
      return fnv_64a_buf(&v->_numval, sizeof(double), hval);

    case RSValueType_RedisString:
    case RSValueType_OwnRstring: {
      size_t sz;
      const char *c = RedisModule_StringPtrLen(v->_rstrval, &sz);
      return fnv_64a_buf((void *)c, sz, hval);
    }
    case RSValueType_Null:
      return hval + 1;

    case RSValueType_Array: {
      for (uint32_t i = 0; i < v->_arrval.len; i++) {
        hval = RSValue_Hash(v->_arrval.vals[i], hval);
      }
      return hval;
    }

    case RSValueType_Map:
      for (uint32_t i = 0; i < v->_mapval.len; i++) {
        hval = RSValue_Hash(v->_mapval.entries[i].key, hval);
        hval = RSValue_Hash(v->_mapval.entries[i].value, hval);
      }
      return hval;

    case RSValueType_Undef:
      return 0;

    case RSValueType_Trio:
      return RSValue_Hash(RSValue_Trio_GetLeft(v), hval);
  }

  return 0;
}

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
const char *RSValue_ConvertStringPtrLen(const RSValue *value, size_t *lenp, char *buf,
                                        size_t buflen);

/**
 * Helper function to allocate memory before passing it to RSValue_NewArray
 */
static inline RSValue **RSValue_AllocateArray(uint32_t len) {
  return (RSValue **)rm_malloc(len * sizeof(RSValue *));
}

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
    case RSValueType_Array:
      return v->_arrval.len != 0;
    case RSValueType_Number:
      return v->_numval != 0;
    case RSValueType_String:
      return v->_strval.len != 0;
    case RSValueType_RedisString:
    case RSValueType_OwnRstring: {
      size_t l = 0;
      const char *p = RedisModule_StringPtrLen(v->_rstrval, &l);
      return l != 0;
    }
    default:
      return 0;
  }
}

/**
 * Formats the passed numeric RSValue as a string.
 * The passed RSValue must be of type RSValueType_Number.
 */
static size_t RSValue_NumToString(const RSValue *v, char *buf, size_t buflen) {
  RS_ASSERT(v->_t == RSValueType_Number);
  double dd = v->_numval;
  long long ll = dd;
  if (ll == dd) {
    return snprintf(buf, buflen, "%lld", ll);
  } else {
    return snprintf(buf, buflen, "%.12g", dd);
  }
}

// Formats the parsed expression object into a string, obfuscating the values if needed based on the
// obfuscate boolean The returned string must be freed by the caller using sdsfree
sds RSValue_DumpSds(const RSValue *v, sds s, bool obfuscate);

#ifdef __cplusplus
}
#endif
#endif
