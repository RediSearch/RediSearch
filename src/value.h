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
  // An array of values, that can be of any type
  RSValue_Array = 6,
  // Reference to another value
  RSValue_Reference = 8,

} RSValueType;

/* Enumerate sub-types of C strings, basically the free() strategy */
typedef enum {
  RSString_Const = 0x00,
  RSString_Malloc = 0x01,
  RSString_RMAlloc = 0x02,
  RSString_SDS = 0x03,
  // Volatile strings are strings that need to be copied when retained
  RSString_Volatile = 0x04,
} RSStringType;

#define RSVALUE_STATIC ((RSValue){.allocated = 0})

#pragma pack(4)
// Variant value union
typedef struct rsvalue {

  union {
    // numeric value
    double numval;

    // string value
    struct {
      char *str;
      uint32_t len : 29;
      // sub type for string
      RSStringType stype : 3;
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
  RSValueType t : 8;
  int refcount : 23;
  uint8_t allocated : 1;
} RSValue;
#pragma pack()

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
void RSValue_Free(RSValue *v);

static inline RSValue *RSValue_IncrRef(RSValue *v) {
  ++v->refcount;
  return v;
}

RSValue *RS_NewValue(RSValueType t);

static RSValue RS_StaticValue(RSValueType t) {
  RSValue v = (RSValue){
      .t = t,
      .refcount = 1,
      .allocated = 0,
  };
  return v;
}

void RSValue_SetNumber(RSValue *v, double n);
void RSValue_SetString(RSValue *v, char *str, size_t len);
void RSValue_SetSDS(RSValue *v, sds s);
void RSValue_SetConstString(RSValue *v, const char *str, size_t len);

static inline void RSValue_MakeReference(RSValue *dst, RSValue *src) {

  *dst = (RSValue){
      .t = RSValue_Reference,
      .refcount = 1,
      .allocated = 0,
      .ref = RSValue_IncrRef(src),
  };
}
/* Return the value itself or its referred value */
static inline RSValue *RSValue_Dereference(RSValue *v) {
  return v->t == RSValue_Reference ? v->ref : v;
}

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
RSValue *RS_StringVal(char *str, uint32_t len);

RSValue *RS_StringValFmt(const char *fmt, ...);

/* Same as RS_StringVal but with explicit string type */
RSValue *RS_StringValT(char *str, uint32_t len, RSStringType t);

RSValue *RS_ConstStringVal(char *str, uint32_t len);

/* Wrap a string with length into a value object, assuming the string is a null terminated C
 * string
 */
RSValue *RS_StringValC(char *str);

RSValue *RS_ConstStringValC(char *str);

/* Wrap a redis string value */
RSValue *RS_RedisStringVal(RedisModuleString *str);

const char *RSValue_TypeName(RSValueType t);

// Returns true if the value contains a string
static inline int RSValue_IsString(const RSValue *value) {
  return value && (value->t == RSValue_String || value->t == RSValue_RedisString);
}

/* Return 1 if the value is NULL, RSValue_Null or a reference to RSValue_Null */
static inline int RSValue_IsNull(const RSValue *value) {
  if (!value || value->t == RSValue_Null) return 1;
  if (value->t == RSValue_Reference) return RSValue_IsNull(value->ref);
  return 0;
}

/* Make sure a value can be long lived. If the underlying value is a volatile string that might go
 * away in the next iteration, we copy it at that stage. This doesn't change the ref count.
 * A volatile string usually comes from a block allocator and is not freed in RSVAlue_Free, so just
 * discarding the pointer here is "safe" */
static inline RSValue *RSValue_MakePersistent(RSValue *v) {
  if (v->t == RSValue_String && v->strval.stype == RSString_Volatile) {
    v->strval.str = strndup(v->strval.str, v->strval.len);
    v->strval.stype = RSString_Malloc;
  } else if (v->t == RSValue_Array) {
    for (size_t i = 0; i < v->arrval.len; i++) {
      RSValue_MakePersistent(v->arrval.vals[i]);
    }
  }
  return v;
}

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
void RSValue_ToString(RSValue *dst, RSValue *v);

/* New value from string, trying to parse it as a number */
RSValue *RSValue_ParseNumber(const char *p, size_t l);

/* Convert a value to a number, either returning the actual numeric values or by parsing a string
into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
not. If possible, we put the actual value into teh double pointer */
int RSValue_ToNumber(RSValue *v, double *d);

/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
static inline uint64_t RSValue_Hash(RSValue *v, uint64_t hval) {
  if (!v) return 0;
  switch (v->t) {
    case RSValue_Reference:
      return RSValue_Hash(v->ref, hval);
    case RSValue_String:

      return fnv_64a_buf(v->strval.str, v->strval.len, hval);
    case RSValue_Number:
      return fnv_64a_buf(&v->numval, sizeof(double), hval);

    case RSValue_RedisString: {
      size_t sz;
      const char *c = RedisModule_StringPtrLen(v->rstrval, &sz);
      return fnv_64a_buf((void *)c, sz, hval);
    }
    case RSValue_Null:
      return 1337;  // TODO: fix...

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
const char *RSValue_StringPtrLen(RSValue *value, size_t *lenp);

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
const char *RSValue_ConvertStringPtrLen(RSValue *value, size_t *lenp, char *buf, size_t buflen);

/* Wrap a number into a value object */
RSValue *RS_NumVal(double n);
/* Wrap an array of RSValue objects into an RSValue array object */
RSValue *RS_ArrVal(RSValue **vals, uint32_t len);

RSValue *RS_VStringArray(uint32_t sz, ...);

/* Wrap an array of NULL terminated C strings into an RSValue array */
RSValue *RS_StringArray(char **strs, uint32_t sz);

/* Initialize all strings in the array with a given string type */
RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st);

/* Create a new NULL RSValue */
RSValue *RS_NullVal();

RSValue *RS_NewValueFromCmdArg(CmdArg *arg);

int RSValue_Cmp(RSValue *v1, RSValue *v2);

static inline RSValue *RSValue_ArrayItem(RSValue *arr, uint32_t index) {
  return arr->arrval.vals[index];
}

static inline uint32_t RSValue_ArrayLen(RSValue *arr) {
  return arr ? arr->arrval.len : 0;
}

/* Based on the value type, serialize the value into redis client response */
int RSValue_SendReply(RedisModuleCtx *ctx, RSValue *v);

void RSValue_Print(RSValue *v);

// Convert a property key from '@property_name' format as used in queries to 'property_name'
#define RSKEY(s) ((s && *s == '@') ? s + 1 : s)

#define RSKEY_NOTFOUND -1
#define RSKEY_NOCACHE -2
#define RSKEY_UNCACHED -3
#define RSKEY_ISVALIDIDX(i) (i >= 0)
typedef struct {
  const char *key;
  int fieldIdx;
  int sortableIdx;
} RSKey;

#define RS_KEY(s)                    \
  ((RSKey){                          \
      .key = s,                      \
      .fieldIdx = RSKEY_UNCACHED,    \
      .sortableIdx = RSKEY_UNCACHED, \
  })

#define RS_KEY_STRDUP(s) RS_KEY(strdup(s))

typedef struct {
  uint16_t len;
  int keysAllocated : 1;
  RSKey keys[];
} RSMultiKey;

RSMultiKey *RS_NewMultiKey(uint16_t len);

RSMultiKey *RS_NewMultiKeyVariadic(int len, ...);
/* Create a multi-key from a string array.
 *  If allowCaching is 1, the keys are set to allow for index caching.
 *  If duplicateStrings is 1, the key strings are copied
 */
RSMultiKey *RS_NewMultiKeyFromArgs(CmdArray *arr, int allowCaching, int duplicateStrings);

RSMultiKey *RSMultiKey_Copy(RSMultiKey *k, int copyKeys);

void RSMultiKey_Free(RSMultiKey *k);

/* A result field is a key/value pair of variant type, used inside a value map */
typedef struct {
  const char *key;
  RSValue *val;
} RSField;

/* Create new KV field */
RSField RS_NewField(const char *k, RSValue *val);

/* A "map" of fields for results and documents. */
typedef struct {
  uint16_t len;
  uint16_t cap;
  RSField fields[];
} RSFieldMap;

/* Create a new field map with a given initial capacity */
RSFieldMap *RS_NewFieldMap(uint16_t cap);

#define FIELDMAP_FIELD(m, i) (m)->fields[i]

/* Get an item by index */
static inline RSValue *RSFieldMap_Item(RSFieldMap *m, uint16_t pos) {
  return RSValue_Dereference(m->fields[pos].val);
}

/* Find an item by name. */
static inline RSValue *RSFieldMap_Get(RSFieldMap *m, const char *k) {
  k = RSKEY(k);
  for (uint16_t i = 0; i < m->len; i++) {
    if (!strcmp(FIELDMAP_FIELD(m, i).key, k)) {
      return RSValue_Dereference(FIELDMAP_FIELD(m, i).val);
    }
  }
  return NULL;
}

RSValue *RSFieldMap_GetByKey(RSFieldMap *m, RSKey *k);

/* Add a filed to the map WITHOUT checking for duplications */
void RSFieldMap_Add(RSFieldMap **m, const char *key, RSValue *val);
/* Set a value in the map for a given key, checking for duplicates and replacing the existing
 * value if needed, and appending a new one if needed */
void RSFieldMap_Set(RSFieldMap **m, const char *key, RSValue *val);

void RSFieldMap_SetNumber(RSFieldMap **m, const char *key, double d);

void RSFieldMap_Reset(RSFieldMap *m);
/* Free the field map. If freeKeys is set to 1 we also free the keys */
void RSFieldMap_Free(RSFieldMap *m, int freeKeys);

void RSFieldMap_Print(RSFieldMap *m);

/* Read an array of RSVAlues into an array of strings or numbers based on fmt. Return 1 on success.
 * fmt:
 *  - s: will be parsed as a string
 *  - l: Will be parsed as a long integer
 *  - d: Will be parsed as a double
 *  - !: will be skipped
 *  - ?: means evrything after is optional
 */
int RSValue_ArrayAssign(RSValue **args, int argc, const char *fmt, ...);
#endif