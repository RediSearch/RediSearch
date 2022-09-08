#ifndef RS_VALUE_H_
#define RS_VALUE_H_

#include <string.h>
#include <sys/param.h>
#include <stdarg.h>
#include <errno.h>
#include <math.h>

#include "rmutil/sds.h"
#include "redisearch.h"
#include "util/fnv.h"
#include "rmutil/args.h"
#include "rmalloc.h"
#include "query_error.h"
#include "rmutil/rm_assert.h"

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
  // Duo value
  RSValue_Duo = 9,

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

#define RSVALUE_STATIC \
  { .allocated = 0 }

#pragma pack(4)
// Variant value union
typedef struct RSValue {

  union {
    // numeric value
    double numval;

    int64_t intval;

    // string value
    struct {
      char *str;
      uint32_t len : 29;
      // sub type for string
      RSStringType stype : 3;
    } strval;

    // array value
    struct {
      struct RSValue **vals;
      uint32_t len : 30;

      /**
       * Whether the storage space of the array itself
       * should be freed
       */
      uint8_t staticarray : 1;
    } arrval;

    struct {
      /**
       * Duo value
       * 
       * Allows keeping a value, together with an addition value.
       * 
       * For example, keeping a value and, in addition, a different value for serialization, such as a JSON String representation.
       */ 
      
      struct RSValue *val;
      struct RSValue *otherval;
    } duoval;

    // redis string value
    struct RedisModuleString *rstrval;

    // reference to another value
    struct RSValue *ref;
  };
  RSValueType t : 8;
  uint32_t refcount : 23;
  uint8_t allocated : 1;

#ifdef __cplusplus
  RSValue() {
  }
  RSValue(RSValueType t_) : ref(NULL), t(t_), refcount(0), allocated(0) {
  }

#endif
} RSValue;
#pragma pack()

/**
 * Clears the underlying storage of the value, and makes it
 * be a reference to the NULL value
 */
void RSValue_Clear(RSValue *v);

/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
void RSValue_Free(RSValue *v);

static inline RSValue *RSValue_IncrRef(RSValue *v) {
  ++v->refcount;
  return v;
}

#define RSValue_Decref(v) \
  if (!--(v)->refcount) { \
    RSValue_Free(v);      \
  }

RSValue *RS_NewValue(RSValueType t);

#ifndef __cplusplus
static RSValue RS_StaticValue(RSValueType t) {
  RSValue v = (RSValue){
      .t = t,
      .refcount = 1,
      .allocated = 0,
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
  dst->t = RSValue_Reference;
  dst->ref = RSValue_IncrRef(src);
}

static inline void RSValue_MakeOwnReference(RSValue *dst, RSValue *src) {
  RSValue_MakeReference(dst, src);
  RSValue_Decref(src);
}
#endif

/* Return the value itself or its referred value */
static inline RSValue *RSValue_Dereference(const RSValue *v) {
  for (; v && v->t == RSValue_Reference; v = v->ref)
    ;
  return (RSValue *)v;
}

/* Wrap a string with length into a value object. Doesn't duplicate the string. Use strdup if
 * the value needs to be detached */
RSValue *RS_StringVal(char *str, uint32_t len);

RSValue *RS_StringValFmt(const char *fmt, ...);

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

const char *RSValue_TypeName(RSValueType t);

// Returns true if the value contains a string
static inline int RSValue_IsString(const RSValue *value) {
  return value && (value->t == RSValue_String || value->t == RSValue_RedisString ||
                   value->t == RSValue_OwnRstring);
}

/* Create a new NULL RSValue */
RSValue *RS_NullVal();

/* Return 1 if the value is NULL, RSValue_Null or a reference to RSValue_Null */
static inline int RSValue_IsNull(const RSValue *value) {
  if (!value || value == RS_NullVal()) return 1;
  if (value->t == RSValue_Reference) return RSValue_IsNull(value->ref);
  return 0;
}

/* Make sure a value can be long lived. If the underlying value is a volatile string that might go
 * away in the next iteration, we copy it at that stage. This doesn't change the ref count.
 * A volatile string usually comes from a block allocator and is not freed in RSVAlue_Free, so just
 * discarding the pointer here is "safe" */
static inline RSValue *RSValue_MakePersistent(RSValue *v) {
  if (v->t == RSValue_String && v->strval.stype == RSString_Volatile) {
    v->strval.str = rm_strndup(v->strval.str, v->strval.len);
    v->strval.stype = RSString_Malloc;
  } else if (v->t == RSValue_Array) {
    for (size_t i = 0; i < v->arrval.len; i++) {
      RSValue_MakePersistent(v->arrval.vals[i]);
    }
  }
  return v;
}

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
not. If possible, we put the actual value into teh double pointer */
int RSValue_ToNumber(const RSValue *v, double *d);

#define RSVALUE_NULL_HASH 1337

/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
/* Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */
static inline uint64_t RSValue_Hash(const RSValue *v, uint64_t hval) {
  switch (v->t) {
    case RSValue_Reference:
      return RSValue_Hash(v->ref, hval);
    case RSValue_String:

      return fnv_64a_buf(v->strval.str, v->strval.len, hval);
    case RSValue_Number:
      return fnv_64a_buf(&v->numval, sizeof(double), hval);

    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t sz;
      const char *c = RedisModule_StringPtrLen(v->rstrval, &sz);
      return fnv_64a_buf((void *)c, sz, hval);
    }
    case RSValue_Null:
      return hval + 1;

    case RSValue_Array: {
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        hval = RSValue_Hash(v->arrval.vals[i], hval);
      }
      return hval;
    }
    case RSValue_Undef:
      return 0;

    case RSValue_Duo:
      return RSValue_Hash(v->duoval.val, hval);
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

/* Don't increment the refcount of the children */
#define RSVAL_ARRAY_NOINCREF 0x01
/* Alloc the underlying array. Absence means the previous array is used */
#define RSVAL_ARRAY_ALLOC 0x02
/* Don't free the underlying list when the array is freed */
#define RSVAL_ARRAY_STATIC 0x04

/**
 * Create a new array
 * @param vals the values to use for the array. If NULL, the array is allocated
 * as empty, but with enough *capacity* for these values
 * @param options RSVAL_ARRAY_*
 */
RSValue *RSValue_NewArrayEx(RSValue **vals, size_t n, int options);

/** Accesses the array element at a given position as an l-value */
#define RSVALUE_ARRELEM(vv, pos) ((vv)->arrval.vals[pos])
/** Accesses the array length as an lvalue */
#define RSVALUE_ARRLEN(vv) ((vv)->arrval.len)

RSValue *RS_VStringArray(uint32_t sz, ...);

/* Wrap an array of NULL terminated C strings into an RSValue array */
RSValue *RS_StringArray(char **strs, uint32_t sz);

/* Initialize all strings in the array with a given string type */
RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st);

/* Wrap a pair of RSValue into an RSValue Duo */
RSValue *RS_DuoVal(RSValue *val, RSValue *otherval);

/* Compare 2 values for sorting */
int RSValue_Cmp(const RSValue *v1, const RSValue *v2, QueryError *status);

/* Return 1 if the two values are equal */
int RSValue_Equal(const RSValue *v1, const RSValue *v2, QueryError *status);

/* "truth testing" for a value. for a number - not zero. For a string/array - not empty. null is
 * considered false */
static inline int RSValue_BoolTest(const RSValue *v) {
  if (RSValue_IsNull(v)) return 0;

  v = RSValue_Dereference(v);
  switch (v->t) {
    case RSValue_Array:
      return v->arrval.len != 0;
    case RSValue_Number:
      return v->numval != 0;
    case RSValue_String:
      return v->strval.len != 0;
    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t l = 0;
      const char *p = RedisModule_StringPtrLen(v->rstrval, &l);
      return l != 0;
    }
    default:
      return 0;
  }
}

static inline RSValue *RSValue_ArrayItem(const RSValue *arr, uint32_t index) {
  return arr->arrval.vals[index];
}

static inline uint32_t RSValue_ArrayLen(const RSValue *arr) {
  return arr ? arr->arrval.len : 0;
}

/* Based on the value type, serialize the value into redis client response */
int RSValue_SendReply(RedisModuleCtx *ctx, const RSValue *v, int typed);

void RSValue_Print(const RSValue *v);

int RSValue_ArrayAssign(RSValue **args, int argc, const char *fmt, ...);

#ifdef __cplusplus
#define RSVALUE_STATICALLOC_INIT(T) RSValue(T)
#else
#define RSVALUE_STATICALLOC_INIT(T) \
  { .t = T }
#endif

/** Static value pointers. These don't ever get decremented */
static RSValue __attribute__((unused)) RS_StaticNull = RSVALUE_STATICALLOC_INIT(RSValue_Null);
static RSValue __attribute__((unused)) RS_StaticUndef = RSVALUE_STATICALLOC_INIT(RSValue_Undef);

/**
 * Maximum number of static/cached numeric values. Integral numbers in this range
 * can benefit by having 'static' values assigned to them, eliminating the need
 * for dynamic allocation
 */
#define RSVALUE_MAX_STATIC_NUMVALS 256

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
