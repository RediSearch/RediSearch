#pragma once

#include <string.h>
#include <sys/param.h>
#include <stdarg.h>
#include <errno.h>
#include <math.h>
#include <assert.h>

#include "rmutil/sds.h"
#include "redisearch.h"
#include "util/fnv.h"
#include "rmutil/args.h"
#include "rmalloc.h"
#include "query_error.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Variant Values - will be used in documents as well

// Enumeration of possible value types
enum RSValueType {
  RSValue_Undef = 0, // NULL/Empty

  RSValue_Number = 1,
  RSValue_String = 3,
  RSValue_Null = 4,
  RSValue_RedisString = 5,
  // An array of values, that can be of any type
  RSValue_Array = 6,
  // A redis string, but we own a refcount to it; tied to RSDummy
  RSValue_OwnRstring = 7,

  RSValue_Reference = 8, // Reference to another value
};

//---------------------------------------------------------------------------------------------

// Enumerate sub-types of C strings, basically the free() strategy
enum RSStringType {
  RSString_Const = 0x00,
  RSString_Malloc = 0x01,
  RSString_RMAlloc = 0x02,
  RSString_SDS = 0x03,
  // Volatile strings are strings that need to be copied when retained
  RSString_Volatile = 0x04,
};

//---------------------------------------------------------------------------------------------

// Don't increment the refcount of the children
#define RSVAL_ARRAY_NOINCREF 0x01
// Alloc the underlying array. Absence means the previous array is used
#define RSVAL_ARRAY_ALLOC 0x02
// Don't free the underlying list when the array is freed
#define RSVAL_ARRAY_STATIC 0x04

// Accesses the array element at a given position as an l-value
#define RSVALUE_ARRELEM(vv, pos) ((vv)->arrval.vals[pos])

// Accesses the array length as an lvalue
#define RSVALUE_ARRLEN(vv) ((vv)->arrval.len)

//---------------------------------------------------------------------------------------------

#pragma pack(4)

// Variant value union

struct RSValue : public Object {
  union {
    double numval; // numeric value

    int64_t intval;

    // string value
    struct {
      char *str;
      uint32_t len : 29;
      RSStringType stype : 3; // sub type for string
    } strval;

    // array value
    struct {
      struct RSValue **vals;
      uint32_t len : 31;

      // Whether the storage space of the array itself should be freed
      uint8_t staticarray : 1;
    } arrval;

    struct RedisModuleString *rstrval; // redis string value
    struct RSValue *ref; // reference to another value
  };

  RSValue(RSValueType t = RSValue_Undef, uint32_t refcount = 0, uint8_t allocated = 0) :
    ref(nullptr), t(t), refcount(refcount), allocated(allocated) {
  }

  ~RSValue();

  RSValueType t : 8;
  uint32_t refcount : 23;
  uint8_t allocated : 1;

  static RSValue *NewArray(RSValue **vals, size_t n, int options);

  void Clear();
  RSValue *IncrRef();
  void Decref();

  RSValue *Dereference();
  const RSValue *Dereference() const;

  bool BoolTest() const;
  bool IsNull() const;
  bool IsString() const;

  void SetNumber(double n);
  void SetString(char *str, size_t len);
  void SetSDS(sds s);
  void SetConstString(const char *str, size_t len);

  void Print() const;

  void ToString(RSValue *dst);
  const char *StringPtrLen(size_t *lenp = nullptr) const;
  const char *ConvertStringPtrLen(size_t *lenp, char *buf, size_t buflen) const;

  bool ToNumber(double *d) const;

  const char *TypeName(RSValueType t);

  uint64_t Hash(uint64_t hval) const;

  void MakeReference(RSValue *src);
  void MakeOwnReference(RSValue *src);
  void MakeRStringOwner();

  RSValue *MakePersistent();

  RSValue *ArrayItem(uint32_t index) const;
  uint32_t ArrayLen() const;

  int SendReply(RedisModuleCtx *ctx, bool typed) const;

  // Compare 2 values for sorting
  static int Cmp(const RSValue *v1, const RSValue *v2, QueryError *status);
  static int CmpNC(const RSValue *v1, const RSValue *v2);

  // Return true if the two values are equal
  static bool Equal(const RSValue *v1, const RSValue *v2, QueryError *status);
};

#pragma pack()

//---------------------------------------------------------------------------------------------

inline RSValue *RSValue::IncrRef() {
  ++refcount;
  return this;
}

inline void RSValue::Decref() {
  if (!--refcount) {
    delete this;
  }
}

static RSValue RS_StaticValue(RSValueType t) {
#ifdef __cplusplus
  RSValue v(t, 1, 0);
#else
  RSValue v = {
      .t = t,
      .refcount = 1,
      .allocated = 0,
  };
#endif
  return v;
}

inline void RSValue::MakeReference(RSValue *src) {
  if (!src) throw Error("RSvalue is missing");
  Clear();
  t = RSValue_Reference;
  ref = src->IncrRef();
}

inline void RSValue::MakeOwnReference(RSValue *src) {
  MakeReference(src);
  src->Decref();
}

RSValue *RS_StringVal(char *str, uint32_t len);

RSValue *RS_StringValFmt(const char *fmt, ...);

// Same as RS_StringVal but with explicit string type
RSValue *RS_StringValT(char *str, uint32_t len, RSStringType t);

// Wrap a string with length into a value object, assuming the string is a null terminated C string

inline RSValue *RS_StringValC(char *s) {
  return RS_StringVal(s, strlen(s));
}

inline RSValue *RS_ConstStringVal(const char *s, size_t n) {
  return RS_StringValT((char *)s, n, RSString_Const);
}

#define RS_ConstStringValC(s) RS_ConstStringVal(s, strlen(s))

// Wrap a redis string value

RSValue *RS_RedisStringVal(RedisModuleString *str);
RSValue *RS_OwnRedisStringVal(RedisModuleString *str);
RSValue *RS_StealRedisStringVal(RedisModuleString *s);

// Returns true if the value contains a string

inline bool RSValue::IsString() const {
  return t == RSValue_String || t == RSValue_RedisString || t == RSValue_OwnRstring;
}

// Return 1 if the value is NULL, RSValue_Null or a reference to RSValue_Null

inline bool RSValue::IsNull() const {
  if (t == RSValue_Null) return true;
  if (t == RSValue_Reference) return ref->IsNull();
  return false;
}

// Make sure a value can be long lived. If the underlying value is a volatile string that might go
// away in the next iteration, we copy it at that stage. This doesn't change the ref count.
// A volatile string usually comes from a block allocator and is not freed in RSVAlue_Free, so just
// discarding the pointer here is "safe"

inline RSValue *RSValue::MakePersistent() {
  if (t == RSValue_String && strval.stype == RSString_Volatile) {
    strval.str = rm_strndup(strval.str, strval.len);
    strval.stype = RSString_Malloc;
  } else if (t == RSValue_Array) {
    for (size_t i = 0; i < arrval.len; i++) {
      arrval.vals[i]->MakePersistent();
    }
  }
  return this;
}

// Copies a string using the default mechanism. Returns the copied value
RSValue *RS_NewCopiedString(const char *s, size_t dst);

// New value from string, trying to parse it as a number
RSValue *RSValue_ParseNumber(const char *p, size_t l);

#define RSVALUE_NULL_HASH 1337

// Return a 64 hash value of an RSValue. If this is not an incremental hashing, pass 0 as hval */

inline uint64_t RSValue::Hash(uint64_t hval) const {
  switch (t) {
    case RSValue_Reference:
      return ref->Hash(hval);
    case RSValue_String:
      return fnv_64a_buf(strval.str, strval.len, hval);
    case RSValue_Number:
      return fnv_64a_buf(&numval, sizeof(double), hval);
    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t sz;
      const char *c = RedisModule_StringPtrLen(rstrval, &sz);
      return fnv_64a_buf((void *)c, sz, hval);
    }
    case RSValue_Null:
      return 1337;  // TODO: fix...

    case RSValue_Array: {
      for (uint32_t i = 0; i < arrval.len; i++) {
        hval = arrval.vals[i]->Hash(hval);
      }
      return hval;
    }
    case RSValue_Undef:
      return 0;
  }

  return 0;
}

// Wrap a number into a value object
RSValue *RS_NumVal(double n);

RSValue *RS_Int64Val(int64_t ii);

RSValue *RS_VStringArray(uint32_t sz, ...);

// Wrap an array of NULL terminated C strings into an RSValue array
RSValue *RS_StringArray(char **strs, uint32_t sz);

// Initialize all strings in the array with a given string type
RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st);

extern RSValue RS_NULL;

// Create a new NULL RSValue
inline RSValue *RS_NullVal() {
  return &RS_NULL;
}

// "truth testing" for a value. for a number - not zero. For a string/array - not empty.
// null is considered false
inline bool RSValue::BoolTest() const {
  if (IsNull()) return false;

  const RSValue *v = Dereference();
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
      return false;
  }
}

inline RSValue *RSValue::ArrayItem(uint32_t index) const {
  return arrval.vals[index];
}

inline uint32_t RSValue::ArrayLen() const {
  return arrval.len;
}

int RSValue_ArrayAssign(RSValue **args, int argc, const char *fmt, ...);

size_t RSValue_NumToString(double dd, char *buf);

//---------------------------------------------------------------------------------------------

// Static value pointers. These don't ever get decremented

static RSValue RS_StaticNull(RSValue_Null);
static RSValue RS_StaticUndef(RSValue_Undef);

// Maximum number of static/cached numeric values. Integral numbers in this range
// can benefit by having 'static' values assigned to them, eliminating the need
// for dynamic allocation

#define RSVALUE_MAX_STATIC_NUMVALS 256

// This macro decrements the refcount of dst (as a pointer), and increments the
// refcount of src, and finally assigns src to the variable dst

#define RSVALUE_REPLACE(dstpp, src) \
  do {                              \
    (*dstpp)->Decref();             \
    src->IncrRef();                 \
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
    v->Decref();            \
  }

///////////////////////////////////////////////////////////////////////////////////////////////
