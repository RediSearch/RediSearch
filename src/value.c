#include "value.h"
#include "module.h"
#include "query_error.h"
#include "util/mempool.h"

#include <assert.h>
#include <pthread.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0

struct mempoolThreadPool {
  mempool_t *values;
  mempool_t *fieldmaps;
};

//---------------------------------------------------------------------------------------------

static void mempoolThreadPoolDtor(void *p) {
  mempoolThreadPool *tp = p;
  if (tp->values) {
    delete (tp->values);
  }
  if (tp->fieldmaps) {
    delete (tp->fieldmaps);
  }
  rm_free(tp);
}

//---------------------------------------------------------------------------------------------

pthread_key_t mempoolKey_g;

//---------------------------------------------------------------------------------------------

static void __attribute__((constructor)) initKey() {
  pthread_key_create(&mempoolKey_g, mempoolThreadPoolDtor);
}

//---------------------------------------------------------------------------------------------

static inline mempoolThreadPool *getPoolInfo() {
  mempoolThreadPool *tp = pthread_getspecific(mempoolKey_g);
  if (tp == nullptr) {
    tp = rm_calloc(1, sizeof(*tp));
    tp->values = new mempool_t(0, 1000, false);
    pthread_setspecific(mempoolKey_g, tp);
  }
  return tp;
}

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////

// Variant Values - will be used in documents as well
size_t RSValue_NumToString(double dd, char *buf) {
  long long ll = dd;
  if (ll == dd) {
    return sprintf(buf, "%lld", ll);
  } else {
    return sprintf(buf, "%.12g", dd);
  }
}

//---------------------------------------------------------------------------------------------

// RSValue::RSValue(RSValueType t) : t(t), refcount(1), allocated(1) {
//   // RSValue *v = mempool_get(getPoolInfo()->values);
// }

//---------------------------------------------------------------------------------------------

// Return the value itself or its referred value

RSValue *RSValue::Dereference() {
  return const_cast<RSValue *>(static_cast<const RSValue*>(this)->Dereference());
}

const RSValue *RSValue::Dereference() const {
  const RSValue *v = this;
  for (; v && v->t == RSValue_Reference; v = v->ref)
    ;
  return v;
}

//---------------------------------------------------------------------------------------------

// Clears the underlying storage of the value, and makes it be a reference to the nullptr value

void RSValue::Clear() {
  switch (t) {
    case RSValue_String:
      // free strings by allocation strategy
      switch (strval.stype) {
        case RSString_Malloc:
          rm_free(strval.str);
          break;
        case RSString_RMAlloc:
          rm_free(strval.str);
          break;
        case RSString_SDS:
          sdsfree(strval.str);
          break;
        case RSString_Const:
        case RSString_Volatile:
          break;
      }
      break;

    case RSValue_Array:
      for (uint32_t i = 0; i < arrval.len; i++) {
        arrval.vals[i]->Decref();
      }
      if (!arrval.staticarray) {
        rm_free(arrval.vals);
      }
      break;

    case RSValue_Reference:
      ref->Decref();
      break;

    case RSValue_OwnRstring:
      RedisModule_FreeString(RSDummyContext, rstrval);
      break;

    case RSValue_Null:
      return;  // prevent changing global RS_NULL to RSValue_Undef

    default:   // no free
      break;
  }

  ref = nullptr;
  t = RSValue_Undef;
}

//---------------------------------------------------------------------------------------------

// Free a value's internal value. It only does anything in the case of a string, and doesn't free
// the actual value object.

RSValue::~RSValue() {
  Clear();
  // if (allocated) {
  //   mempool_release(getPoolInfo()->values, v);
  // }
}

//---------------------------------------------------------------------------------------------

void RSValue::SetNumber(double n) {
  t = RSValue_Number;
  numval = n;
}

//---------------------------------------------------------------------------------------------

void RSValue::SetString(char *str, size_t len) {
  t = RSValue_String;
  strval.len = len;
  strval.str = str;
  strval.stype = RSString_Malloc;
}

//---------------------------------------------------------------------------------------------

RSValue *RS_NewCopiedString(const char *s, size_t n) {
  RSValue *v = new RSValue(RSValue_String);
  char *cp = static_cast<char *>(rm_malloc(n + 1));
  cp[n] = 0;
  memcpy(cp, s, n);
  v->SetString(cp, n);
  return v;
}

//---------------------------------------------------------------------------------------------

void RSValue::SetSDS(sds s) {
  t = RSValue_String;
  strval.len = sdslen(s);
  strval.str = s;
  strval.stype = RSString_SDS;
}

//---------------------------------------------------------------------------------------------

void RSValue::SetConstString(const char *str, size_t len) {
  t = RSValue_String;
  strval.len = len;
  strval.str = (char *)str;
  strval.stype = RSString_Const;
}

//---------------------------------------------------------------------------------------------

// Wrap a string with length into a value object. Doesn't duplicate the string.
// Use strdup if the value needs to be detached.

RSValue *RS_StringVal(char *str, uint32_t len) {
  if (len > (UINT32_MAX >> 4)) throw Error("string length exceeds limit");
  RSValue *v = new RSValue(RSValue_String);
  v->strval.str = str;
  v->strval.len = len;
  v->strval.stype = RSString_Malloc;
  return v;
}

//---------------------------------------------------------------------------------------------

// Same as RS_StringVal but with explicit string type

RSValue *RS_StringValT(char *str, uint32_t len, RSStringType t) {
  RSValue *v = new RSValue(RSValue_String);
  v->strval.str = str;
  v->strval.len = len;
  v->strval.stype = t;
  return v;
}

//---------------------------------------------------------------------------------------------

RSValue *RS_StringValFmt(const char *fmt, ...) {
  char *buf;
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&buf, fmt, ap);
  va_end(ap);
  return RS_StringVal(buf, strlen(buf));
}

//---------------------------------------------------------------------------------------------

// Wrap a redis string value

RSValue *RS_RedisStringVal(RedisModuleString *str) {
  RSValue *v = new RSValue(RSValue_RedisString, 1, 1);
  v->rstrval = str;
  return v;
}

//---------------------------------------------------------------------------------------------

// Create a new value object which increments and owns a reference to the string

RSValue *RS_OwnRedisStringVal(RedisModuleString *str) {
  RSValue *r = RS_RedisStringVal(str);
  r->MakeRStringOwner();
  return r;
}

//---------------------------------------------------------------------------------------------

// Create a new value object which steals a reference to the string

RSValue *RS_StealRedisStringVal(RedisModuleString *str) {
  RSValue *ret = RS_RedisStringVal(str);
  ret->rstrval = str;
  ret->t = RSValue_OwnRstring;
  return ret;
}

//---------------------------------------------------------------------------------------------

void RSValue::MakeRStringOwner() {
  if (t != RSValue_RedisString) throw Error("RSvalue type should be string");
  t = RSValue_OwnRstring;
  RedisModule_RetainString(RSDummyContext, rstrval);
}

//---------------------------------------------------------------------------------------------

// Convert a value to a string value. If the value is already a string value it gets
// shallow-copied (no string buffer gets copied)

void RSValue::ToString(RSValue *dst) {
  switch (t) {
    case RSValue_String:
      dst->MakeReference(this);
      break;

    case RSValue_RedisString:
    case RSValue_OwnRstring: {
      size_t sz;
      const char *str = RedisModule_StringPtrLen(rstrval, &sz); //@@ to guarentee on rstrval lifetime
      dst->SetConstString(str, sz);
      break;
    }

    case RSValue_Number: {
      char tmpbuf[128] = {0};
      RSValue_NumToString(numval, tmpbuf);
      char *buf = rm_strdup(tmpbuf);
      dst->SetString(buf, strlen(buf));
      break;
    }

    case RSValue_Reference:
      return dst->ToString(ref);

    case RSValue_Null:
    default:
      return dst->SetConstString("", 0);
  }
}

//---------------------------------------------------------------------------------------------

RSValue *RSValue_ParseNumber(const char *p, size_t l) {
  char *e;
  errno = 0;
  double d = strtod(p, &e);
  if ((errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL)) || (errno != 0 && d == 0) ||
      *e != '\0') {
    return nullptr;
  }
  return RS_NumVal(d);
}

//---------------------------------------------------------------------------------------------

// Convert a value to a number, either returning the actual numeric values or by parsing a string
// into a number.
// Return 1 if the value is a number or a numeric string and can be converted, or 0 if not.
// If possible, we put the actual value into the double pointer

bool RSValue::ToNumber(double *d) const {
  if (IsNull()) return false;
  const RSValue *v = Dereference();

  const char *p = nullptr;
  size_t l = 0;
  switch (v->t) {
    // for numerics - just set the value and return
    case RSValue_Number:
      *d = v->numval;
      return true;

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

    case RSValue_Null:
    case RSValue_Array:
    case RSValue_Undef:
    default:
      return false;
  }

  // If we have a string - try to parse it
  if (p) {
    char *e;
    errno = 0;
    *d = strtod(p, &e);
    if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
        *e != '\0') {
      return false;
    }
    return true;
  }
  return false;
}

//---------------------------------------------------------------------------------------------

/**
 * Returns the value as a simple opaque buffer
inline const void *RSValue_ToBuffer(RSValue *value, size_t *outlen) {
  value = value->Dereference();

  switch (value->t) {
    case RSValue_Number:
      *outlen = sizeof(value->numval);
      return &value->numval;
    case RSValue_String:
      *outlen = value->strval.len;
      return value->strval.str;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      return RedisModule_StringPtrLen(value->rstrval, outlen);
    case RSValue_Array:
    case RSValue_Null:
    default:
      *outlen = 0;
      return "";
  }
}
 */

//---------------------------------------------------------------------------------------------

// Gets the string pointer and length from the value
const char *RSValue::StringPtrLen(size_t *lenp) const {
  auto value = Dereference();

  switch (value->t) {
    case RSValue_String:
      if (lenp) {
        *lenp = value->strval.len;
      }
      return value->strval.str;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      return RedisModule_StringPtrLen(value->rstrval, lenp);
    default:
      return nullptr;
  }
}

//---------------------------------------------------------------------------------------------

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns nullptr if buf is required, but is too small

const char *RSValue::ConvertStringPtrLen(size_t *lenp, char *buf, size_t buflen) const {
  auto value = Dereference();

  if (value->IsString()) {
    return value->StringPtrLen(lenp);
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

//---------------------------------------------------------------------------------------------

/* Wrap a number into a value object */
RSValue *RS_NumVal(double n) {
  RSValue *v = new RSValue(RSValue_Number);
  v->numval = n;
  return v;
}

//---------------------------------------------------------------------------------------------

RSValue *RS_Int64Val(int64_t dd) {
  RSValue *v = new RSValue(RSValue_Number);
  v->numval = dd;
  return v;
}

//---------------------------------------------------------------------------------------------

// Create a new array
// @param vals the values to use for the array. If nullptr, the array is allocated
// as empty, but with enough *capacity* for these values
// @param options RSVAL_ARRAY_*

RSValue *RSValue::NewArray(RSValue **vals, size_t n, int options) {
  RSValue *arr = new RSValue(RSValue_Array);
  RSValue **list;
  if (options & RSVAL_ARRAY_ALLOC) {
    list = vals;
  } else {
    list = static_cast<RSValue **>(rm_malloc(n * sizeof *list));
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
        v->IncrRef();
      }
    }
  }

  return arr;
}

//---------------------------------------------------------------------------------------------

RSValue *RS_VStringArray(uint32_t sz, ...) {
  RSValue **arr = static_cast<RSValue **>(rm_calloc(sz, sizeof *arr));
  va_list ap;
  va_start(ap, sz);
  for (uint32_t i = 0; i < sz; i++) {
    char *p = va_arg(ap, char *);
    arr[i] = RS_StringValC(p);
  }
  va_end(ap);
  return RSValue::NewArray(arr, sz, RSVAL_ARRAY_NOINCREF | RSVAL_ARRAY_ALLOC);
}

//---------------------------------------------------------------------------------------------

// Wrap an array of nul-terminated C strings into an RSValue array
RSValue *RS_StringArray(char **strs, uint32_t sz) {
  RSValue **arr = static_cast<RSValue **>(rm_calloc(sz, sizeof *arr));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValC(strs[i]);
  }
  return RSValue::NewArray(arr, sz, RSVAL_ARRAY_NOINCREF | RSVAL_ARRAY_ALLOC);
}

//---------------------------------------------------------------------------------------------

RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st) {
  RSValue **arr = static_cast<RSValue **>(rm_calloc(sz, sizeof *arr));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValT(strs[i], strlen(strs[i]), st);
  }
  return RSValue::NewArray(arr, sz, RSVAL_ARRAY_NOINCREF | RSVAL_ARRAY_ALLOC);
}

//---------------------------------------------------------------------------------------------

RSValue RS_NULL {RSValue_Null, 1, 0};

// Create a new NULL RSValue
/*
inline RSValue *RS_NullVal() {
  return &RS_NULL;
}
*/

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

static inline int cmp_numbers(const RSValue *v1, const RSValue *v2) {
  return v1->numval > v2->numval ? 1 : (v1->numval < v2->numval ? -1 : 0);
}

//---------------------------------------------------------------------------------------------

static inline bool convert_to_number(const RSValue *v, RSValue *vn, QueryError *qerr) {
  double d;
  if (!v->ToNumber(&d)) {
    if (!qerr) return false;

    const char *s = v->StringPtrLen(nullptr);
    qerr->SetErrorFmt(QUERY_ENOTNUMERIC, "Error converting string '%s' to number", s);
    return false;
  }

  vn->SetNumber(d);
  return true;
}

//---------------------------------------------------------------------------------------------

int RSValue::CmpNC(const RSValue *v1, const RSValue *v2) {
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
    case RSValue_Null:
      return 0;
    case RSValue_Array:  // can't compare arrays ATM
    default:
      return 0;
  }
}

//---------------------------------------------------------------------------------------------

int RSValue::Cmp(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  if (!v1 || !v2) throw Error("missing RSvalue");
  v1 = v1->Dereference();
  v2 = v2->Dereference();

  if (v1->t == v2->t) {
    return CmpNC(v1, v2);
  }

  // if one of the values is null, the other wins
  if (v1->t == RSValue_Null) {
    return -1;
  } else if (v2->t == RSValue_Null) {
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
  const char *s1 = v1->ConvertStringPtrLen(&l1, buf1, sizeof(buf1));
  const char *s2 = v2->ConvertStringPtrLen(&l2, buf2, sizeof(buf2));
  return cmp_strings(s1, s2, l1, l2);
}

//---------------------------------------------------------------------------------------------

bool RSValue::Equal(const RSValue *v1, const RSValue *v2, QueryError *qerr) {
  if (!v1 || !v2) throw Error("missing RSvalue");
  v1 = v1->Dereference();
  v2 = v2->Dereference();

  if (v1->t == v2->t) {
    return CmpNC(v1, v2) == 0;
  }

  if (v1->t == RSValue_Null || v2->t == RSValue_Null) {
    return false;
  }

  // if either of the arguments is a number, convert the other one to a number
  RSValue vn;
  if (v1->t == RSValue_Number) {
    if (!convert_to_number(v2, &vn, nullptr)) return 0;
    return cmp_numbers(v1, &vn) == 0;
  } else if (v2->t == RSValue_Number) {
    if (!convert_to_number(v1, &vn, nullptr)) return 0;
    return cmp_numbers(&vn, v2) == 0;
  }

  // cast to strings and compare as strings
  char buf1[100], buf2[100];

  size_t l1, l2;
  const char *s1 = v1->ConvertStringPtrLen(&l1, buf1, sizeof(buf1));
  const char *s2 = v2->ConvertStringPtrLen(&l2, buf2, sizeof(buf2));
  return cmp_strings(s1, s2, l1, l2) == 0;
}

//---------------------------------------------------------------------------------------------

// Based on the value type, serialize the value into redis client response

int RSValue::SendReply(RedisModuleCtx *ctx, bool isTyped) const {
  const RSValue *v = Dereference();

  switch (v->t) {
    case RSValue_String:
      return RedisModule_ReplyWithStringBuffer(ctx, v->strval.str, v->strval.len);
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      return RedisModule_ReplyWithString(ctx, v->rstrval);
    case RSValue_Number: {
      char buf[128] = {0};
      RSValue_NumToString(v->numval, buf);

      if (isTyped) {
        return RedisModule_ReplyWithError(ctx, buf);
      } else {
        return RedisModule_ReplyWithStringBuffer(ctx, buf, strlen(buf));
      }
    }
    case RSValue_Null:
      return RedisModule_ReplyWithNull(ctx);
    case RSValue_Array:
      RedisModule_ReplyWithArray(ctx, v->arrval.len);
      for (uint32_t i = 0; i < v->arrval.len; i++) {
        v->arrval.vals[i]->SendReply(ctx, isTyped);
      }
      return REDISMODULE_OK;
    default:
      RedisModule_ReplyWithNull(ctx);
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void RSValue::Print() const {
  FILE *fp = stderr;
  switch (t) {
    case RSValue_String:
      fprintf(fp, "\"%.*s\"", strval.len, strval.str);
      break;
    case RSValue_RedisString:
    case RSValue_OwnRstring:
      fprintf(fp, "\"%s\"", RedisModule_StringPtrLen(rstrval, nullptr));
      break;
    case RSValue_Number: {
      char tmp[128] = {0};
      RSValue_NumToString(numval, tmp);
      fprintf(fp, "%s", tmp);
      break;
    }
    case RSValue_Null:
      fprintf(fp, "nullptr");
      break;
    case RSValue_Undef:
      fprintf(fp, "<Undefined>");
    case RSValue_Array:
      fprintf(fp, "[");
      for (uint32_t i = 0; i < arrval.len; i++) {
        arrval.vals[i]->Print();
        printf(", ");
      }
      fprintf(fp, "]");
      break;
    case RSValue_Reference:
      ref->Print();
      break;
  }
}

//---------------------------------------------------------------------------------------------

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
        if (!args[i]->IsString()) {
          goto err;
        }
        *ptr = (char *)args[i]->StringPtrLen(nullptr);
        break;
      }
      case 'l': {
        long long *lp = va_arg(ap, long long *);
        double d;
        if (!args[i]->ToNumber(&d)) {
          goto err;
        }
        *lp = (long long)d;
        break;
      }
      case 'd': {
        double *dp = va_arg(ap, double *);
        if (!args[i]->ToNumber(dp)) {
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

//---------------------------------------------------------------------------------------------

const char *RSValue::TypeName(RSValueType t) {
  switch (t) {
    case RSValue_Array:
      return "array";
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
    default:
      return "!!UNKNOWN TYPE!!";
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
