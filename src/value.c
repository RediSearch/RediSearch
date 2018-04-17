#include "value.h"
#include "util/mempool.h"
#include <pthread.h>

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////

typedef struct {
  mempool_t *values;
  mempool_t *fieldmaps;
} mempoolThreadPool;

static void mempoolThreadPoolDtor(void *p) {
  mempoolThreadPool *tp = p;
  mempool_destroy(tp->values);
  mempool_destroy(tp->fieldmaps);
  free(tp);
}

pthread_key_t mempoolKey_g;

static void *_valueAlloc() {
  return malloc(sizeof(RSValue));
}

static void _valueFree(void *p) {
  free(p);
}

/* The byte size of the field map */
static inline size_t RSFieldMap_SizeOf(uint16_t cap) {
  return sizeof(RSFieldMap) + cap * sizeof(RSField);
}

void *_fieldMapAlloc() {
  RSFieldMap *ret = calloc(1, RSFieldMap_SizeOf(8));
  ret->cap = 8;
  return ret;
}

static void __attribute__((constructor)) initKey() {
  pthread_key_create(&mempoolKey_g, mempoolThreadPoolDtor);
}

static inline mempoolThreadPool *getPoolInfo() {
  mempoolThreadPool *tp = pthread_getspecific(mempoolKey_g);
  if (tp == NULL) {
    tp = calloc(1, sizeof(*tp));
    tp->values = mempool_new_limited(1000, 0, _valueAlloc, _valueFree);
    tp->fieldmaps = mempool_new_limited(100, 1000, _fieldMapAlloc, free);
    pthread_setspecific(mempoolKey_g, tp);
  }
  return tp;
}

RSValue *RS_NewValue(RSValueType t) {
  RSValue *v = mempool_get(getPoolInfo()->values);
  v->t = t;
  v->refcount = 0;
  v->allocated = 1;
  return v;
}
/* Free a value's internal value. It only does anything in the case of a string, and doesn't free
 * the actual value object */
inline void RSValue_Free(RSValue *v) {
  if (!v) return;
  --v->refcount;
  // RSValue_Print(v);
  if (v->refcount <= 0) {

    switch (v->t) {
      case RSValue_String:
        // free strings by allocation strategy
        switch (v->strval.stype) {
          case RSString_Malloc:
            free(v->strval.str);
            break;
          case RSString_RMAlloc:
            RedisModule_Free(v->strval.str);
            break;
          case RSString_SDS:
            sdsfree(v->strval.str);
            break;
          case RSString_Const:
          case RSString_Volatile:
            break;
        }
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
      default:  // no free
        break;
    }

    if (v->allocated) {
      mempool_release(getPoolInfo()->values, v);
    }
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
  assert(len <= (UINT32_MAX >> 4));
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
inline RSValue *RS_ConstStringVal(char *str, uint32_t len) {
  return RS_StringValT(str, len, RSString_Const);
}

/* Wrap a string with length into a value object, assuming the string is a null terminated C
 * string
 */
inline RSValue *RS_StringValC(char *str) {
  return RS_StringVal(str, strlen(str));
}

RSValue *RS_StringValFmt(const char *fmt, ...) {

  char *buf;
  va_list ap;
  va_start(ap, fmt);
  vasprintf(&buf, fmt, ap);
  va_end(ap);
  return RS_StringVal(buf, strlen(buf));
}

inline RSValue *RS_ConstStringValC(char *str) {
  return RS_StringValT(str, strlen(str), RSString_Const);
}

/* Wrap a redis string value */
inline RSValue *RS_RedisStringVal(RedisModuleString *str) {
  RSValue *v = RS_NewValue(RSValue_RedisString);
  v->rstrval = str;
  return v;
}

/* Convert a value to a string value. If the value is already a string value it gets
 * shallow-copied (no string buffer gets copied) */
void RSValue_ToString(RSValue *dst, RSValue *v) {
  switch (v->t) {
    case RSValue_String:
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
inline int RSValue_ToNumber(RSValue *v, double *d) {
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
inline const void *RSValue_ToBuffer(RSValue *value, size_t *outlen) {
  value = RSValue_Dereference(value);

  switch (value->t) {
    case RSValue_Number:
      *outlen = sizeof(value->numval);
      return &value->numval;
    case RSValue_String:
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

// Gets the string pointer and length from the value
inline const char *RSValue_StringPtrLen(RSValue *value, size_t *lenp) {
  value = RSValue_Dereference(value);

  switch (value->t) {
    case RSValue_String:
      if (lenp) {
        *lenp = value->strval.len;
      }
      return value->strval.str;
    case RSValue_RedisString:
      return RedisModule_StringPtrLen(value->rstrval, lenp);
    default:
      return NULL;
  }
}

// Combines PtrLen with ToString to convert any RSValue into a string buffer.
// Returns NULL if buf is required, but is too small
inline const char *RSValue_ConvertStringPtrLen(RSValue *value, size_t *lenp, char *buf,
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
inline RSValue *RS_NumVal(double n) {
  RSValue *v = RS_NewValue(RSValue_Number);
  v->numval = n;
  return v;
}

/* Wrap an array of RSValue objects into an RSValue array object */
inline RSValue *RS_ArrVal(RSValue **vals, uint32_t len) {

  RSValue *v = RS_NewValue(RSValue_Array);
  v->arrval.vals = vals;
  v->arrval.len = len;
  for (uint32_t i = 0; i < len; i++) {
    RSValue_IncrRef(v->arrval.vals[i]);
  }
  return v;
}

RSValue *RS_VStringArray(uint32_t sz, ...) {
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
RSValue *RS_StringArray(char **strs, uint32_t sz) {
  RSValue **arr = calloc(sz, sizeof(RSValue *));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValC(strs[i]);
  }
  return RS_ArrVal(arr, sz);
}

RSValue *RS_StringArrayT(char **strs, uint32_t sz, RSStringType st) {
  RSValue **arr = calloc(sz, sizeof(RSValue *));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValT(strs[i], strlen(strs[i]), st);
  }
  return RS_ArrVal(arr, sz);
}
RSValue RS_NULL = {.t = RSValue_Null, .refcount = 1, .allocated = 0};
/* Create a new NULL RSValue */
inline RSValue *RS_NullVal() {
  return &RS_NULL;
}

RSValue *RS_NewValueFromCmdArg(CmdArg *arg) {
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
int RSValue_Cmp(RSValue *v1, RSValue *v2) {
  assert(v1);
  assert(v2);
  v1 = RSValue_Dereference(v1);
  v2 = RSValue_Dereference(v2);

  if (v1->t == v2->t) {
    switch (v1->t) {
      case RSValue_Number:

        return v1->numval > v2->numval ? 1 : (v1->numval < v2->numval ? -1 : 0);
      case RSValue_String:
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

  // if one of the values is null, the other wins
  if (v1->t == RSValue_Null) {
    return -1;
  } else if (v2->t == RSValue_Null) {
    return 1;
  }

  // cast to strings and compare as strings
  char buf1[100], buf2[100];

  size_t l1, l2;
  const char *s1 = RSValue_ConvertStringPtrLen(v1, &l1, buf1, sizeof(buf1));
  const char *s2 = RSValue_ConvertStringPtrLen(v2, &l2, buf2, sizeof(buf2));
  return cmp_strings(s1, s2, l1, l2);
}

/* Based on the value type, serialize the value into redis client response */
int RSValue_SendReply(RedisModuleCtx *ctx, RSValue *v) {
  if (!v) {
    return RedisModule_ReplyWithNull(ctx);
  }
  v = RSValue_Dereference(v);

  switch (v->t) {
    case RSValue_String:
      return RedisModule_ReplyWithStringBuffer(ctx, v->strval.str, v->strval.len);
    case RSValue_RedisString:
      return RedisModule_ReplyWithString(ctx, v->rstrval);
    case RSValue_Number: {
      char buf[128];
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

void RSValue_Print(RSValue *v) {
  if (!v) {
    printf("nil");
  }
  printf("{%d}", v->t);
  switch (v->t) {
    case RSValue_String:
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

RSMultiKey *RS_NewMultiKey(uint16_t len) {
  RSMultiKey *ret = calloc(1, sizeof(RSMultiKey) + len * sizeof(RSKey));
  ret->len = len;
  ret->keysAllocated = 0;
  return ret;
}

RSMultiKey *RS_NewMultiKeyVariadic(int len, ...) {
  RSMultiKey *ret = calloc(1, sizeof(RSMultiKey) + len * sizeof(RSKey));
  ret->len = len;
  ret->keysAllocated = 0;
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
RSMultiKey *RS_NewMultiKeyFromArgs(CmdArray *arr, int allowCaching, int duplicateStrings) {
  RSMultiKey *ret = RS_NewMultiKey(arr->len);
  ret->keysAllocated = duplicateStrings;
  for (size_t i = 0; i < arr->len; i++) {
    assert(CMDARRAY_ELEMENT(arr, i)->type == CmdArg_String);
    ret->keys[i] = RS_KEY(RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(arr, i))));
    if (duplicateStrings) {
      ret->keys[i].key = strdup(ret->keys[i].key);
    }
  }
  return ret;
}

RSMultiKey *RSMultiKey_Copy(RSMultiKey *k, int copyKeys) {
  RSMultiKey *ret = RS_NewMultiKey(k->len);
  ret->keysAllocated = copyKeys;

  for (size_t i = 0; i < k->len; i++) {
    ret->keys[i] = RS_KEY(copyKeys ? strdup(k->keys[i].key) : k->keys[i].key);
  }
  return ret;
}

void RSMultiKey_Free(RSMultiKey *k) {
  if (k->keysAllocated) {
    for (size_t i = 0; i < k->len; i++) {
      free((char *)k->keys[i].key);
    }
  }
  free(k);
}

/* Create new KV field */
inline RSField RS_NewField(const char *k, RSValue *val) {
  RSField ret;
  ret.key = (RSKEY(k));
  ret.val = RSValue_IncrRef(val);
  return ret;
}

/* Create a new field map with a given initial capacity */
RSFieldMap *RS_NewFieldMap(uint16_t cap) {
  if (!cap) cap = 1;
  RSFieldMap *m = mempool_get(getPoolInfo()->fieldmaps);
  m->len = 0;
  return m;
}

/* Make sure the fieldmap has enough capacity to add elements */
void RSFieldMap_EnsureCap(RSFieldMap **m) {
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

RSValue *RSFieldMap_GetByKey(RSFieldMap *m, RSKey *k) {
  if (RSKEY_ISVALIDIDX(k->fieldIdx)) {
    return RSValue_Dereference(FIELDMAP_FIELD(m, k->fieldIdx).val);
  }

  for (uint16_t i = 0; i < m->len; i++) {
    if (!strcmp(FIELDMAP_FIELD(m, i).key, (k->key))) {
      if (k->fieldIdx != RSKEY_NOCACHE) {
        k->fieldIdx = i;
      }
      return RSValue_Dereference(FIELDMAP_FIELD(m, i).val);
    }
  }
  if (k->fieldIdx != RSKEY_NOCACHE) {
    k->fieldIdx = RSKEY_NOTFOUND;
  }

  return RS_NullVal();
}

/* Add a filed to the map WITHOUT checking for duplications */
void RSFieldMap_Add(RSFieldMap **m, const char *key, RSValue *val) {
  RSFieldMap_EnsureCap(m);
  // Creating the field will create a  reference and increase the ref count on val
  FIELDMAP_FIELD(*m, (*m)->len++) = RS_NewField(key, val);
}

/* Set a value in the map for a given key, checking for duplicates and replacing the existing
 * value if needed, and appending a new one if needed */
void RSFieldMap_Set(RSFieldMap **m, const char *key, RSValue *val) {
  key = RSKEY(key);
  if (*m) {
    for (uint16_t i = 0; i < (*m)->len; i++) {
      if (!strcmp(FIELDMAP_FIELD(*m, i).key, (key))) {

        // avoid memory leaks...
        RSValue_Free(FIELDMAP_FIELD(*m, i).val);
        // assign the new field
        FIELDMAP_FIELD(*m, i).val = RSValue_IncrRef(val);
        return;
      }
    }
  }
  RSFieldMap_EnsureCap(m);

  // not found - append a new field
  FIELDMAP_FIELD(*m, (*m)->len++) = RS_NewField(key, val);
}

void RSFieldMap_SetNumber(RSFieldMap **m, const char *key, double d) {
  RSFieldMap_Set(m, key, RS_NumVal(d));
}

inline void RSFieldMap_Reset(RSFieldMap *m) {
  if (m) {
    for (size_t i = 0; i < m->len; i++) {
      RSValue_Free(m->fields[i].val);
    }
    m->len = 0;
  }
}
/* Free the field map. If freeKeys is set to 1 we also free the keys */
void RSFieldMap_Free(RSFieldMap *m, int freeKeys) {
  if (!m) return;
  for (uint16_t i = 0; i < m->len; i++) {
    RSValue_Free(m->fields[i].val);

    if (freeKeys) free((void *)m->fields[i].key);
  }
  m->len = 0;
  mempool_release(getPoolInfo()->fieldmaps, m);
  // free(m);
}

void RSFieldMap_Print(RSFieldMap *m) {
  for (uint16_t i = 0; i < m->len; i++) {
    printf("%s: ", m->fields[i].key);
    RSValue_Print(m->fields[i].val);
    printf(", ");
  }
  printf("\n");
}

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
    case RSValue_Number:
      return "number";
    case RSValue_String:
      return "string";
    case RSValue_Null:
      return "(null)";
    case RSValue_RedisString:
      return "redis-string";
    case RSValue_Reference:
      return "reference";
    default:
      return "!!UNKNOWN TYPE!!";
  }
}