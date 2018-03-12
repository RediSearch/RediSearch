#include "value.h"
#include "util/mempool.h"

///////////////////////////////////////////////////////////////
// Variant Values - will be used in documents as well
///////////////////////////////////////////////////////////////

struct mempool_t *valuePool_g = NULL;

static void *_valueAlloc() {
  return malloc(sizeof(RSValue));
}

static void _valueFree(void *p) {
  free(p);
}

RSValue *RS_NewValue(RSValueType t) {
  if (!valuePool_g) {
    valuePool_g = mempool_new(100, _valueAlloc, _valueFree);
  }
  RSValue *v = mempool_get(valuePool_g);
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
      mempool_release(valuePool_g, v);
    }
  }
}

/* Deep copy an object duplicate strings and array, and duplicate sub values recursively on
 * arrays. On numeric values it's no slower than shallow copy. Redis strings ar not recreated
 */
#define RSValue_Copy RSValue_IncrRef

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

inline void RSValue_MakeReference(RSValue *dst, RSValue *src) {
  *dst = (RSValue){
      .t = RSValue_Reference,
      .refcount = 1,
      .allocated = 0,
      .ref = RSValue_IncrRef(src),
  };
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

int RSValue_ParseNumber(const char *p, size_t l, RSValue *v) {
  v = RSValue_Dereference(v);
  char *e;
  errno = 0;
  double d = strtod(p, &e);
  if ((errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL)) || (errno != 0 && d == 0) ||
      *e != '\0') {
    return 0;
  }
  v->t = RSValue_Number;
  v->numval = d;

  return 1;
}

/* Convert a value to a number, either returning the actual numeric values or by parsing a string
into a number. Return 1 if the value is a number or a numeric string and can be converted, or 0 if
not. If possible, we put the actual value into teh double pointer */
inline int RSValue_ToNumber(RSValue *v, double *d) {
  if (!v) return 0;
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
  return v;
}

inline RSValue *RS_VStringArray(uint32_t sz, ...) {
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
inline RSValue *RS_StringArray(char **strs, uint32_t sz) {
  RSValue **arr = calloc(sz, sizeof(RSValue *));

  for (uint32_t i = 0; i < sz; i++) {
    arr[i] = RS_StringValC(strs[i]);
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
  return ret;
}

RSMultiKey *RS_NewMultiKeyVariadic(int len, ...) {
  RSMultiKey *ret = calloc(1, sizeof(RSMultiKey) + len * sizeof(RSKey));
  ret->len = len;
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
RSMultiKey *RS_NewMultiKeyFromArgs(CmdArray *arr, int allowCaching) {
  RSMultiKey *ret = RS_NewMultiKey(arr->len);
  for (size_t i = 0; i < arr->len; i++) {
    assert(CMDARRAY_ELEMENT(arr, i)->type == CmdArg_String);
    ret->keys[i] = RS_KEY(RSKEY(CMDARG_STRPTR(CMDARRAY_ELEMENT(arr, i))));
  }
  return ret;
}

void RSMultiKey_Free(RSMultiKey *k) {
  free(k);
}

/* Create new KV field */
inline RSField RS_NewField(const char *k, RSValue *val) {
  RSField ret;
  ret.key = (RSKEY(k));
  ret.val = RSValue_IncrRef(val);
  return ret;
}

/* The byte size of the field map */
static inline size_t RSFieldMap_SizeOf(uint16_t cap) {
  return sizeof(RSFieldMap) + cap * sizeof(RSField);
}

/* Create a new field map with a given initial capacity */
RSFieldMap *RS_NewFieldMap(uint16_t cap) {
  if (!cap) cap = 1;
  RSFieldMap *m = calloc(1, RSFieldMap_SizeOf(cap));
  *m = (RSFieldMap){.len = 0, .cap = cap};
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
  if (RSKEY_ISVALIDIDX(k->cachedIdx)) {
    return RSValue_Dereference(FIELDMAP_FIELD(m, k->cachedIdx).val);
  }

  for (uint16_t i = 0; i < m->len; i++) {
    if (!strcmp(FIELDMAP_FIELD(m, i).key, (k->key))) {
      if (k->cachedIdx != RSKEY_NOCACHE) {
        k->cachedIdx = i;
      }
      return RSValue_Dereference(FIELDMAP_FIELD(m, i).val);
    }
  }
  if (k->cachedIdx != RSKEY_NOCACHE) {
    k->cachedIdx = RSKEY_NOTFOUND;
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

void setValuevp(RSValue *v, RSValueType t, va_list ap) {
  v->t = t;
  v->allocated = 0;
  v->refcount = 1;
  switch (t) {
    case RSValue_String:
      v->strval.str = va_arg(ap, char *);
      v->strval.len = va_arg(ap, size_t);

      break;
    case RSValue_RedisString:
      v->rstrval = va_arg(ap, RedisModuleString *);
      break;
    case RSValue_Number:
      v->numval = va_arg(ap, double);
      break;
    default:
      break;
  }
}

//  void RSFieldMap_SetRawValue(RSFieldMap **m, const char *key, RSValueType t, ...) {
//   key = RSKEY(key);
//   va_list ap;
//   va_start(ap, t);
//   if (*m) {
//     for (uint16_t i = 0; i < (*m)->len; i++) {
//       if (!strcmp(FIELDMAP_FIELD(*m, i).key, (key))) {

//         // avoid memory leaks...
//         RSValue_Free(&FIELDMAP_FIELD(*m, i).val);
//         setValuevp(&FIELDMAP_FIELD(*m, i).val, t, ap);
//         goto end;
//       }
//     }
//   }
//   RSFieldMap_EnsureCap(m);

//   // not found - append a new field
//   setValuevp(&FIELDMAP_FIELD(*m, (*m)->len).val, t, ap);
//   FIELDMAP_FIELD(*m, (*m)->len).key = key;
//   (*m)->len++;
// end:
//   va_end(ap);
// }

//  void RSFieldMap_Set(RSFieldMap **m, const char *key, RSValue *in) {
//   key = RSKEY(key);
//   if (*m) {
//     for (uint16_t i = 0; i < (*m)->len; i++) {
//       if (!strcmp(FIELDMAP_FIELD(*m, i).key, (key))) {

//         // avoid memory leaks...
//         RSValue_Free(&FIELDMAP_FIELD(*m, i).val);
//         // assign the new field
//         FIELDMAP_FIELD(*m, i).val = *in;
//         FIELDMAP_FIELD(*m, i).val.allocated = 0;
//         FIELDMAP_FIELD(*m, i).val.refcount = 1;
//         return;
//       }
//     }
//   }
//   RSFieldMap_EnsureCap(m);

//   // not found - append a new field
//   FIELDMAP_FIELD(*m, (*m)->len) = (RSField){.key = key, .val = *in};
//   FIELDMAP_FIELD(*m, (*m)->len).val.allocated = 0;
//   FIELDMAP_FIELD(*m, (*m)->len).val.refcount = 1;
//   (*m)->len++;
// }

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
  free(m);
}

void RSFieldMap_Print(RSFieldMap *m) {
  for (uint16_t i = 0; i < m->len; i++) {
    printf("%s: ", m->fields[i].key);
    RSValue_Print(m->fields[i].val);
    printf("\n");
  }
}
