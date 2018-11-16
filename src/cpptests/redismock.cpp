#include "redismock.h"
#include "redismock-internal.h"
#include <string>
#include <map>
#include <vector>
#include <list>
#include <set>
#include <cstdarg>
#include <cstring>
#include <cerrno>
#include <cmath>
#include <cstdlib>

std::string HashValue::Key::makeKey() const {
  if (flags & REDISMODULE_HASH_CFIELDS) {
    return std::string(cstr);
  } else {
    return *rstr;
  }
}

void HashValue::hset(const HashValue::Key &k, const RedisModuleString *value) {
  std::string skey;
  if (value == REDISMODULE_HASH_DELETE) {
    m_map.erase(skey);
    return;
  }

  if (k.flags & REDISMODULE_HASH_XX) {
    if (m_map.find(skey) == m_map.end()) {
      return;
    }
  } else if (k.flags & REDISMODULE_HASH_NX) {
    if (m_map.find(skey) != m_map.end()) {
      return;
    }
  }
  if (k.flags & REDISMODULE_HASH_CFIELDS) {
    m_map[k.cstr] = *value;
  } else {
    m_map[*k.rstr] = *value;
  }
}

const std::string *HashValue::hget(const Key &e) const {
  auto entry = m_map.find(e.makeKey());
  if (entry == m_map.end()) {
    return NULL;
  }
  return &entry->second;
}

RedisModuleString **HashValue::kvarray(RedisModuleCtx *allocctx) const {
  std::vector<RedisModuleString *> ll;
  for (auto it : m_map) {
    RedisModuleString *keyp = new RedisModuleString(it.first);
    RedisModuleString *valp = new RedisModuleString(it.second);
    ll.push_back(keyp);
    ll.push_back(valp);
    allocctx->addPointer(keyp);
    allocctx->addPointer(valp);
  }

  RedisModuleString **strs = (RedisModuleString **)calloc(ll.size(), sizeof(*strs));
  std::copy(ll.begin(), ll.end(), strs);
  return strs;
}

RedisModuleKey *RMCK_OpenKey(RedisModuleCtx *ctx, RedisModuleString *s, int mode) {
  // Look up in db:
  Value *vv = ctx->db->get(s);
  if (vv) {
    return new RedisModuleKey(ctx, s, vv, mode);
  } else if (mode & REDISMODULE_WRITE) {
    return new RedisModuleKey(ctx, s, NULL, mode);
  } else {
    return NULL;
  }
}

int RMCK_DeleteKey(RedisModuleKey *k) {
  if (!k->ref) {
    return REDISMODULE_OK;
  }
  // Delete the key from the db
  k->parent->db->erase(k->key);
  RedisModule_CloseKey(k);
  return REDISMODULE_OK;
}

void RMCK_CloseKey(RedisModuleKey *k) {
  k->parent->notifyRemoved(k);
  delete k;
}

int RMCK_KeyType(RedisModuleKey *k) {
  if (k->ref == NULL) {
    return REDISMODULE_KEYTYPE_EMPTY;
  } else {
    return k->ref->typecode();
  }
}

size_t RMCK_ValueLength(RedisModuleKey *k) {
  if (k->ref == NULL) {
    return 0;
  } else {
    return k->ref->size();
  }
}

/** String functions */
RedisModuleString *RMCK_CreateString(RedisModuleCtx *ctx, const char *s, size_t n) {
  RedisModuleString *rs = new RedisModuleString(s, n);
  ctx->addPointer(rs);
  return rs;
}

RedisModuleString *RMCK_CreateStringPrintf(RedisModuleCtx *ctx, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  char *outp = NULL;
  vasprintf(&outp, fmt, ap);
  va_end(ap);
  RedisModuleString *ret = RMCK_CreateString(ctx, outp, strlen(outp));
  free(outp);
  return ret;
}

void RMCK_FreeString(RedisModuleCtx *ctx, RedisModuleString *s) {
  s->decref();
  ctx->notifyRemoved(s);
}

void RMCK_RetainString(RedisModuleCtx *ctx, RedisModuleString *s) {
  s->incref();
}

const char *RMCK_StringPtrLen(RedisModuleString *s, size_t *len) {
  if (len) {
    *len = s->size();
  }
  return s->c_str();
}

int RMCK_StringToDouble(RedisModuleString *s, double *outval) {
  char *eptr = NULL;
  double value = strtod(s->c_str(), &eptr);

  if (s->empty() || isspace(s->at(0))) {
    return REDISMODULE_ERR;
  }
  if (eptr - s->c_str() != s->size()) {
    return REDISMODULE_ERR;
  }
  if ((errno == ERANGE && (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
      isnan(value)) {
    return REDISMODULE_ERR;
  }
  *outval = value;
  return REDISMODULE_OK;
}

static int string2ll(const char *s, size_t slen, long long *value) {
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen) return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value != NULL) *value = 0;
    return 1;
  }

  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen) return 0;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
  } else if (p[0] == '0' && slen == 1) {
    *value = 0;
    return 1;
  } else {
    return 0;
  }

  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (ULLONG_MAX / 10)) /* Overflow. */
      return 0;
    v *= 10;

    if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
      return 0;
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen) return 0;

  if (negative) {
    if (v > ((unsigned long long)(-(LLONG_MIN + 1)) + 1)) /* Overflow. */
      return 0;
    if (value != NULL) *value = -v;
  } else {
    if (v > LLONG_MAX) /* Overflow. */
      return 0;
    if (value != NULL) *value = v;
  }
  return 1;
}

int RMCK_StringToLongLong(RedisModuleString *s, long long *l) {
  if (string2ll(s->c_str(), s->size(), l)) {
    return REDISMODULE_OK;
  }
  return REDISMODULE_ERR;
}

/** Hash functions */
#define ENTRY_OK 1
#define ENTRY_DONE 0
#define ENTRY_ERROR -1
static int getNextEntry(va_list &ap, HashValue::Key &e, void **vpp) {
  void *kp = va_arg(ap, void *);
  if (!kp) {
    return ENTRY_DONE;
  }
  *vpp = va_arg(ap, RedisModuleString *);
  if (!vpp) {
    return ENTRY_ERROR;
  }
  e.rawkey = kp;
  return ENTRY_OK;
}

int RMCK_HashSet(RedisModuleKey *key, int flags, ...) {
  bool wasEmpty = false;
  if (!key->ref) {
    // Empty...
    wasEmpty = true;
    key->ref = new HashValue(key->key);
  } else if (key->ref->typecode() != REDISMODULE_KEYTYPE_HASH) {
    return REDISMODULE_ERR;
  }

  HashValue *hv = static_cast<HashValue *>(key->ref);
  va_list ap;
  va_start(ap, flags);
  HashValue::Key e(flags);

  while (true) {
    RedisModuleString *vp;
    int rc = getNextEntry(ap, e, (void **)&vp);
    if (rc == ENTRY_DONE) {
      break;
    } else if (rc == ENTRY_ERROR) {
      goto error;
    } else {
      hv->hset(e, vp);
    }
  }
  va_end(ap);

  if (wasEmpty) {
    // Assign this value to the main DB:
    key->parent->db->set(hv);
    // and delete the original reference
    hv->decref();
  }
  return REDISMODULE_OK;

error:
  if (wasEmpty) {
    delete key->ref;
    key->ref = NULL;
  }
  return REDISMODULE_ERR;
}

int RMCK_HashGet(RedisModuleKey *key, int flags, ...) {
  va_list ap;
  va_start(ap, flags);

  HashValue::Key e(flags);
  if (!key->ref || key->ref->typecode() != REDISMODULE_KEYTYPE_HASH) {
    return REDISMODULE_ERR;
  }

  HashValue *hv = static_cast<HashValue *>(key->ref);

  while (true) {
    void *vpp = NULL;
    int rc = getNextEntry(ap, e, (void **)&vpp);
    if (rc != ENTRY_OK) {
      break;
    }

    // Get the key
    const std::string *value = hv->hget(e);
    if (!value) {
      if (flags & REDISMODULE_HASH_EXISTS) {
        *reinterpret_cast<int *>(vpp) = 0;
      } else {
        *reinterpret_cast<RedisModuleString **>(vpp) = NULL;
      }
    } else {
      if (flags & REDISMODULE_HASH_EXISTS) {
        *reinterpret_cast<int *>(vpp) = 1;
      } else {
        RedisModuleString *newv = new RedisModuleString(*value);
        key->parent->addPointer(newv);
        *reinterpret_cast<RedisModuleString **>(vpp) = newv;
      }
    }
  }
  va_end(ap);
  return REDISMODULE_OK;
}

RedisModuleString **RMCK_HashGetAll(RedisModuleKey *key) {
  if (key->ref == NULL || key->ref->typecode() != REDISMODULE_KEYTYPE_HASH) {
    return NULL;
  }
  auto *hv = static_cast<HashValue *>(key->ref);
  return hv->kvarray(key->parent);
}

/**
 * MODULE TYPES
 */
struct RedisModuleType {
  std::string name;
  int encver;
};
RedisModuleType *RMCK_CreateDataType(RedisModuleCtx *ctx, const char *name, int encver,
                                     RedisModuleTypeMethods *meths) {
  RedisModuleType *ret = new RedisModuleType();
  ret->name = name;
  ret->encver = encver;
  return ret;
}

int RMCK_ModuleTypeSetValue(RedisModuleKey *k, RedisModuleType *mt, void *value) {
  ModuleValue *mv = NULL;
  if (!k->ref) {
    mv = new ModuleValue(k->key, mt);
    k->parent->db->set(mv);
  } else if (k->ref->typecode() != REDISMODULE_KEYTYPE_MODULE) {
    return REDISMODULE_ERR;
  }
  mv->value = value;
  return REDISMODULE_OK;
}

RedisModuleType *RMCK_ModuleTypeGetType(RedisModuleKey *key) {
  if (key->ref == NULL || key->ref->typecode() != REDISMODULE_KEYTYPE_MODULE) {
    return NULL;
  }
  return static_cast<ModuleValue *>(key->ref)->mtype;
}

void *RMCK_ModuleTypeGetValue(RedisModuleKey *key) {
  if (key->ref == NULL || key->ref->typecode() != REDISMODULE_KEYTYPE_MODULE) {
    return NULL;
  }
  return static_cast<ModuleValue *>(key->ref)->value;
}

/** Allocators */
void *RMCK_Alloc(size_t n) {
  return malloc(n);
}

void RMCK_Free(void *p) {
  free(p);
}

void *RMCK_Calloc(size_t nmemb, size_t size) {
  return calloc(nmemb, size);
}

void *RMCK_Realloc(void *p, size_t n) {
  return realloc(p, n);
}

char *RMCK_Strdup(const char *s) {
  return strdup(s);
}

#define REPLY_FUNC(basename, ...)                           \
  int RMCK_Reply##basename(RedisModuleCtx *, __VA_ARGS__) { \
    return REDISMODULE_OK;                                  \
  }

REPLY_FUNC(WithLongLong, long long)
REPLY_FUNC(WithSimpleString, const char *)
REPLY_FUNC(WithError, const char *);
REPLY_FUNC(WithArray, size_t)
REPLY_FUNC(WithStringBuffer, const char *, size_t)
REPLY_FUNC(WithDouble, double)
REPLY_FUNC(WithString, RedisModuleString)

int RMCK_ReplyWithNull(RedisModuleCtx *) {
  return REDISMODULE_OK;
}

int RMCK_ReplySetArrayLength(RedisModuleCtx *, size_t) {
  return REDISMODULE_OK;
}

/** Misc */
RedisModuleCtx::~RedisModuleCtx() {
  for (auto it : allockeys) {
    delete it;
  }
}

/**
 * ENTRY POINTS
 */
std::map<std::string, void *> fnregistry;
#define REGISTER_API(basename) fnregistry["RedisModule_" #basename] = (void *)RMCK_##basename

static void registerApis() {
  REGISTER_API(Alloc);
  REGISTER_API(Calloc);
  REGISTER_API(Realloc);
  REGISTER_API(Strdup);
  REGISTER_API(Free);

  REGISTER_API(OpenKey);
  REGISTER_API(CloseKey);
  REGISTER_API(KeyType);
  REGISTER_API(DeleteKey);
  REGISTER_API(ValueLength);

  REGISTER_API(HashSet);
  REGISTER_API(HashGet);
  REGISTER_API(HashGetAll);

  REGISTER_API(CreateString);
  REGISTER_API(CreateStringPrintf);
  REGISTER_API(FreeString);
  REGISTER_API(RetainString);
  REGISTER_API(StringPtrLen);
  REGISTER_API(StringToDouble);
  REGISTER_API(StringToLongLong);

  REGISTER_API(ModuleTypeSetValue);
  REGISTER_API(ModuleTypeGetValue);
  REGISTER_API(ModuleTypeGetType);
}

int RMCK_GetApi(const char *s, void **pp) {
  if (fnregistry.empty()) {
    registerApis();
  }
  *pp = fnregistry[s];
  return *pp ? REDISMODULE_OK : REDISMODULE_ERR;
}