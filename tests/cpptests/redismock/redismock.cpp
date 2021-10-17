#include "internal.h"
#include "util.h"
#include "redismock.h"

#include <string>
#include <map>
#include <vector>
#include <iostream>
#include <list>
#include <set>
#include <cstdarg>
#include <cstring>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <climits>
#include <cassert>
#include <mutex>

static std::mutex RMCK_GlobalLock;

std::string HashValue::Key::makeKey() const {
  if (flags & REDISMODULE_HASH_CFIELDS) {
    return std::string(cstr);
  } else {
    return *rstr;
  }
}

void HashValue::add(const char *key, const char *value, int mode) {
  if (mode & REDISMODULE_HASH_XX) {
    if (m_map.find(key) == m_map.end()) {
      return;
    }
  } else if (mode & REDISMODULE_HASH_NX) {
    if (m_map.find(key) != m_map.end()) {
      return;
    }
  }
  m_map[key] = value;
}

void HashValue::hset(const HashValue::Key &k, const RedisModuleString *value) {
  const char *skey;
  if (k.flags & REDISMODULE_HASH_CFIELDS) {
    skey = k.cstr;
  } else {
    skey = (*k.rstr).c_str();
  }

  if (value == REDISMODULE_HASH_DELETE) {
    m_map.erase(skey);
    return;
  }

  add(skey, value->c_str(), k.flags);

  if (k.flags & REDISMODULE_HASH_XX) {
    if (m_map.find(skey) == m_map.end()) {
      return;
    }
  } else if (k.flags & REDISMODULE_HASH_NX) {
    if (m_map.find(skey) != m_map.end()) {
      return;
    }
  }
  m_map[skey] = *value;

  if (k.flags & REDISMODULE_HASH_CFIELDS) {
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
  k->ref->decref();
  k->ref = NULL;
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
  if (ctx) {
    ctx->addPointer(rs);
  }
  return rs;
}

RedisModuleString *RMCK_CreateStringFromString(RedisModuleCtx *ctx, RedisModuleString *src) {
  size_t n;
  const char *s = RedisModule_StringPtrLen(src, &n);
  return RedisModule_CreateString(ctx, s, n);
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
  if (ctx) {
    ctx->notifyRemoved(s);
  }
}

void RMCK_RetainString(RedisModuleCtx *ctx, RedisModuleString *s) {
  s->incref();
}

void RMCK_SetModuleOptions(RedisModuleCtx *ctx, int options) {
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
      std::isnan(value)) {
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
    key->ref->incref();
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

typedef enum {
  LL_DEBUG = 0,  // nlb
  LL_VERBOSE,
  LL_NOTICE,
  LL_WARNING
} LogLevel;

int RMCK_LogLevel = LL_NOTICE;
static int loglevelFromString(const char *s) {
  switch (*s) {
    case 'd':
    case 'D':
      return LL_DEBUG;
    case 'v':
    case 'V':
      return LL_VERBOSE;
    case 'n':
    case 'N':
      return LL_NOTICE;
    case 'w':
    case 'W':
      return LL_WARNING;
    default:
      return LL_DEBUG;
  }
}
void RMCK_Log(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {
  int ilevel = loglevelFromString(level);
  if (ilevel < RMCK_LogLevel) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
}

int RMCK_StringCompare(RedisModuleString *a, RedisModuleString *b) {
  return a->compare((std::string)*b);
}

/** MODULE TYPES */
RedisModuleType *RMCK_CreateDataType(RedisModuleCtx *ctx, const char *name, int encver,
                                     RedisModuleTypeMethods *meths) {
  if (Datatype::typemap.find(name) != Datatype::typemap.end()) {
    return NULL;
  }
  RedisModuleType *ret = new RedisModuleType();
  ret->name = name;
  ret->encver = encver;
  ret->typemeths = *meths;
  Datatype::typemap[name] = ret;
  return ret;
}

int RMCK_ModuleTypeSetValue(RedisModuleKey *k, RedisModuleType *mt, void *value) {
  ModuleValue *mv = NULL;
  if (!k->ref) {
    mv = new ModuleValue(k->key, mt);
    k->parent->db->set(mv);
    mv->decref();
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

ModuleValue::~ModuleValue() {
  if (mtype->typemeths.free) {
    mtype->typemeths.free(value);
    value = NULL;
  }
}

Datatype::TypemapType Datatype::typemap;
Command::CommandMap Command::commands;

int RMCK_CreateCommand(RedisModuleCtx *ctx, const char *s, RedisModuleCmdFunc handler, const char *,
                       int, int, int) {
  if (Command::commands.find(s) != Command::commands.end()) {
    return REDISMODULE_ERR;
  }
  Command *c = new Command();
  c->name = s;
  c->handler = handler;
  Command::commands[s] = c;
  return REDISMODULE_OK;
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

void RMCK_SetModuleAttribs(RedisModuleCtx *ctx, const char *name, int ver, int) {
  // Nothing yet.. we're not saving anything anyway
}

RedisModuleCtx *RMCK_GetThreadSafeContext(RedisModuleBlockedClient *bc) {
  assert(bc == NULL);
  return new RedisModuleCtx();
}

void RMCK_FreeThreadSafeContext(RedisModuleCtx *ctx) {
  delete ctx;
}

void RMCK_AutoMemory(RedisModuleCtx *ctx) {
  ctx->automemory = true;
}

void RMCK_ThreadSafeContextLock(RedisModuleCtx *) {
  RMCK_GlobalLock.lock();
}

void RMCK_ThreadSafeContextUnlock(RedisModuleCtx *) {
  RMCK_GlobalLock.unlock();
}

static RedisModuleCallReply *RMCK_CallHset(RedisModuleCtx *ctx, const char *cmd, const char *fmt,
                                           va_list ap) {
  if (strcmp(fmt, "!v") != 0) {
    return NULL;  // we support only !v for now
  }

  RedisModuleString **args = va_arg(ap, RedisModuleString **);
  size_t argLen = va_arg(ap, size_t) - 1;
  Value *v = ctx->db->get(args[0]);
  if (!v) {
    v = new HashValue(RedisModule_StringPtrLen(args[0], NULL));
    ctx->db->set(v);
    v->decref();
  }
  HashValue *hv = static_cast<HashValue *>(v);
  for (size_t i = 1; i < argLen; i += 2) {
    RedisModuleString *field = args[i];
    RedisModuleString *val = args[i + 1];
    HashValue::Key e(0);
    e.rstr = field;
    hv->hset(e, val);
  }

  RMCK_Notify("hset", REDISMODULE_NOTIFY_HASH, RedisModule_StringPtrLen(args[0], NULL));
  return NULL;
}

static RedisModuleCallReply *RMCK_CallHgelall(RedisModuleCtx *ctx, const char *cmd, const char *fmt,
                                              va_list ap) {
  const char *id = NULL;
  if (*fmt == 'c') {
    id = va_arg(ap, const char *);
  } else if (*fmt == 's') {
    RedisModuleString *rid = va_arg(ap, RedisModuleString *);
    id = rid->c_str();
  }

  if (!id) {
    return NULL;
  }

  auto v = ctx->db->get(id);
  RedisModuleCallReply *r = new RedisModuleCallReply(ctx);
  r->type = REDISMODULE_REPLY_ARRAY;
  if (!v) {
    return r;
  }
  if (v->typecode() != REDISMODULE_KEYTYPE_HASH) {
    return r;
  }
  HashValue *hv = static_cast<HashValue *>(v);
  for (auto it : hv->items()) {
    r->arr.push_back(RedisModuleCallReply(ctx, it.first));
    r->arr.push_back(RedisModuleCallReply(ctx, it.second));
  }
  return r;
}

RedisModuleCallReply *RMCK_Call(RedisModuleCtx *ctx, const char *cmd, const char *fmt, ...) {
  // We only support HGETALL for now
  va_list ap;
  RedisModuleCallReply *reply = NULL;
  va_start(ap, fmt);
  if (strcasecmp(cmd, "HGETALL") == 0) {
    reply = RMCK_CallHgelall(ctx, cmd, fmt, ap);
  }

  if (strcasecmp(cmd, "HSET") == 0) {
    reply = RMCK_CallHset(ctx, cmd, fmt, ap);
  }

  va_end(ap);

  return reply;
}

int RMCK_CallReplyType(RedisModuleCallReply *r) {
  return r->type;
}

void RMCK_FreeCallReply(RedisModuleCallReply *r) {
  delete r;
}

size_t RMCK_CallReplyLength(RedisModuleCallReply *r) {
  if (r->type == REDISMODULE_REPLY_ARRAY) {
    return r->arr.size();
  } else if (r->type == REDISMODULE_REPLY_STRING) {
    return r->s.size();
  } else {
    return 0;
  }
}

RedisModuleCallReply *RMCK_CallReplyArrayElement(RedisModuleCallReply *r, size_t idx) {
  assert(r->type == REDISMODULE_REPLY_ARRAY && r->arr.size() > idx);
  return &r->arr[idx];
}

RedisModuleString *RMCK_CreateStringFromCallReply(RedisModuleCallReply *r) {
  switch (r->type) {
    case REDISMODULE_REPLY_STRING:
      return RedisModule_CreateString(r->ctx, r->s.c_str(), r->s.size());
    case REDISMODULE_REPLY_INTEGER:
      return RedisModule_CreateStringPrintf(r->ctx, "%lld", r->ll);
    default:
      return NULL;
  }
}

const char *RMCK_CallReplyStringPtr(RedisModuleCallReply *r, size_t *n) {
  if (r->type != REDISMODULE_REPLY_STRING && r->type != REDISMODULE_REPLY_ERROR) {
    return NULL;
  }
  *n = r->s.size();
  return r->s.c_str();
}

Module::ModuleMap Module::modules;
std::vector<KVDB *> KVDB::dbs;
static int RMCK_GetApi(const char *s, void *pp);

/** Keyspace Events */
std::vector<KeyspaceEventFunction> KeyspaceEvents_g;

void KeyspaceEventFunction::notify(const char *action, int events, const char *key) {
  RMCK::RString rstring(key);
  for (auto ff : KeyspaceEvents_g) {
    if (ff.events & events) {
      ff.call(action, events, rstring);
    }
  }
}

static int RMCK_SubscribeToKeyspaceEvents(RedisModuleCtx *, int types,
                                          RedisModuleNotificationFunc cb) {
  KeyspaceEventFunction fn;
  fn.fn = cb;
  fn.events = types;
  KeyspaceEvents_g.push_back(fn);
  return REDISMODULE_OK;
}

static int RMCK_RegisterCommandFilter(RedisModuleCtx *ctx, RedisModuleCommandFilterFunc callback,
                                      int flags) {
  return REDISMODULE_OK;
}

static std::vector<RedisModuleEventCallback> flushCallbacks;

static int RMCK_SubscribeToServerEvent(RedisModuleCtx *ctx, RedisModuleEvent event,
                                       RedisModuleEventCallback callback) {
  // Make sure we do flush?
  if (event.id == REDISMODULE_EVENT_FLUSHDB) {
    flushCallbacks.push_back(callback);
  }
  return REDISMODULE_OK;
}

/** Misc */
RedisModuleCtx::~RedisModuleCtx() {
  if (automemory) {
    for (auto it : allockeys) {
      delete it;
    }
    for (auto it : allocstrs) {
      delete it;
    }
  }
}

RedisModuleCtx::RedisModuleCtx(uint32_t id) : getApi(RMCK_GetApi), dbid(id) {
  if (id >= KVDB::dbs.size()) {
    KVDB::dbs.resize(id + 1);
  }
  db = KVDB::dbs[id];
  if (!db) {
    KVDB::dbs[id] = new KVDB();
    db = KVDB::dbs[id];
    db->id = id;
  }
}

void KVDB::debugDump() const {
  std::cerr << "DB: " << id << std::endl;
  std::cerr << "Containing " << db.size() << " items" << std::endl;
  for (auto ii : db) {
    std::cerr << "Key: " << ii.first << std::endl;
    std::cerr << "  Type: " << Value::typecodeToString(ii.second->typecode()) << std::endl;
    ii.second->debugDump("  ");
  }
}

/**
 * ENTRY POINTS
 */
std::map<std::string, void *> fnregistry;
#define REGISTER_API(basename) fnregistry["RedisModule_" #basename] = (void *)RMCK_##basename

static int RMCK_ExportSharedAPI(RedisModuleCtx *, const char *name, void *funcptr) {
  if (fnregistry.find(name) != fnregistry.end()) {
    return REDISMODULE_ERR;
  }
  fnregistry[name] = funcptr;
  return REDISMODULE_OK;
}
static void *RMCK_GetSharedAPI(RedisModuleCtx *, const char *name) {
  return fnregistry[name];
}

static void registerApis() {
  REGISTER_API(GetApi);
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
  REGISTER_API(CreateStringFromString);
  REGISTER_API(FreeString);
  REGISTER_API(RetainString);
  REGISTER_API(StringPtrLen);
  REGISTER_API(StringToDouble);
  REGISTER_API(StringToLongLong);

  REGISTER_API(CreateCommand);
  REGISTER_API(CreateDataType);
  REGISTER_API(ModuleTypeSetValue);
  REGISTER_API(ModuleTypeGetValue);
  REGISTER_API(ModuleTypeGetType);

  REGISTER_API(SetModuleAttribs);
  REGISTER_API(Log);
  REGISTER_API(Call);

  REGISTER_API(FreeCallReply);
  REGISTER_API(CallReplyLength);
  REGISTER_API(CallReplyType);
  REGISTER_API(CreateStringFromCallReply);
  REGISTER_API(CallReplyArrayElement);
  REGISTER_API(CallReplyStringPtr);

  REGISTER_API(GetThreadSafeContext);
  REGISTER_API(FreeThreadSafeContext);
  REGISTER_API(ThreadSafeContextLock);
  REGISTER_API(ThreadSafeContextUnlock);
  REGISTER_API(StringCompare);
  REGISTER_API(AutoMemory);
  REGISTER_API(ExportSharedAPI);
  REGISTER_API(GetSharedAPI);

  REGISTER_API(SubscribeToKeyspaceEvents);
  REGISTER_API(SubscribeToServerEvent);
  REGISTER_API(RegisterCommandFilter);

  REGISTER_API(SetModuleOptions);
}

static int RMCK_GetApi(const char *s, void *pp) {
  if (fnregistry.empty()) {
    registerApis();
  }
  *(void **)pp = fnregistry[s];
  return *(void **)pp ? REDISMODULE_OK : REDISMODULE_ERR;
}

extern "C" {

void RMCK_Notify(const char *action, int events, const char *key) {
  KeyspaceEventFunction::notify("hset", REDISMODULE_NOTIFY_HASH, key);
}

void RMCK_Bootstrap(RMCKModuleLoadFunction fn, const char **s, size_t n) {
  // Create the context:
  RedisModuleCtx ctxTmp;
  RMCK::ArgvList args(&ctxTmp, s, n);
  fn(&ctxTmp, &args[0], args.size());
}

void RMCK_Shutdown(void) {
  for (auto db : KVDB::dbs) {
    delete db;
  }
  KVDB::dbs.clear();

  for (auto c : Command::commands) {
    delete c.second;
  }

  for (auto c : Datatype::typemap) {
    delete c.second;
  }
  Datatype::typemap.clear();

  Command::commands.clear();
}
}
