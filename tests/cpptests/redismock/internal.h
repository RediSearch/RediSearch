/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef REDISMOCK_INTERNAL_H
#define REDISMOCK_INTERNAL_H

// This is to be included only by redismock.cpp
#include "redismodule.h"

#include <string>
#include <map>
#include <unordered_map>
#include <list>
#include <set>
#include <unordered_set>
#include <vector>
#include <iostream>
#include <boost/optional.hpp>

// TODO find out why std::optional doesn't compile on some environments
template <typename T>
using Optional = boost::optional<T>;

struct RedisModuleString : public std::string {
  using std::string::string;

  RedisModuleString(const std::string &s) : std::string(s) {
  }

  size_t refcount = 1;

  void decref() {
    if (!--refcount) {
      delete this;
    }
  }

  void incref() {
    refcount++;
  }

  void trim() {
    this->shrink_to_fit();
  }
};

class Value {
 public:
  std::string m_key;
  int m_typecode = REDISMODULE_KEYTYPE_EMPTY;
  size_t m_refcount = 0;

  virtual size_t size() {
    return 0;
  }

  virtual void debugDump(const char *indent = NULL) const = 0;

  void incref() {
    ++m_refcount;
  }

  const std::string &key() const {
    return m_key;
  }

  int typecode() const {
    return m_typecode;
  }

  void decref() {
    if (!--m_refcount) {
      delete this;
    }
  }

  Value(const std::string &key, int typecode) : m_key(key), m_typecode(typecode), m_refcount(1) {
  }

  static const char *typecodeToString(int tt) {
    switch (tt) {
      case REDISMODULE_KEYTYPE_EMPTY:
        return "<EMPTY>";
      case REDISMODULE_KEYTYPE_HASH:
        return "HASH";
      case REDISMODULE_KEYTYPE_LIST:
        return "LIST";
      case REDISMODULE_KEYTYPE_MODULE:
        return "MODULE";
      case REDISMODULE_KEYTYPE_SET:
        return "SET";
      case REDISMODULE_KEYTYPE_ZSET:
        return "ZSET";
      case REDISMODULE_KEYTYPE_STRING:
        return "STRING";
      default:
        return "UNKNOWN";
    }
  }

  virtual ~Value() {
  }
};

class HashValue : public Value {
  // holds the expiration time for groups of keys (fields)
  using ExpirationMapType = std::map<mstime_t, std::unordered_set<std::string>>;
  // Key to value map
  struct Entry {
    std::string value;
    ExpirationMapType::iterator expirationIt;
  };
  // Example:
  // HSET doc foo bar goo zoo
  // HEXPIRE doc 1 fields 1 foo
  // HEXPIRE doc 3 fields 1 goo
  // KeyMapType: { "foo": ("bar", *), "goo": ("zoo", *) }
  // ExpirationMapType: { 1: [ "foo" ], 3: [ "goo", ... ] }
  using KeyMapType = std::unordered_map<std::string, Entry>;

 public:
  struct Key {
    int flags = 0;

    union {
      const void *rawkey = NULL;
      const char *cstr;
      const RedisModuleString *rstr;
    };

    Key(int f) : flags(f) {
    }
    Key(int, void *);
    std::string makeKey() const;
  };

  HashValue(const std::string &k) : Value(k, REDISMODULE_KEYTYPE_HASH) {
  }

  size_t size() override {
    return m_map.size();
  }

  virtual void debugDump(const char *indent) const override {
    for (auto ii : m_map) {
      std::cerr << indent << ii.first << ": " << ii.second.value << std::endl;
    }
  }
  void hset(const Key &, const RedisModuleString *);
  void add(const char *key, const char *value, int mode = REDISMODULE_HASH_NONE);
  bool hexpire(const Key &, mstime_t expireAt);
  Optional<mstime_t> min_expire_time() const;
  Optional<mstime_t> get_expire_time(const Key &) const;

  const std::string *hget(const Key &) const;
  RedisModuleString **kvarray(RedisModuleCtx *allocctx) const;
  const KeyMapType &items() const {
    return m_map;
  }

  auto begin() {
    return m_map.begin();
  }

  auto end() {
    return m_map.end();
  }

 private:
  KeyMapType m_map;
  ExpirationMapType m_expiration;
};

class ListValue : public Value {
 public:
  std::list<std::string> m_list;
  virtual size_t size() override {
    return m_list.size();
  }
  ListValue(const std::string &k) : Value(k, REDISMODULE_KEYTYPE_LIST) {
  }
  virtual void debugDump(const char *indent) const override {
    for (auto ii : m_list) {
      std::cerr << indent << ii << std::endl;
    }
  }
};

class StringValue : public Value {
 public:
  std::string m_string;
  virtual size_t size() override {
    return m_string.size();
  }
  StringValue(const std::string &k) : Value(k, REDISMODULE_KEYTYPE_STRING) {
  }
  virtual void debugDump(const char *indent) const override {
    std::cerr << indent << m_string << std::endl;
  }
};

class ModuleValue : public Value {
 public:
  RedisModuleType *mtype = NULL;
  void *value = NULL;
  ModuleValue(const std::string &k, RedisModuleType *mtype)
      : Value(k, REDISMODULE_KEYTYPE_MODULE), mtype(mtype) {
  }
  virtual void debugDump(const char *indent) const override {
    std::cerr << indent << "Type=" << std::hex << mtype << " Value=" << std::hex << value
              << std::endl;
  }

  virtual ~ModuleValue();
};

struct RedisModuleKey {
  std::string key;
  Value *ref = NULL;
  RedisModuleCtx *parent = NULL;
  int mode = 0;

  RedisModuleKey(RedisModuleCtx *parent_, RedisModuleString *key_, Value *ref_, int mode_)
      : key(*key_), ref(ref_), parent(parent_), mode(mode_) {
    if (ref) {
      ref->incref();
    }
  }

  ~RedisModuleKey() {
    if (ref) {
      ref->decref();
      ref = NULL;
    }
  }
};

struct KVDB {
  std::map<std::string, Value *> db;
  uint32_t id = -1;

  Value *get(const std::string &s) {
    auto ret = db.find(s);
    if (ret == db.end()) {
      return NULL;
    }
    return ret->second;
  }

  Value *get(RedisModuleString *s) {
    return get(*s);
  }

  Value *get(const char *s) {
    std::string tmp(s);
    return get(tmp);
  }

  void set(Value *v) {
    Value *oldv = db[v->key()];
    db[v->key()] = v;
    v->incref();
    if (oldv) {
      oldv->decref();
    }
  }

  bool erase(const std::string &key) {
    auto e = db.find(key);
    if (e == db.end()) {
      return false;
    }
    Value *v = e->second;
    db.erase(e);
    v->decref();
    return true;
  }

  void clear() {
    for (auto it : db) {
      it.second->decref();
    }
    db.clear();
  }

  size_t size() const {
    return db.size();
  }

  ~KVDB() {
    clear();
  }

  void debugDump() const;

  static std::vector<KVDB *> dbs;
};

typedef int (*RedisModule_GetApiFunctionType)(const char *name, void *pp);

struct RedisModuleCtx {
  RedisModule_GetApiFunctionType getApi = NULL;
  bool automemory = false;
  std::set<RedisModuleString *> allocstrs;
  std::set<RedisModuleKey *> allockeys;
  KVDB *db = NULL;
  uint32_t dbid = 0;

  RedisModuleCtx(uint32_t dbid = 0);

  void addPointer(RedisModuleString *s) {
    if (automemory) {
      allocstrs.insert(s);
    }
  }

  void addPointer(RedisModuleKey *kk) {
    if (automemory) {
      allockeys.insert(kk);
    }
  }

  void notifyRemoved(RedisModuleKey *k) {
    allockeys.erase(k);
  }

  void notifyRemoved(RedisModuleString *s) {
    allocstrs.erase(s);
  }

  ~RedisModuleCtx();
};

class Module {
 public:
  typedef std::map<std::string, Module *> ModuleMap;
  int apiver;
  std::string name;
  static ModuleMap modules;
};

extern int RMCK_LogLevel;

class RedisModuleCommand {
 public:
  typedef std::map<std::string, RedisModuleCommand *> CommandMap;
  std::string name;
  RedisModuleCmdFunc handler;
  CommandMap subcommands;
  ~RedisModuleCommand() {
    for (auto &sc : subcommands) {
      delete sc.second;
    }
  }
  static CommandMap commands;
};

struct RedisModuleType {
  typedef std::map<std::string, RedisModuleType *> TypemapType;
  std::string name;
  int encver;
  RedisModuleTypeMethods typemeths;
  static TypemapType typemap;
};

typedef struct RedisModuleType Datatype;

struct RedisModuleCallReply {
  int type = 0;
  long long ll = 0;
  std::string s;
  std::vector<RedisModuleCallReply> arr;
  RedisModuleCtx *ctx;
  RedisModuleCallReply(RedisModuleCtx *ctx, const std::string &s_)
      : type(REDISMODULE_REPLY_STRING), s(s_), ctx(ctx) {
  }
  RedisModuleCallReply(RedisModuleCtx *ctx) : ctx(ctx) {
  }
  ~RedisModuleCallReply() {
  }
};

struct KeyspaceEventFunction {
  RedisModuleNotificationFunc fn;
  int events;
  void call(const char *action, int events, RedisModuleString *k) {
    RedisModuleCtx ctx;
    fn(&ctx, events, action, k);
  }
  static void notify(const char *action, int events, const char *key);
};

#endif
