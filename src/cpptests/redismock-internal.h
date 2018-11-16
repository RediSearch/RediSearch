#ifndef REDISMOCK_INTERNAL_H
#define REDISMOCK_INTERNAL_H

// This is to be included only by redismock.cpp
#include <string>
#include <map>
#include <list>
#include <set>
#include <redismodule.h>

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
};

class Value {
 public:
  std::string m_key;
  int m_typecode = REDISMODULE_KEYTYPE_EMPTY;
  size_t m_refcount = 0;

  virtual size_t size() {
    return 0;
  }

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

  Value(const std::string &key, int typecode) : m_key(key), m_typecode(typecode) {
  }

  virtual ~Value() {
  }
};

class HashValue : public Value {
  typedef std::map<std::string, std::string> maptype;

 public:
  struct Key {
    int flags = 0;

    union {
      const void *rawkey = NULL;
      const char *cstr;
      const RedisModuleString *rstr;
    };

    Key(int);
    Key(int, void *);
    std::string makeKey() const;
  };

  HashValue(const std::string &k) : Value(k, REDISMODULE_KEYTYPE_HASH) {
  }

  size_t size() override {
    return m_map.size();
  }

  void hset(const Key &, const RedisModuleString *);
  const std::string *hget(const Key &) const;
  RedisModuleString **kvarray(RedisModuleCtx *allocctx) const;

 private:
  std::map<std::string, std::string> m_map;
};

class ListValue : public Value {
 public:
  std::list<std::string> m_list;
  virtual size_t size() override {
    return m_list.size();
  }
  ListValue(const std::string &k) : Value(k, REDISMODULE_KEYTYPE_LIST) {
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
};

class ModuleValue : public Value {
 public:
  RedisModuleType *mtype = NULL;
  void *value = NULL;
  ModuleValue(const std::string &k, RedisModuleType *mtype)
      : Value(k, REDISMODULE_KEYTYPE_MODULE), mtype(mtype) {
  }
};

struct RedisModuleKey {
  std::string key;
  Value *ref = NULL;
  RedisModuleCtx *parent = NULL;
  int mode = 0;

  RedisModuleKey(RedisModuleCtx *parent_, RedisModuleString *key_, Value *ref_, int mode_)
      : key(*key_), ref(ref_), parent(parent_), mode(mode_) {
    ref->incref();
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
  Value *get(RedisModuleString *s) {
    auto ret = db.find(*s);
    if (ret == db.end()) {
      return NULL;
    }
    return ret->second;
  }

  void set(Value *v) {
    Value *origv = v, *oldv = NULL;
    std::swap(oldv, db[v->key()]);

    if (oldv) {
      oldv->decref();
    }

    origv->incref();
  }

  void erase(const std::string &key) {
    auto e = db.find(key);
    Value *v = e->second;
    db.erase(e);
    v->decref();
  }
};

struct RedisModuleCtx {
  bool automemory = false;
  std::set<RedisModuleString *> allocstrs;
  std::set<RedisModuleKey *> allockeys;
  KVDB *db = NULL;

  void addPointer(RedisModuleString *s) {
    allocstrs.insert(s);
  }
  void addPointer(RedisModuleKey *kk) {
    allockeys.insert(kk);
  }

  void notifyRemoved(RedisModuleKey *k) {
    allockeys.erase(k);
  }

  void notifyRemoved(RedisModuleString *s) {
    allocstrs.erase(s);
  }

  ~RedisModuleCtx();
};

#endif