
#pragma once

#include "redismodule.h"
#include "util/dllist.h"
#include "object.h"

#include <time.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct GCAPI {
  virtual int PeriodicCallback(RedisModuleCtx* ctx) { return 0; }
  virtual void RenderStats(RedisModuleCtx* ctx) {}
  virtual void OnDelete() {}
  virtual void OnTerm() {}

  // Send a "kill signal" to the GC, requesting it to terminate asynchronously
  virtual void Kill() {}
  virtual struct timespec GetInterval() { struct timespec ts = {0}; return ts; }
  virtual RedisModuleCtx *GetRedisCtx() { return NULL; }

  virtual ~GCAPI() {}
};

//---------------------------------------------------------------------------------------------

#define DEADBEEF (void*)0xDEADBEEF

struct GC : public Object {
  GCAPI* gc;
  RedisModuleTimerID timerID;
  bool stopped;

  GC(struct IndexSpec* sp, float initialHZ, uint64_t uniqueId, uint32_t gcPolicy);
  GC(RedisModuleString* keyName, float initialHZ, uint64_t uniqueId);
  virtual ~GC();

  void Start();
  void Stop();
  void RenderStats(RedisModuleCtx* ctx);
  void OnDelete();
  void Kill();
  void ForceInvoke(RedisModuleBlockedClient* bc = DEADBEEF);
  void ForceBGInvoke();

  long long GetNextPeriod();

  static void ThreadPoolStart();
  static void ThreadPoolDestroy();

  static void _destroyCallback(GC* gc);
};

//---------------------------------------------------------------------------------------------

struct GCTask : public Object {
  GC* gc;
  RedisModuleBlockedClient* bClient;

  GCTask(GC *gc, RedisModuleBlockedClient* bClient) : gc(gc), bClient(bClient) {}

  RedisModuleTimerID scheduleNext();

  static void _taskThread(GCTask *task);
  static void _timerCallback(RedisModuleCtx* ctx, GCTask *task);
};



///////////////////////////////////////////////////////////////////////////////////////////////
