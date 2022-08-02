
///////////////////////////////////////////////////////////////////////////////////////////////


/** The maximal size of the concurrent query thread pool. Since only one thread is operational at a
 * time, it's not a problem besides memory consumption, to have much more threads than CPU cores.
 * By default the pool starts with just one thread, and scales up as needed  */

/**
 * The maximum number of threads performing indexing on documents.
 * It's good to set this to approximately the number of CPUs running.
 *
 * NOTE: This is merely the *fallback* value if for some reason the number of
 * CPUs cannot be automatically determined. If you want to force the number
 * of tokenizer threads, make sure you also disable the CPU detection in the
 * source file
 */

// The number of execution "ticks" per elapsed time check. This is intended to reduce the number of
// calls to clock_gettime()
#define CONCURRENT_TICK_CHECK 50

// The timeout after which we try to switch to another query thread - in Nanoseconds
#define CONCURRENT_TIMEOUT_NS 100000

///////////////////////////////////////////////////////////////////////////////////////////////

void ConcurrentSearch::CloseKeys() {
  for (size_t i = 0; i < concKeys.size(); i++) {
    if (concKeys[i].key) {
      RedisModule_CloseKey(concKeys[i].key);
      concKeys[i].key = NULL;
    }
  }
}

//---------------------------------------------------------------------------------------------

void ConcurrentSearch::ReopenKeys() {
  for (size_t i = 0; i < concKeys.size(); i++) {
    concKeys[i].key = RedisModule_OpenKey(ctx, concKeys[i].keyName, concKeys[i].keyFlags);
    concKeys[i].Reopen();
  }
}

//---------------------------------------------------------------------------------------------

// Check the elapsed timer, and release the lock if enough time has passed.
// Return true if switching took place

bool ConcurrentSearch::CheckTimer() {
  static struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);

  long long durationNS = (long long)1000000000 * (now.tv_sec - lastTime.tv_sec) +
                         (now.tv_nsec - lastTime.tv_nsec);

  // Timeout - release the thread safe context lock and let other threads run as well
  if (durationNS > CONCURRENT_TIMEOUT_NS) {
    Unlock();

    // Right after releasing, we try to acquire the lock again.
    // If other threads are waiting on it, the kernel will decide which one
    // will get the chance to run again. Calling sched_yield is not necessary here.
    // See http://blog.firetree.net/2005/06/22/thread-yield-after-mutex-unlock/
    Lock();

    // Right after re-acquiring the lock, we sample the current time.
    // This will be used to calculate the elapsed running time
    ResetClock();
    return true;
  }

  return false;
}

//---------------------------------------------------------------------------------------------

// Reset the clock variables in the concurrent search context

void ConcurrentSearch::ResetClock() {
  clock_gettime(CLOCK_MONOTONIC_RAW, &lastTime);
  ticker = 0;
}

//---------------------------------------------------------------------------------------------

// Initialize a concurrent context

ConcurrentSearch::ConcurrentSearch(RedisModuleCtx *rctx) {
  ctx = rctx;
  isLocked = false;
  ResetClock();
}

//---------------------------------------------------------------------------------------------

ConcurrentSearch::~ConcurrentSearch() {
  // Release the monitored open keys
  for (size_t i = 0; i < concKeys.size(); i++) {
    RedisModule_FreeString(ctx, concKeys[i].keyName);
  }

  CloseKeys();
  delete ctx;
}

//---------------------------------------------------------------------------------------------

// Add a "monitored" key to the context. When keys are open during concurrent execution, they need
// to be closed before we yield execution and release the GIL, and reopened when we get back the
// execution context.
// To simplify this, each place in the program that holds a reference to a redis key
// based data, registers itself and the key to be automatically reopened.
//
// After reopening, a callback
// is being called to notify the key holder that it has been reopened, and handle the consequences.
// This is used by index iterators to avoid holding reference to deleted keys or changed data.
//
// We register the key, the flags to reopen it, a string holding its name for reopening, a callback
// for notification, and private callback data. if freePrivDataCallback is provided, we will call it
// when the context is freed to release the private data. If NULL is passed, we do nothing

template <class ConcurrentKey1>
void ConcurrentSearch::AddKey(ConcurrentKey1 &&key) {
  concKeys.emplace_back(std::move(key));
  //RedisModule_RetainString(ctx, concKeys[0].keyName); //@@ TODO: ensure valid
}

//---------------------------------------------------------------------------------------------

void ConcurrentSearch::Lock() {
  if (!isLocked) {
    throw Error("Redis GIL shouldn't be locked");
  }
  RedisModule_ThreadSafeContextLock(ctx);
  isLocked = true;
  ReopenKeys();
}

//---------------------------------------------------------------------------------------------

void ConcurrentSearch::Unlock() {
  CloseKeys();
  RedisModule_ThreadSafeContextUnlock(ctx);
  isLocked = false;
}

//---------------------------------------------------------------------------------------------

// This function is called by concurrent executors (currently the query only).
// It checks if enough time has passed and releases the global lock if that is the case.

bool ConcurrentSearch::Tick() {
  if (++ticker % CONCURRENT_TICK_CHECK == 0) {
    if (CheckTimer()) {
      return true;
    }
  }
  return false;
}

///////////////////////////////////////////////////////////////////////////////////////////////
