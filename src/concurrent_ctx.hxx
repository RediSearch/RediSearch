
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

template <class T>
void ConcurrentSearch<T>::CloseKeys() {
  size_t sz = openKeys.size();
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKey<T> *kx = openKeys[i];
    if (kx->key) {
      RedisModule_CloseKey(kx->key);
      kx->key = NULL;
    }
  }
}

//---------------------------------------------------------------------------------------------

template <class T>
void ConcurrentSearch<T>::ReopenKeys() {
  size_t sz = openKeys.size();
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKey<T> *kx = openKeys[i];
    kx->key = RedisModule_OpenKey(ctx, kx->keyName, kx->keyFlags);
    // if the key is marked as shared, make sure it isn't now
    kx->Reopen();
  }
}

//---------------------------------------------------------------------------------------------

// Check the elapsed timer, and release the lock if enough time has passed.
// Return true if switching took place

template <class T>
bool ConcurrentSearch<T>::CheckTimer() {
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

template <class T>
void ConcurrentSearch<T>::ResetClock() {
  clock_gettime(CLOCK_MONOTONIC_RAW, &lastTime);
  ticker = 0;
}

//---------------------------------------------------------------------------------------------

// Initialize a concurrent context

template <class T>
ConcurrentSearch<T>::ConcurrentSearch(RedisModuleCtx *rctx) {
  ctx = rctx;
  isLocked = false;
  ResetClock();
}

//---------------------------------------------------------------------------------------------

template <class T>
ConcurrentSearch<T>::~ConcurrentSearch() {
  // Release the monitored open keys
  for (size_t i = 0; i < openKeys.size(); i++) {
    ConcurrentKey<T> *concKey = openKeys[i];
    RedisModule_FreeString(ctx, concKey->keyName);

    if (concKey->key) {
      RedisModule_CloseKey(concKey->key);
      concKey->key = NULL;
    }

    delete concKey;
  }

  delete openKeys;
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

template <class T>
void ConcurrentSearch<T>::AddKey(T &&key) {
  openKeys.empalace_back(std::move(key));
  //RedisModule_RetainString(ctx, concKey->keyName); //@@ TODO: ensure valid
}

//---------------------------------------------------------------------------------------------

// Replace the key at a given position. The context must not be locked. It
// is assumed that the callback for the key remains the same
// - redisCtx is the redis module context which owns this key
// - keyName is the name of the new key
// - pos is the position at which the key resides (usually 0)
// - arg is the new arg to be passed to the callback

template <class T>
void ConcurrentSearch<T>::SetKey(RedisModuleString *keyName, void *privdata) {
  openKeys[0]->keyName = keyName;
  openKeys[0]->privdata = privdata; //@@ Can we remove privdata from ConcurrentKey?
}

//---------------------------------------------------------------------------------------------

template <class T>
void ConcurrentSearch<T>::Lock() {
  RS_LOG_ASSERT(!isLocked, "Redis GIL shouldn't be locked");
  RedisModule_ThreadSafeContextLock(ctx);
  isLocked = true;
  ReopenKeys();
}

//---------------------------------------------------------------------------------------------

template <class T>
void ConcurrentSearch<T>::Unlock() {
  CloseKeys();
  RedisModule_ThreadSafeContextUnlock(ctx);
  isLocked = false;
}

//---------------------------------------------------------------------------------------------

// This function is called by concurrent executors (currently the query only).
// It checks if enough time has passed and releases the global lock if that is the case.

template <class T>
bool ConcurrentSearch<T>::Tick() {
  if (++ticker % CONCURRENT_TICK_CHECK == 0) {
    if (CheckTimer()) {
      return true;
    }
  }
  return false;
}

///////////////////////////////////////////////////////////////////////////////////////////////
