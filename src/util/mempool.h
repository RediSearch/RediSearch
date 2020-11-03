
#pragma once

// Mempool - an uber simple, thread-unsafe, memory pool
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
#include <utility>
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

// mempool - the struct holding the memory pool

struct mempool_t {
  struct options {
    size_t initialCap;  // Initial size of the pool
    size_t maxCap;      // maxmimum size of the pool

    // if true, will be added to the list of global mempool objects which
    // will be destroyed via mempool_free_global(). 
    // This also means you cannot call mempool_destroy() on it manually.
    int isGlobal;
  };

  void **entries;
  size_t top;
  size_t cap;
  size_t max;  // max size for pool
  pthread_mutex_t lock;

  // Create a new memory pool
  mempool_t(size_t initialCap, size_t maxCap, bool isGlobal);
  ~mempool_t() { destroy(); }

  // Get an entry from the pool, allocating a new instance if unavailable
  void *get();

  // Release an allocated instance to the pool
  void release(void *ptr);

  // destroy the pool, releasing all entries in it and destroying its internal array
  void destroy();

  // Free all created memory pools
  static void free_global();

protected:
  // stateless allocation function for the pool
  virtual void *alloc();

// free function for the pool
  virtual void _free(void *);

  static int mempoolDisable_g;
};

//---------------------------------------------------------------------------------------------

#if 0

#define MEMPOOOL_STATIC_ALLOCATOR(name, sz) \
  void *name() {                            \
    return rm_malloc(sz);                   \
  }

#endif // 0

//---------------------------------------------------------------------------------------------

#ifdef __cplusplus

//---------------------------------------------------------------------------------------------

class MemPool : public mempool_t {
	const char *name;
public:
  MemPool(size_t initialCap, size_t maxCap, bool isGlobal) : mempool_t(initialCap, maxCap, isGlobal) {}

  void* alloc(std::size_t sz) { return get(); }
  void free(void *p) { release(p); }
};

//---------------------------------------------------------------------------------------------

template <class Pool>
class MemPoolObject {
public:
	static Pool pool;

  void* operator new(std::size_t sz) { return pool.alloc(sz); }
  void operator delete(void *p) { pool.free(p); }

  template <class P>
  void* operator new(std::size_t sz, P &pool) { return pool.alloc(sz); }
  
  template <class P>
  void operator delete(void *p, P &pool) { pool.free(p); }
};

//---------------------------------------------------------------------------------------------

template<typename Pool> Pool MemPoolObject<Pool>::pool;

//---------------------------------------------------------------------------------------------

#endif // __cplusplus

///////////////////////////////////////////////////////////////////////////////////////////////
