#include "mempool.h"
#include <sys/param.h>
#include <stdio.h>
#include <pthread.h>
#include "rmalloc.h"
#include "config.h"

struct mempool_t {
  void **entries;
  size_t top;
  size_t cap;
  size_t max;  // max size for pool
  mempool_alloc_fn alloc;
  mempool_free_fn free;
  pthread_mutex_t lock;
};

static int mempoolDisable_g = -1;

struct {
  mempool_t **pools;
  size_t numPools;
} globalPools_g = {NULL};
pthread_mutex_t globalPools_lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;

static void mempool_append_to_global_pools(mempool_t *p) {
  pthread_mutex_lock(&globalPools_lock);
  globalPools_g.numPools++;
  globalPools_g.pools =
      rm_realloc(globalPools_g.pools, sizeof(*globalPools_g.pools) * globalPools_g.numPools);
  globalPools_g.pools[globalPools_g.numPools - 1] = p;
  pthread_mutex_unlock(&globalPools_lock);
}

mempool_t *mempool_new(const mempool_options *options) {
  mempool_t *p = rm_calloc(1, sizeof(*p));
  p->entries = rm_calloc(options->initialCap, sizeof(void *));
  p->alloc = options->alloc;
  p->free = options->free;
  p->cap = options->initialCap;
  p->max = options->maxCap;
  p->top = 0;
  p->lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
  if (mempoolDisable_g == -1) {
    if (getenv("REDISEARCH_NO_MEMPOOL")) {
      fprintf(stderr, "[redisearch]: REDISEARCH_NO_MEMPOOL in environment. Disabling\n");
      mempoolDisable_g = 1;
    } else {
      mempoolDisable_g = 0;
    }
  }
  if (mempoolDisable_g || RSGlobalConfig.noMemPool) {
    p->cap = 0;
    p->max = 0;
    rm_free(p->entries);
    p->entries = NULL;
  }
  return p;
}

void mempool_test_set_global(mempool_t **global_p, const mempool_options *options) {
    mempool_t *new_pool = mempool_new(options);
    mempool_t *uninitialized = NULL;

    if (__atomic_compare_exchange_n(global_p, &uninitialized, new_pool, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
      // If we set the global pool, we want to add it to the list of global pools to free later.
      mempool_append_to_global_pools(new_pool);
    } else {
      // Otherwise, the global pool was initialized while we created the pool, so we can destroy ours.
      mempool_destroy(new_pool);
    }
}

void *mempool_get(mempool_t *p) {
  void *ret = NULL;
  pthread_mutex_lock(&p->lock);
  if (p->top > 0) {
    ret = p->entries[--p->top];

  } else {
    ret = p->alloc();
  }
  pthread_mutex_unlock(&p->lock);
  return ret;
}

inline void mempool_release(mempool_t *p, void *ptr) {
  pthread_mutex_lock(&p->lock);

  if (p->entries == NULL || (p->max && p->max <= p->top)) {
    p->free(ptr);
  } else {
    if (p->top == p->cap) {
      // grow the pool
      p->cap += p->cap ? MIN(p->cap, 1024) : 1;
      p->entries = rm_realloc(p->entries, p->cap * sizeof(void *));
    }
    p->entries[p->top++] = ptr;
  }

  pthread_mutex_unlock(&p->lock);
}

// TODO: guyav-multi-threaded: make sure we don't call this on a pool that is in use.
// Assumes that the pool is not in use and its lock is not locked
void mempool_destroy(mempool_t *p) {
  for (size_t i = 0; i < p->top; i++) {
    p->free(p->entries[i]);
  }
  rm_free(p->entries);
  pthread_mutex_destroy(&p->lock);
  rm_free(p);
}

void mempool_free_global(void) {
  for (size_t ii = 0; ii < globalPools_g.numPools; ++ii) {
    mempool_destroy(globalPools_g.pools[ii]);
  }
  rm_free(globalPools_g.pools);
  globalPools_g.numPools = 0;
  pthread_mutex_destroy(&globalPools_lock);
}
