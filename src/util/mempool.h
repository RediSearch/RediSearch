#ifndef __RS_MEMPOOL_H__
#define __RS_MEMPOOL_H__

/* Mempool - an uber simple, thread-unsafe, memory pool */
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* stateless allocation function for the pool */
typedef void *(*mempool_alloc_fn)(void);
/* free function for the pool */
typedef void (*mempool_free_fn)(void *);

/* mempool - the struct holding the memory pool */
typedef struct mempool_t mempool_t;

typedef struct {
  mempool_alloc_fn alloc;
  mempool_free_fn free;
  size_t initialCap;  // Initial size of the pool
  size_t maxCap;      // maxmimum size of the pool

  /**
   * if true, will be added to the list of global mempool objects which
   * will be destroyed via mempool_free_global(). This also means you
   * cannot call mempool_destroy() on it manually
   */
  int isGlobal;
} mempool_options;

#define MEMPOOOL_STATIC_ALLOCATOR(name, sz) \
  void *name() {                            \
    return rm_malloc(sz);                   \
  }
/* Create a new memory pool */
mempool_t *mempool_new(const mempool_options *options);

/* Get an entry from the pool, allocating a new instance if unavailable */
void *mempool_get(struct mempool_t *p);

/* Release an allocated instance to the pool */
void mempool_release(struct mempool_t *p, void *ptr);

/* destroy the pool, releasing all entries in it and destroying its internal array */
void mempool_destroy(struct mempool_t *p);

/** Free all created memory pools */
void mempool_free_global(void);
#ifdef __cplusplus
}
#endif
#endif
