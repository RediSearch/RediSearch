#ifndef __RS_MEMPOOL_H__
#define __RS_MEMPOOL_H__

/* Mempool - an uber simple, thread-unsafe, memory pool */
#include <stdint.h>
#include <stdlib.h>

/* stateless allocation function for the pool */
typedef void *(*mempool_alloc_fn)();
/* free function for the pool */
typedef void (*mempool_free_fn)(void *);

/* mempool - the struct holding the memory pool */
#ifndef _RS_MEMPOOL_C_
typedef struct mempool_t mempool_t;
#else
struct mempool_t;
#endif
/* Create a new memory pool */
struct mempool_t *mempool_new(size_t cap, mempool_alloc_fn alloc, mempool_free_fn free);

/* Get an entry from the pool, allocating a new instance if unavailable */
void *mempool_get(struct mempool_t *p);

/* Release an allocated instance to the pool */
void mempool_release(struct mempool_t *p, void *ptr);

/* destroy the pool, releasing all entries in it and destroying its internal array */
void mempool_destroy(struct mempool_t *p);
#endif