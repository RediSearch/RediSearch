#ifndef UTIL_ARR_H_
#define UTIL_ARR_H_
/* arr.h - simple, easy to use dynamic array with fat pointers,
 * to allow native access to members. It can accept pointers, struct literals and scalars.
 *
 * Example usage:
 *
 *  int *arr = array_new(int, 8);
 *  // Add elements to the array
 *  for (int i = 0; i < 100; i++) {
 *   arr = array_append(arr, i);
 *  }
 *
 *  // read individual elements
 *  for (int i = 0; i < array_len(arr); i++) {
 *    printf("%d\n", arr[i]);
 *  }
 *
 *  array_free(arr);
 *
 *
 *  */
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <stdarg.h>
#include <stdint.h>
#include <assert.h>

/* Definition of malloc & friedns that can be overridden before including arr.h.
 * Alternatively you can include arr_rm_alloc.h, which wraps arr.h and sets the allcoation functions
 * to those of the RM_ family
 */
#ifndef array_alloc_fn
#define array_alloc_fn malloc
#define array_realloc_fn realloc
#define array_free_fn free
#endif

typedef struct {
  uint32_t len;
  // TODO: optimize memory by making cap a 16-bit delta from len, and elem_sz 16 bit as well. This
  // makes the whole header fit in 64 bit
  uint32_t cap;
  uint32_t elem_sz;
  char buf[];
} array_hdr_t;

typedef void *array_t;
/* Internal - calculate the array size for allocations */
#define array_sizeof(hdr) (sizeof(array_hdr_t) + hdr->cap * hdr->elem_sz)
/* Internal - get a pointer to the array header */
#define array_hdr(arr) ((array_hdr_t *)(((char *)arr) - sizeof(array_hdr_t)))
/* Interanl - get a pointer to an element inside the array at a given index */
#define array_elem(arr, idx) (*((void **)((char *)arr + (idx * array_hdr(arr)->elem_sz))))

/* Initialize a new array with a given element size and capacity. Should not be used directly - use
 * array_new instead */
static array_t array_new_sz(uint32_t elem_sz, uint32_t cap, uint32_t len) {
  array_hdr_t *hdr = array_alloc_fn(sizeof(array_hdr_t) + cap * elem_sz);
  hdr->cap = cap;
  hdr->elem_sz = elem_sz;
  hdr->len = len;
  return (array_t)(hdr->buf);
}

/* Initialize an array for a given type T with a given capacity and zero length. The array should be
 * case to a pointer to that type. e.g.
 *
 *  int *arr = array_new(int, 4);
 *
 * This allows direct access to elements
 *  */
#define array_new(T, cap) (array_new_sz(sizeof(T), cap, 0))

/* Initialize an array for a given type T with a given length. The capacity allocated is identical
 * to the length
 *  */
#define array_newlen(T, len) (array_new_sz(sizeof(T), len, len))

static inline array_t array_ensure_cap(array_t arr, uint32_t cap) {
  array_hdr_t *hdr = array_hdr(arr);
  if (cap > hdr->cap) {
    hdr->cap = MAX(hdr->cap * 2, cap);
    hdr = array_realloc_fn(hdr, array_sizeof(hdr));
  }
  return (array_t)hdr->buf;
}

/* Ensure capacity for the array to grow by one */
static inline array_t array_grow(array_t arr) {
  return array_ensure_cap(arr, ++array_hdr(arr)->len);
}

/* get the last element in the array */
#define array_tail(arr) (arr[array_hdr(arr)->len - 1])

/* Append an element to the array, returning the array which may have been reallocated */
#define array_append(arr, x)   \
  ({                           \
    (arr) = array_grow((arr)); \
    array_tail((arr)) = (x);   \
    (arr);                     \
  })

/* Get the length of the array */
static inline uint32_t array_len(array_t arr) {
  return arr ? array_hdr(arr)->len : 0;
}

static inline void *array_trimm(array_t arr, uint32_t len, uint32_t cap) {
  array_hdr_t *arr_hdr = array_hdr(arr);
  assert(len >= 0 && "trimming len is negative");
  assert((cap == -1 || cap > 0 || len == cap) && "trimming capacity is illegal");
  assert((cap == -1 || cap >= len) && "trimming len is greater then capacity");
  assert((len <= arr_hdr->len) && "trimming len is greater then current len");
  arr_hdr->len = len;
  if (cap != -1) {
    arr_hdr->cap = cap;
    arr_hdr = array_realloc_fn(arr_hdr, array_sizeof(arr_hdr));
  }
  return arr_hdr->buf;
}

#define array_trimm_len(arr, len) array_trimm(arr, len, -1)
#define array_trimm_cap(arr, len) array_trimm(arr, len, len)

/* Free the array, without dealing with individual elements */
static void array_free(array_t arr) {
  array_free_fn(array_hdr(arr));
}

/* Repeate the code in "blk" for each element in the array, and give it the name of "as".
 * e.g:
 *  int *arr = array_new(int, 10);
 *  arr = array_append(arr, 1);
 *  array_foreach(arr, i, printf("%d\n", i));
 */
#define array_foreach(arr, as, blk)                 \
  ({                                                \
    for (uint32_t i = 0; i < array_len(arr); i++) { \
      typeof(*arr) as = arr[i];                     \
      blk;                                          \
    }                                               \
  })

/* Free the array, freeing individual elements with free_cb */
#define array_free_ex(arr, blk)                       \
  ({                                                  \
    if (arr) {                                        \
      for (uint32_t i = 0; i < array_len(arr); i++) { \
        void *ptr = &arr[i];                          \
        { blk; }                                      \
      }                                               \
      array_free(arr);                                \
    }                                                 \
  })

/* Pop the top element from the array, reduce the size and return it */
#define array_pop(arr)               \
  ({                                 \
    assert(array_hdr(arr)->len > 0); \
    arr[--(array_hdr(arr)->len)];    \
  })

#endif
