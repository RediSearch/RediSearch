#ifndef UTIL_ARR_H_
#define UTIL_ARR_H_
/* arr.h - simple, easy to use dynamic array with fat pointers,
 * to allow native access to members. It can accept pointers, struct literals and scalars.
 *
 * Example usage:
 *
 *  int *arr =array_new(int, 8);
 *  // Add elements to the array
 *  for (int i = 0; i < 100; i++) {
 *   arr = array_append(arr, i);
 *  }
 *
 *  // read individual alements
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

typedef struct {
  uint32_t len;
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
#define array_elem(arr, idx) ((void **)((char *)arr + (idx * array_hdr(arr)->elem_sz)))

/* Initialize a new array with a given element size and capacity. Should not be used directly - use
 * array_new instead */
static array_t array_new_sz(uint32_t elem_sz, uint32_t cap) {
  array_hdr_t *hdr = malloc(sizeof(array_hdr_t) + cap * elem_sz);
  hdr->cap = cap;
  hdr->elem_sz = elem_sz;
  hdr->len = 0;
  return (array_t)(hdr->buf);
}

/* Initialize an array for a given type T with a given capacity. The array should be case to a
 * pointer to that type. e.g.
 *
 *  int *arr = array_new(int, 4);
 *
 * This allows direct access to elements
 *  */
#define array_new(T, cap) (array_new_sz(sizeof(T), cap))

static inline array_t array_ensure_cap(array_t arr, uint32_t cap) {
  array_hdr_t *hdr = array_hdr(arr);
  if (cap > hdr->cap) {
    hdr->cap = MAX(MIN(hdr->cap * 2, hdr->cap + 1024), cap);
    hdr = realloc(hdr, array_sizeof(hdr));
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
  return array_hdr(arr)->len;
}

/* Free the array, optionally freeing individual elements with free_cb */
static void array_free(array_t arr, void (*free_cb)(void *)) {
  if (free_cb) {
    for (uint32_t i = 0; i < array_len(arr); i++) {
      free_cb(*array_elem(arr, i));
    }
  }
  free(array_hdr(arr));
}

#endif
