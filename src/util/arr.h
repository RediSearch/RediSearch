#ifndef UTIL_ARR_H_
#define UTIL_ARR_H_

#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <stdio.h>

typedef struct {
  uint32_t len;
  uint32_t cap;
  uint32_t elem_sz;
  char buf[];
} array_hdr_t;

#define INITIAL_CAP ;
typedef void *array_t;

#define array_sizeof(hdr) (sizeof(array_hdr_t) + hdr->cap * hdr->elem_sz)
#define array_bytelen(hdr) (hdr->len * hdr->elem_sz)

#define array_bytecap(hdr) (hdr->cap * hdr->elem_sz)

#define array_hdr(arr) ((array_hdr_t *)(((char *)arr) - sizeof(array_hdr_t)))
#define array_elemptr(arr, idx) (((char *)arr) + (array_hdr(arr)->elem_sz * idx))

static array_t array_new_sz(size_t elem_sz, size_t cap) {
  array_hdr_t *hdr = calloc(1, sizeof(array_hdr_t) + cap * elem_sz);
  hdr->cap = cap;
  hdr->elem_sz = elem_sz;
  hdr->len = 0;
  return (array_t)(hdr->buf);
}

#define array_new(T, cap) (array_new_sz(sizeof(T), cap))

static inline array_t array_ensure_cap(array_t arr, size_t cap) {
  array_hdr_t *hdr = array_hdr(arr);
  if (cap > hdr->cap) {
    hdr->cap = MAX(hdr->cap * 2, cap);
    hdr = realloc(hdr, array_sizeof(hdr));
  }
  return (array_t)hdr->buf;
}

/* Ensure capacity for the array to grow by one */
static inline array_t array_grow(array_t arr) {
  return array_ensure_cap(arr, ++array_hdr(arr)->len);
}

static array_t array_push(array_t arr, void *ptr) {
  array_hdr_t *hdr = array_hdr(arr);
  if (hdr->len + 1 >= hdr->cap) {
    hdr->cap += MIN(hdr->cap, 1024);
    hdr = realloc(hdr, array_sizeof(hdr));
  }
  memcpy(hdr->buf + array_bytelen(hdr), ptr, hdr->elem_sz);
  hdr->len += 1;

  return (array_t)hdr->buf;
}

#define array_append(arr, x)               \
  {                                        \
    *arr = array_grow(*arr);               \
    (*arr)[array_hdr(arr)->len - 1] = (x); \
  }

static void *array_get(array_t arr, size_t idx) {
  return (void *)array_elemptr(arr, idx);
}

static void array_set(array_t arr, size_t idx, void *ptr) {
  memcpy(array_elemptr(arr, idx), ptr, array_hdr(arr)->elem_sz);
}

static size_t array_len(array_t arr) {
  return array_hdr(arr)->len;
}

static void array_free(array_t arr, void (*free_cb)(void *)) {
  if (free_cb) {
    for (size_t i = 0; i < array_len(arr); i++) {
      free_cb(array_elemptr(arr, i));
    }
  }
  free(array_hdr(arr));
}

  // #define ARRAY_T(T, TNAME)                             \
//   typedef T *TNAME##_t;                               \
//   static TNAME##_t TNAME##_new(size_t cap) {          \
//     return array_new(sizeof(T), cap);                 \
//   }                                                   \
//                                                       \
//   static TNAME##_t TNAME##_push(TNAME##_t arr, T x) { \
//     return array_push(arr, (void *)&x);               \
//   }

  // ARRAY_T(int, int_array);
  // ARRAY_T(double, double_array);
  // ARRAY_T(char *, str_array);

#ifdef WITHARRAY_MAIN
#include <stdio.h>

#pragma pack(1)
typedef struct {
  int x;
  double y;
} foo;
#pragma pack()
int main(int argc, char **argv) {

  foo *arr = array_new(foo, 8);
  foo f;
  for (int i = 0; i < 100; i++) {
    // printf("%p <> %p\n", array_elemptr(arr, i), arr + i);
    // f.x = i;
    // arr = array_push(arr, &f);
    // arr[i].x = i * 2;
    arr = array_ensure_cap(arr, i);
    arr[i].x = i * 2;
  }

  for (int i = 0; i < 100; i++) {
    printf("%d %zd\n", arr[i].x, array_len(arr));
  }
  // array_free(arr, NULL);
  return 0;
}
#endif
#endif
