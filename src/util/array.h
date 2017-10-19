#ifndef ARRAY_H
#define ARRAY_H

#include <stdlib.h>
#include <stdint.h>

typedef struct {
  void *(*Alloc)(size_t);
  void *(*Realloc)(void *, size_t);
  void (*Free)(void *);
} ArrayAllocProcs;

/** Array datatype. Simple wrapper around a C array, with capacity and length. */
typedef struct Array {
  char *data;
  uint32_t len;
  uint32_t capacity;
  const ArrayAllocProcs *procs;
} Array;

typedef enum {
  ArrayAlloc_LibC,
  ArrayAlloc_RM,
  ArrayAlloc_Default = ArrayAlloc_RM
} ArrayAllocatorType;

void Array_InitEx(Array *array, ArrayAllocatorType allocType);

static inline void Array_Init(Array *array) {
  Array_InitEx(array, ArrayAlloc_Default);
}

/**
 * Free any memory allocated by this array.
 */
void Array_Free(Array *array);

/**
 * "Steal" the contents of the array. The caller now owns its contents.
 */
static inline char *Array_Steal(Array *array, size_t *len) {
  *len = array->len;
  char *ret = array->data;
  array->data = NULL;
  array->len = 0;
  array->capacity = 0;
  return ret;
}

/**
 * Add item to the array
 * elemSize is the size of the new item.
 * Returns a pointer to the newly added item. The memory is allocated but uninitialized
 */
void *Array_Add(Array *array, uint32_t elemSize);
void Array_Write(Array *array, const void *data, size_t len);
int Array_Resize(Array *array, uint32_t newSize);

/**
 * Shrink the array down to size, so that any preemptive allocations are removed.
 * This should be used when no more elements will be added to the array.
 */
void Array_ShrinkToSize(Array *array);

#define ARRAY_GETSIZE_AS(arr, T) ((arr)->len / (sizeof(T)))
#define ARRAY_GETARRAY_AS(arr, T) ((T)((arr)->data))
#define ARRAY_ADD_AS(arr, T) Array_Add(arr, sizeof(T))
#define ARRAY_GETITEM_AS(arr, ix, T) (ARRAY_GETARRAY_AS(arr, T) + ix)
#endif