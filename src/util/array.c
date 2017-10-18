#include "array.h"
#include "rmalloc.h"
#include "util/minmax.h"

static ArrayAllocProcs libcAllocProcs_g = {.Alloc = malloc, .Realloc = realloc, .Free = free};
static ArrayAllocProcs rmAllocProcs_g = {
    .Alloc = rm_malloc, .Realloc = rm_realloc, .Free = rm_free};

void Array_InitEx(Array *array, ArrayAllocatorType allocType) {
  array->capacity = 0;
  array->len = 0;
  array->data = NULL;
  if (allocType == ArrayAlloc_LibC) {
    array->procs = &libcAllocProcs_g;
  } else {
    array->procs = &rmAllocProcs_g;
  }
}

void Array_Free(Array *array) {
  array->procs->Free(array->data);
  array->capacity = 0;
  array->len = 0;
  array->data = NULL;
}

int Array_Resize(Array *array, uint32_t newSize) {
  uint32_t newCapacity = array->capacity ? array->capacity : 16;
  while (newCapacity - array->len < newSize) {
    newCapacity *= 2;
    if (newCapacity < array->capacity) {
      return -1;
    }
  }
  newCapacity = Max(newCapacity, 16);
  if ((array->data = array->procs->Realloc(array->data, newCapacity)) == NULL) {
    return -1;
  }
  array->capacity = newCapacity;
  array->len = newSize;
  return 0;
}

void *Array_Add(Array *array, uint32_t toAdd) {
  uint32_t oldLen = array->len;
  if (array->capacity - array->len < toAdd) {
    if (Array_Resize(array, array->len + toAdd) != 0) {
      return NULL;
    }
  } else {
    array->len += toAdd;
  }

  return array->data + oldLen;
}

void Array_Write(Array *arr, const void *data, size_t len) {
  void *ptr = Array_Add(arr, len);
  memcpy(ptr, data, len);
}

void Array_ShrinkToSize(Array *array) {
  if (array->capacity > array->len) {
    array->capacity = array->len;
    array->data = array->procs->Realloc(array->data, array->capacity);
  }
}