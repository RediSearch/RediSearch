#include "array.h"
#include "rmalloc.h"
#include "util/minmax.h"

void Array_Init(Array *array) {
  array->capacity = 0;
  array->len = 0;
  array->data = NULL;
}

void Array_Free(Array *array) {
  rm_free(array->data);
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
  if ((array->data = rm_realloc(array->data, newCapacity)) == NULL) {
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

void Array_ShrinkToSize(Array *array) {
  if (array->capacity > array->len) {
    array->capacity = array->len;
    array->data = rm_realloc(array->data, array->capacity);
  }
}