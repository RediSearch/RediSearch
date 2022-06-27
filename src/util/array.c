#include "array.h"
#include "rmalloc.h"
#include "util/minmax.h"

static ArrayAllocProcs libcAllocProcs_g = {
    .Alloc = malloc, .Realloc = realloc, .Free = free
  };

static ArrayAllocProcs rmAllocProcs_g = {
    .Alloc = rm_malloc, .Realloc = rm_realloc, .Free = rm_free
  };

void Array::ctor(ArrayAllocatorType allocType) {
  capacity = 0;
  len = 0;
  data = NULL;
  if (allocType == ArrayAlloc_LibC) {
    procs = &libcAllocProcs_g;
  } else {
    procs = &rmAllocProcs_g;
  }
}

Array::~Array() {
  procs->Free(data);
  capacity = 0;
  len = 0;
  data = NULL;
}

//---------------------------------------------------------------------------------------------

int Array::Resize(uint32_t newSize) {
  uint32_t newCapacity = capacity ? capacity : 16;
  while (newCapacity - len < newSize) {
    newCapacity *= 2;
    if (newCapacity < capacity) {
      return -1;
    }
  }
  newCapacity = Max(newCapacity, 16);
  if ((data = procs->Realloc(data, newCapacity)) == NULL) {
    return -1;
  }
  capacity = newCapacity;
  len = newSize;
  return 0;
}

//---------------------------------------------------------------------------------------------

/**
 * Add item to the array
 * elemSize is the size of the new item.
 * Returns a pointer to the newly added item. The memory is allocated but uninitialized
 */

void *Array::Add(uint32_t toAdd) {
  uint32_t oldLen = len;
  if (capacity - len < toAdd) {
    if (Resize(len + toAdd) != 0) {
      return NULL;
    }
  } else {
    len += toAdd;
  }

  return data + oldLen;
}

//---------------------------------------------------------------------------------------------

void Array::Write(const T *data, size_t len) {
  void *ptr = Add(len);
  memcpy(ptr, data, len);
}

//---------------------------------------------------------------------------------------------

/**
 * Shrink the array down to size, so that any preemptive allocations are removed.
 * This should be used when no more elements will be added to the array.
 */

void Array::ShrinkToSize() {
  if (capacity > len) {
    capacity = len;
    data = procs->Realloc(data, capacity);
  }
}

//---------------------------------------------------------------------------------------------

/**
 * "Steal" the contents of the array. The caller now owns its contents.
 */

inline char *Array::Steal(size_t *len_) {
  *len_ = len;
  char *ret = data;
  data = NULL;
  len = 0;
  capacity = 0;
  return ret;
}
