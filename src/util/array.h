#ifndef ARRAY_H
#define ARRAY_H

#include <stdlib.h>
#include <stdint.h>

struct ArrayAllocProcs {
  void *(*Alloc)(size_t);
  void *(*Realloc)(void *, size_t);
  void (*Free)(void *);
};

enum ArrayAllocatorType {
  ArrayAlloc_LibC,
  ArrayAlloc_RM,
  ArrayAlloc_Default = ArrayAlloc_RM
};

/** Array datatype. Simple wrapper around a C array, with capacity and length. */
template <class T> //@@ Is it correct to male it template (?)
struct Array {
  T *data;
  uint32_t len;
  uint32_t capacity;
  const ArrayAllocProcs *procs;

  void ctor(ArrayAllocatorType allocType)
  Array(ArrayAllocatorType allocType) { ctor(allocType); }
  Array() { ctor(ArrayAlloc_Default); }

  void *Add(uint32_t elemSize);
  void Write(const void *data, size_t len);

  void ShrinkToSize();
  int Resize(uint32_t newSize);

  //@@ need help in changing to funcs
  size_t ARRAY_GETSIZE_AS() { return len / (sizeof(T)); }
  // T ARRAY_GETARRAY_AS() { return data; }
  void *ARRAY_ADD_AS() { return Add(sizeof(T)); }
  T ARRAY_GETITEM_AS(ix) (data + ix)
};

#endif