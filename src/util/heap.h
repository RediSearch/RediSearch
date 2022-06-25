#pragma once

#include "object.h"
#include "rmutil/vector.h"

#include <stdlib.h>

template <class T>
struct Heap : Vector<T> {
  typedef Vector<T> Super;

  const void *udata; // user data
  int (*cmp)(const void *, const void *, const void *);

  Heap(int (*cmp) (const void *, const void *, const void *udata), const void *udata);

  int offer(T *item);
  int offerx(T *item);
  T *poll();
  T *peek() const;
  //void clear();
  void *remove_item(const T *item);
  bool contains_item(const T *item) const;
};

#include "heap.hxx"
