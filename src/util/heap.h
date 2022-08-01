#pragma once

#include "object.h"
#include "rmutil/vector.h"

#include <stdlib.h>

template <class T>
struct Heap : Vector<T> {
  typedef Vector<T> Super;

  const void *udata; // user data
  int (*cmp)(const void *, const void *, const void *);

  Heap(int (*cmp)(const void *, const void *, const void *udata), const void *udata);

  T &_at(size_t i) { return (*this)[i]; }
  const T &_at(size_t i) const { return (*this)[i]; }

  int offer(T item);
  int offerx(T item);
  T poll();
  T peek() const;
  //void clear();
  T remove_item(const T item);
  bool contains_item(const T item) const;

protected:
  void __ensurecapacity();
  void __swap(const int i1, const int i2);
  int __pushup(unsigned int idx);
  void __pushdown(unsigned int idx);
  void __offerx(T item);
  int __item_get_idx(const T item) const;
};

#include "heap.hxx"
