#pragma once

#include <stdlib.h>

template<class T>
struct MinMaxHeap : Vector<T> {
  const void *udata; // user data
  int (*cmp)(const void *, const void *, const void *);

  MinMaxHeap(int (*cmp)(const void *, const void *, const void *udata), const void *udata) :
    cmp(cmp), udata(udata) {}

  void insert(T value);
  T pop_min();
  T pop_max();
  T peek_min() const;
  T peek_max() const;

  T &_at(size_t i) { return (*this)[i]; }
  const T &_at(size_t i) const { return (*this)[i]; }

protected:
  void _bubbleup_min(int i);
  void _bubbleup_max(int i);
  void _bubbleup(int i);
  void _swap(int i, int j);

  int _index_max_child_grandchild(int i);
  int _index_min_child_grandchild(int i);

  void _trickledown_max(int i);
  void _trickledown_min(int i);
  void _trickledown(int i);

  bool _gt(int x, int y) { return cmp((at(x), at(y), udata)) > 0; }
  bool _lt(int x, int y) { return cmp((at(x), at(y), udata)) < 0; }
  bool _max(int x, int y) { return _gt(x, y) ? at(x) :  at(y); }
};


