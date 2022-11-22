#pragma once

#include <stdlib.h>

static inline int log2_32(int value) { return 31 - __builtin_clz(value); }
template<class T>
using Comparator = std::function<int(const T&, const T&)>;

template<class T>
struct MinMaxHeap {
  MinMaxHeap(size_t reserve, Comparator<T> cmp) : heap_{reserve}, cmp_{cmp} {}

  void insert(T value);
  T pop_min();
  T pop_max();
  const T& peek_min() const;
  const T& peek_max() const;

  bool empty() const { return heap_.empty(); }
  size_t size() const { return heap_.size(); }
  size_t capacity() const { return heap_.capacity(); }
  int cmp(const T& a, const T& b) const { return cmp_(a, b); }
private:
  std::vector<T> heap_;
  Comparator<T> cmp_;

  bool is_min(int n) const { return (log2_32(n + 1) & 1) == 0; }
  int parent(int n) const { return (n - 1) / 2; }
  int first_child(int n) const { return (n * 2) + 1; }
  int second_child(int n) const { return (n * 2) + 2; }
  int index_max_child_grandchild(int i) const;
  int index_min_child_grandchild(int i) const;

  void bubbleup_min(int i);
  void bubbleup_max(int i);
  void bubbleup(int i);

  void trickledown_max(int i);
  void trickledown_min(int i);
  void trickledown(int i);

  void swap(int i, int j);
  T& at(int i) { return heap_[i]; }
  const T& at(int i) const { return heap_[i]; }
  bool gt(int x, int y) const { return cmp_(at(x), at(y)) > 0; }
  bool lt(int x, int y) const { return cmp_(at(x), at(y)) < 0; }
  const T& max(int x, int y) const { return gt(x, y) ? at(x) : at(y); }
};

#include "minmax_heap.hxx"
