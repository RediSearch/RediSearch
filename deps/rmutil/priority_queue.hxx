#include "heap.h"

template<typename T, typename Compare>
void PriorityQueue<T, Compare>::_sift_up(size_t first, size_t last) {
  size_t len = last - first;
  if (len > 1) {
    len = (len - 2) / 2;
    size_t ptr = first + len;
    if (Compare(at(ptr), at(--last)) < 0) {
      T t = at(last);
      do {
        at(last) = at(ptr);
        last = ptr;
        if (len == 0) break;
        len = (len - 1) / 2;
        ptr = first + len;
      } while (Compare(at(ptr), t) < 0);
      at(last) = t;
    }
  }
}

template<typename T, typename Compare>
void PriorityQueue<T, Compare>::_sift_down(size_t first, size_t last, size_t start) {
  size_t len = last - first;
  size_t child = start - first;

  if (len < 2 || (len - 2) / 2 < child) return;

  child = 2 * child + 1;

  if ((child + 1) < len &&
      Compare(at(first + child), at(first + child + 1)) < 0) {
    // right-child exists and is greater than left-child
    ++child;
  }

  // check if we are in heap-order
  if (Compare(at(first + child), at(start)) < 0)
    // we are, __start is larger than it's largest child
    return;

  T top = at(start);
  do {
    // we are not in heap-order, swap the parent with it's largest child
    at(start) = at(first + child);
    start = first + child;
    if ((len - 2) / 2 < child) break;

    // recompute the child based off of the updated parent
    child = 2 * child + 1;

    if ((child + 1) < len &&
        Compare(at(first + child), at(first + child + 1)) < 0) {
      // right-child exists and is greater than left-child
      ++child;
    }

    // check if we are in heap-order
  } while (Compare(at(first + child), top) >= 0);
  at(start) = top;
}

template<typename T, typename Compare>
void PriorityQueue<T, Compare>::_pop(size_t first, size_t last) {
  if (last - first > 1) {
    std::swap(at(first), at(--last));
    _sift_down(first, last, first);
  }
}
