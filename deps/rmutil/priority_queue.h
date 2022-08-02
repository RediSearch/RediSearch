#pragma once

#include "vector.h"

/* Priority queue
 * Priority queues are designed such that its first element is always the greatest of the elements it contains.
 * This context is similar to a heap, where elements can be inserted at any moment, and only the max heap element can be
 * retrieved (the one at the top in the priority queue).
 * Priority queues are implemented as Vectors. Elements are popped from the "back" of Vector, which is known as the top
 * of the priority queue.
 */

template<typename T, typename Compare = std::less<T>>
struct PriorityQueue : public Vector<T> {
  PriorityQueue(size_t cap) {
    reserve(cap);
  }

  /* Access top element
  * Copy the top element in the priority_queue to ptr.
  * The top element is the element that compares higher in the priority_queue.
  */
  int Top(T *ptr) {
    return Get(0, ptr);
  }

  void Push(T *elem) {
    push_back(elem);
    _sift_up(0, size()-1);
  }

  /* Remove top element
  * Removes the element on top of the priority_queue, effectively reducing its size by one. The element removed is the
  * one with the highest value.
  * The value of this element can be retrieved before being popped by calling Priority_Queue_Top.
  */
  void Pop()  {
    _pop(0, size()-1);
  }

private:
  void _sift_up(size_t first, size_t last);
  void _pop(size_t first, size_t last);
  void _sift_down(size_t first, size_t last, size_t start);
};

#include "priority_queue.hxx"
