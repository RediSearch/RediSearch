#pragma once

#include "object.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <vector>
#include <list>

#if 1

template <class T>
class Vector : public std::vector<T, rm_allocator<T>> {
  typedef std::vector<T, rm_allocator<T>> Super;

public:
  Vector() {}
  Vector(size_t size) : Super(size) {}
  Vector(Super &&v) : Super(v) {}
};

template <class T>
class List : public std::list<T, rm_allocator<T>> {
  typedef std::list<T, rm_allocator<T>> Super;

public:
  List() {}
};

#else
	
/*
 * Generic resizable vector that can be used if you just want to store stuff
 * temporarily.
 * Works like C++ std::vector with an underlying resizable buffer
 */
template<class T = void>
struct Vector : public Object {
  T *data;
  size_t elemSize;
  size_t cap;
  size_t top;

  /* Create a new vector with element size. This should generally be used
  * internall by the NewVector macro */
  Vector(size_t cap_) {
    data = rm_calloc(cap_, T);
    top = 0;
    // elemSize = elemSize;
    cap = cap_;
  }

  /* free the vector and the underlying data. Does not release its elements if
  * they are pointers*/
  ~Vector() {
    rm_free(data);
  }

  /*
  * get the element at index pos. The value is copied in to ptr. If pos is outside
  * the vector capacity, we return 0
  * otherwise 1
  */
  bool Get(size_t pos, T *ptr) {
    // return 0 if pos is out of bounds
    if (pos >= top) {
      return false;
    }

  /* Get the element at the end of the vector, decreasing the size by one */
  virtual bool Pop(void *ptr) {
    if (top > 0) {
      if (ptr != NULL) {
        Get(top - 1, ptr);
      }
      top--;
      return true;
    }
    return false;
  }

    memcpy(ptr, data + (pos * elemSize), elemSize);
    return true;
  }

  /*
  * Put an element at pos.
  * Note: If pos is outside the vector capacity, we resize it accordingly
  */
  int Put(size_t pos, T *elem) { //@@ might break current behaviour: v[n] with n>cap
    // resize if pos is out of bounds
    if (pos >= cap) {
      Resize(pos + 1);
    }

    if (elem) {
      memcpy(data + pos * elemSize, elem, elemSize);
    } else {
      memset(data + pos * elemSize, 0, elemSize);
    }
    // move the end offset to pos if we grew
    if (pos >= top) {
      top = pos + 1;
    }
    return 1;
  }

  /* Push an element at the end of v, resizing it if needed. This macro wraps
  * PushPtr */
  virtual int Push(T *elem) {
    if (top == cap) {
      Resize(cap ? cap * 2 : 1);
    }

    Put(top, elem);
    return top;
  }

  size_t Resize(size_t newcap) {
    size_t oldcap = cap;
    cap = newcap;

    data = rm_realloc(data, cap * elemSize);

    // If we grew:
    // put all zeros at the newly realloc'd part of the vector
    if (newcap > oldcap) {
      size_t offset = oldcap * elemSize;
      memset(data + offset, 0, cap * elemSize - offset);
    }

    return cap;
  }

  size_t Cap() const { return cap; }
  size_t Size() const { return top; }
};

/* free the vector and the underlying data. Calls freeCB() for each non null element */
//@@ how should I change it
//@@ Also there is no implemetation for this
void Vector_FreeEx(Vector *v, void (*freeCB)(void *));

#endif // 0
