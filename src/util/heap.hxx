
#include "util/heap.h"
#include "rmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define DEFAULT_CAPACITY 13

//---------------------------------------------------------------------------------------------

static int __child_left(const int idx) {
  return idx * 2 + 1;
}

static int __child_right(const int idx) {
  return idx * 2 + 2;
}

static int __parent(const int idx) {
  return (idx - 1) / 2;
}

//---------------------------------------------------------------------------------------------

/**
 * Create new heap and initialise it.
 *
 *
 * @param[in] cmp Callback used to get an item's priority
 * @param[in] udata User data passed through to cmp callback
 */

template <class T>
Heap<T>::Heap(int (*cmp)(const void *, const void *, const void *udata), const void *udata) :
    cmp(cmp), udata(udata) {
  reserve(DEFAULT_CAPACITY);
}

//---------------------------------------------------------------------------------------------

/**
 * @return a new heap on success; nullptr otherwise */

template <class T>
void Heap<T>::__ensurecapacity() {
  if (size() < capacity()) return;
  resize(size() * 2);
}

//---------------------------------------------------------------------------------------------

template <class T>
void Heap<T>::__swap(const int i1, const int i2) {
  std::swap(_at(i1), _at(i2));
}

//---------------------------------------------------------------------------------------------

template <class T>
int Heap<T>::__pushup(unsigned int idx) {
  /* 0 is the root node */
  while (0 != idx) {
    int parent = __parent(idx);

    // we are smaller than the parent
    if (cmp(_at(idx), _at(parent), udata) < 0)
      return -1;
    else
      __swap(idx, parent);

    idx = parent;
  }

  return idx;
}

//---------------------------------------------------------------------------------------------

template <class T>
void Heap<T>::__pushdown(unsigned int idx) {
  while (1) {
    unsigned int childl, childr, child;

    childl = __child_left(idx);
    childr = __child_right(idx);

    if (childr >= size()) {
      // can't pushdown any further
      if (childl >= size()) return;

      child = childl;
    }
    // find biggest child
    else if (cmp(_at(childl), _at(childr), udata) < 0)
      child = childr;
    else
      child = childl;

    // idx is smaller than child
    if (cmp(_at(idx), _at(child), udata) < 0) {
      __swap(idx, child);
      idx = child;
      // bigger than the biggest child, we stop, we win
    } else
      return;
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Add item
 *
 * An error will occur if there isn't enough space for this item.
 *
 * NOTE:
 *  no malloc()s called.
 *
 * @param[in] item The item to be added
 * @return 0 on success; -1 on error */

template <class T>
void Heap<T>::__offerx(T item) {
  _at(size()) = item;

  // ensure heap properties
  __pushup(size()+1);
}

//---------------------------------------------------------------------------------------------

template <class T>
int Heap<T>::offerx(T item) {
  if (size() == capacity()) return -1;
  __offerx(item);
  return 0;
}

//---------------------------------------------------------------------------------------------

/**
 * Add item
 *
 * Ensures that the data structure can hold the item.
 *
 * NOTE:
 *  realloc() possibly called.
 *  The heap pointer will be changed if the heap needs to be enlarged.
 *
 * @param[in/out] hp_ptr Pointer to the heap. Changed when heap is enlarged.
 * @param[in] item The item to be added
 * @return 0 on success; -1 on failure */

template <class T>
int Heap<T>::offer(T item) {
  __ensurecapacity();
  __offerx(item);
  return 0;
}

//---------------------------------------------------------------------------------------------

/**
 * Remove the item with the top priority
 *
 * @return top item */

template <class T>
T Heap<T>::poll() {
  if (empty()) return nullptr;

  T item = at(0);

  front() = back();
  pop_back();

  if (size() > 1) __pushdown(0);

  return item;
}

//---------------------------------------------------------------------------------------------

/**
 * @return top item of the heap */

template <class T>
T Heap<T>::peek() const {
  if (empty()) return nullptr;
  return at(0);
}

//---------------------------------------------------------------------------------------------

/**
 * @return item's index on the heap's array; otherwise -1 */

template <class T>
int Heap<T>::__item_get_idx(const T item) const {
  unsigned int idx;

  for (idx = 0; idx < size(); idx++)
    if (0 == cmp((_at(idx), item, udata))) return idx;

  return -1;
}

//---------------------------------------------------------------------------------------------

/**
 * Remove item
 *
 * @param[in] item The item that is to be removed
 * @return item to be removed; nullptr if item does not exist */

template <class T>
T Heap<T>::remove_item(const T item) {
  int idx = __item_get_idx(item);

  if (idx == -1) return nullptr;

  // swap the item we found with the last item on the heap
  T ret_item = _at(idx);
  _at(idx) = back();
  back() = nullptr;

  pop_back();

  // ensure heap property
  __pushup(idx);

  return ret_item;
}

//---------------------------------------------------------------------------------------------

/**
 * Test membership of item
 *
 * @param[in] item The item to test
 * @return 1 if the heap contains this item; otherwise 0 */

template <class T>
bool Heap<T>::contains_item(const T item) const {
  return __item_get_idx(item) != -1;
}

///////////////////////////////////////////////////////////////////////////////////////////////
