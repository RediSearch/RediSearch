
#include "util/heap.h"
#include "rmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define DEFAULT_CAPACITY 13

//---------------------------------------------------------------------------------------------

template <class T>
int Heap<T>::__child_left(const int idx) {
  return idx * 2 + 1;
}

template <class T>
int __child_right(const int idx) {
  return idx * 2 + 2;
}

template <class T>
int __parent(const int idx) {
  return (idx - 1) / 2;
}

//---------------------------------------------------------------------------------------------

/**
 * Create new heap and initialise it.
 *
 * malloc()s space for heap.
 *
 * @param[in] cmp Callback used to get an item's priority
 * @param[in] udata User data passed through to cmp callback
 */

template <class T>
Heap::Heap(int (*cmp)(const void *, const void *, const void *udata), const void *udata) {
  reserve(DEFAULT_CAPACITY);
  cmp = cmp;
  udata = udata;
}

//---------------------------------------------------------------------------------------------

/**
 * @return a new heap on success; NULL otherwise */

template <class T>
void Heap::__ensurecapacity() {
  if (size() < capacity()) return;
  resize(size() * 2);
}

//---------------------------------------------------------------------------------------------

template <class T>
void Heap::__swap(const int i1, const int i2) {
  void *tmp = array[i1];
  array[i1] = array[i2];
  array[i2] = tmp;
}

//---------------------------------------------------------------------------------------------

template <class T>
int Heap::__pushup(unsigned int idx) {
  /* 0 is the root node */
  while (0 != idx) {
    int parent = __parent(idx);

    // we are smaller than the parent
    if (cmp(array[idx], array[parent], udata) < 0)
      return -1;
    else
      __swap(idx, parent);

    idx = parent;
  }

  return idx;
}

//---------------------------------------------------------------------------------------------

template <class T>
void Heap::__pushdown(unsigned int idx) {
  while (1) {
    unsigned int childl, childr, child;

    childl = __child_left(idx);
    childr = __child_right(idx);

    if (childr >= count) {
      // can't pushdown any further
      if (childl >= count) return;

      child = childl;
    }
    // find biggest child
    else if (cmp(array[childl], array[childr], udata) < 0)
      child = childr;
    else
      child = childl;

    // idx is smaller than child
    if (cmp(array[idx], array[child], udata) < 0) {
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
void Heap::__offerx(T *item) {
  array[count] = item;

  // ensure heap properties
  __pushup(count++);
}

//---------------------------------------------------------------------------------------------

template <class T>
int Heap::offerx(void *item) {
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
int Heap::offer(void *item) {
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
T *Heap::poll() {
  if (empty()) return NULL;

  T *item = at(0);

  array[0] = array[count - 1];
  count--;

  if (count > 1) __pushdown(0);

  return item;
}

//---------------------------------------------------------------------------------------------

/**
 * @return top item of the heap */

template <class T>
T *Heap::peek() const {
  if (empty()) return NULL;
  return at(0);
}

//---------------------------------------------------------------------------------------------

#if 0

/**
 * Clear all items
 *
 * NOTE:
 *  Does not free items.
 *  Only use if item memory is managed outside of heap */

template <class T>
void Heap::clear() {
  count = 0;
}

#endif // 0

//---------------------------------------------------------------------------------------------

/**
 * @return item's index on the heap's array; otherwise -1 */

template <class T>
int Heap::__item_get_idx(const T *item) const {
  unsigned int idx;

  for (idx = 0; idx < count; idx++)
    if (0 == cmp(array[idx], item, udata)) return idx;

  return -1;
}

//---------------------------------------------------------------------------------------------

/**
 * Remove item
 *
 * @param[in] item The item that is to be removed
 * @return item to be removed; NULL if item does not exist */

template <class T>
T *Heap::remove_item(const T *item) {
  int idx = __item_get_idx(item);

  if (idx == -1) return NULL;

  // swap the item we found with the last item on the heap
  T *ret_item = array[idx];
  array[idx] = array[count - 1];
  array[count - 1] = NULL;

  count -= 1;

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
bool Heap::contains_item(const T *item) const {
  return __item_get_idx(item) != -1;
}

///////////////////////////////////////////////////////////////////////////////////////////////
