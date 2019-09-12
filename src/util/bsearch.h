#ifndef RSBSEARCH_H
#define RSBSEARCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <assert.h>

/**
 * Compare two elements; return <0, 0, or >0 if s is less than, equal to, or
 * greater than elem.
 *
 * `s` is the target to locate, and `elem` is an array element.
 */
typedef int (*rsbcompare)(const void *s, const void *elem);

/**
 * In order to locate a range between A and B, the proper indexes must be found.
 * The beginning index is going to be the first element which is >= A, and the
 * end index is going to be the first element which is >= B
 */

/**
 * Find the index of the first element in the sorted array which is greater than,
 * or equal to the provided item. The array must not have duplicate items.
 *
 * @param arr the array
 * @param narr the array to search for
 * @param elemsz element width/stride
 * @param begin the first element to search (usually 0)
 * @param end one after the last element to search (usually narr)
 * @param s the item to search for
 * @param cmp the comparison function
 * @return `end`
 */
static inline int rsb_gt(const void *arr, size_t narr, size_t elemsz, const void *s,
                         rsbcompare cmp) {
  size_t begin = 0;
  size_t end = narr - 1;

  while (begin < end) {
    size_t cur = (begin + end) / 2;
    size_t tmpidx = cur * elemsz;
    const void *p = ((const char *)arr) + tmpidx;
    int rc = cmp(s, p);
    if (rc == 0) {
      // Matches!
      begin = cur + 1;
    } else if (rc < 0) {
      end = cur;
    } else {
      begin = cur + 1;
    }
  }
  assert(begin == end);
  if (begin != narr - 1) {
    return begin;  // we found what we was looking for!
  }
  size_t tmpidx = begin * elemsz;
  const void *p = ((const char *)arr) + tmpidx;
  int rc = cmp(s, p);
  if (rc >= 0) {
    begin += 1;  // we could not find what we was looking for
  }
  return begin;
}

static inline int rsb_lt(const void *arr, size_t narr, size_t elemsz, const void *s,
                         rsbcompare cmp) {
  size_t begin = 0;
  size_t end = narr - 1;

  while (begin < end) {
    size_t cur = ((begin + end) / 2) + ((begin + end) % 2);
    size_t tmpidx = cur * elemsz;
    const void *p = ((const char *)arr) + tmpidx;
    int rc = cmp(s, p);
    if (rc == 0) {
      // Matches!
      end = cur - 1;
    } else if (rc < 0) {
      end = cur - 1;
    } else {
      begin = cur;
    }
  }
  assert(begin == end);
  if (begin != 0) {
    return begin;  // we found what we was looking for!
  }
  size_t tmpidx = begin * elemsz;
  const void *p = ((const char *)arr) + tmpidx;
  int rc = cmp(s, p);
  if (rc <= 0) {
    return begin -= 1;  // what we are looking for does not exists
  }
  return begin;
}

static inline int rsb_eq(const void *arr, size_t narr, size_t elemsz, const void *s,
                         rsbcompare cmp) {
  size_t begin = 0;
  size_t end = narr - 1;

  while (begin < end) {
    size_t cur = ((begin + end) / 2) + ((begin + end) % 2);
    size_t tmpidx = cur * elemsz;
    const void *p = ((const char *)arr) + tmpidx;
    int rc = cmp(s, p);
    if (rc == 0) {
      // Matches!
      return cur;
    } else if (rc < 0) {
      end = cur - 1;
    } else {
      begin = cur;
    }
  }
  assert(begin == end);
  size_t tmpidx = begin * elemsz;
  const void *p = ((const char *)arr) + tmpidx;
  return cmp(s, p) == 0 ? begin : -1;
}

#ifdef __cplusplus
}
#endif
#endif
