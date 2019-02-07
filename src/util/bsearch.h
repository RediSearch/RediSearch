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
static inline size_t rsb_ge(const void *arr, size_t narr, size_t elemsz, size_t begin, size_t end,
                            const void *s, rsbcompare cmp) {

  while (begin < end) {
    size_t cur = (begin + end) / 2;
    size_t tmpidx = cur * elemsz;
    const void *p = ((const char *)arr) + tmpidx;
    int rc = cmp(s, p);
    if (rc == 0) {
      // Matches!
      return cur;
    } else if (rc < 0) {
      end = cur;
    } else {
      begin = cur + 1;
    }
  }
  assert(begin == end);
  return begin;
}

#ifdef __cplusplus
}
#endif
#endif