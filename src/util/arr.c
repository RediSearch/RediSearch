#include "arr.h"

void array_debug(void *pp) {
  const array_hdr_t *hdr = array_hdr(pp);
  printf("Array: %p, hdr@%p", pp, hdr);
  printf("Len: %u. Cap: %u. ElemSize: %u\n", hdr->len, hdr->cap, hdr->elem_sz);
}
