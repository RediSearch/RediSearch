
#include "fnv.h"
#include <stdint.h>
#include <stdlib.h>

/*
 * fnv_32a_buf - perform a 32 bit Fowler/Noll/Vo FNV-1a hash on a buffer
 *
 * input:
 *	buf	- start of buffer to hash
 *	len	- length of buffer in octets
 *	hval	- previous hash value or 0 if first call
 *
 * returns:
 *	32 bit hash as a static hash type
 *
 * NOTE: To use the recommended 32 bit FNV-1a hash, use FNV1_32A_INIT as the
 * 	 hval arg on the first call to either fnv_32a_buf() or fnv_32a_str().
 */
uint32_t rs_fnv_32a_buf(const void *buf, size_t len, uint32_t hval) {
  unsigned char *bp = (unsigned char *)buf; /* start of buffer */
  unsigned char *be = bp + len;             /* beyond end of buffer */

  /*
   * FNV-1a hash each octet in the buffer
   */
  while (bp < be) {
    /* xor the bottom with the current octet */
    hval ^= (uint32_t)*bp++;

/* multiply by the 32 bit FNV magic prime mod 2^32 */
#if defined(NO_FNV_GCC_OPTIMIZATION)
    hval *= FNV_32_PRIME;
#else
    hval += (hval << 1) + (hval << 4) + (hval << 7) + (hval << 8) + (hval << 24);
#endif
  }

  /* return our new hash value */
  return hval;
}

/*
 * 64 bit magic FNV-1a prime
 */
#define FNV_64_PRIME ((uint64_t)0x100000001b3ULL)

/*
 * fnv_64a_buf - perform a 64 bit Fowler/Noll/Vo FNV-1a hash on a buffer
 *
 * input:
 *	buf	- start of buffer to hash
 *	len	- length of buffer in octets
 *	hval	- previous hash value or 0 if first call
 *
 * returns:
 *	64 bit hash as a static hash type
 *
 * NOTE: To use the recommended 64 bit FNV-1a hash, use FNV1A_64_INIT as the
 * 	 hval arg on the first call to either fnv_64a_buf() or fnv_64a_str().
 */
uint64_t fnv_64a_buf(const void *buf, size_t len, uint64_t hval) {
  unsigned char *bp = (unsigned char *)buf; /* start of buffer */
  unsigned char *be = bp + len;             /* beyond end of buffer */

  /*
   * FNV-1a hash each octet of the buffer
   */
  while (bp < be) {

    /* xor the bottom with the current octet */
    hval ^= (uint64_t)*bp++;

    /* multiply by the 64 bit FNV magic prime mod 2^64 */
#if defined(NO_FNV_GCC_OPTIMIZATION)
    hval *= FNV_64_PRIME;
#else  /* NO_FNV_GCC_OPTIMIZATION */
    hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40);
#endif /* NO_FNV_GCC_OPTIMIZATION */
  }

  /* return our new hash value */
  return hval;
}
