#include <stdlib.h>

typedef struct {
    void *ptr;
    size_t size;
    size_t cap;

    size_t valsz;

} QuickHashTable;

#define FNV_32_PRIME ((Fnv32_t)0x01000193)

u_int32_t fnv_32a(void *buf, size_t len) {
    u_int32_t hval = 0;
    unsigned char *bp = (unsigned char *)buf; /* start of buffer */
    unsigned char *be = bp + len;             /* beyond end of buffer */

    /*
     * FNV-1a hash each octet in the buffer
     */
    while (bp < be) {
        /* xor the bottom with the current octet */
        hval ^= (u_int32_t)*bp++;

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

int QH_Put(char *key, size_t len, void *ptr) {}

int QH_Get(char *key, size_t len, void *ptr) {}