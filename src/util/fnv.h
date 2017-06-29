#ifndef __FT_FNV_H__
#define __FT_FNV_H__

#include <stdint.h>
#include <stdlib.h>

#define FNV_32_PRIME ((Fnv32_t)0x01000193)

uint32_t fnv_32a_buf(void *buf, size_t len, uint32_t hval);

#endif