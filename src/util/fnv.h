#ifndef __FT_FNV_H__
#define __FT_FNV_H__

#include <sys/types.h>
#define FNV_32_PRIME ((Fnv32_t)0x01000193)

u_int32_t fnv_32a_buf(void *buf, size_t len, u_int32_t hval);

#endif