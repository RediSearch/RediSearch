#include "stdint.h"
#include "stddef.h"
#include "rmalloc.h"
uint64_t allocated = 0;
uint64_t alloc_count = 0;
size_t allocation_header_size = sizeof(size_t);
size_t getPointerAllocationSize(void *p) { return *(((size_t *)p) - 1); }
