/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// SanitizerCoverage runtime for the fuzzer, linked into redisearch.so only
// when built with FUZZ_COV. Edges are written straight into a shared-memory
// segment the fuzzer creates, so edges reached by a forked GC child or just
// before a crash are kept. The segment layout is mirrored by
// `fuzz/src/coverage.rs`.

#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define NOCOV __attribute__((no_sanitize("coverage"), no_sanitize("address")))

#define COVERAGE_MAGIC 0x52534656434f5631ULL
#define COVERAGE_SHM_ENV "REDISEARCH_COV_SHM"

typedef struct {
  uint64_t magic;
  uint64_t capacity;
  uint64_t guards;
  uint8_t edges[];
} CoverageMap;

static CoverageMap *coverage_map;
static uint32_t coverage_next_guard = 1;

NOCOV static void coverage_attach(void) {
  const char *name = getenv(COVERAGE_SHM_ENV);
  if (!name) {
    return;
  }
  int fd = shm_open(name, O_RDWR, 0);
  if (fd < 0) {
    return;
  }
  struct stat st;
  if (fstat(fd, &st) != 0 || (size_t)st.st_size < sizeof(CoverageMap)) {
    close(fd);
    return;
  }
  void *mapped = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (mapped == MAP_FAILED) {
    return;
  }
  CoverageMap *map = mapped;
  if (map->magic != COVERAGE_MAGIC) {
    munmap(mapped, (size_t)st.st_size);
    return;
  }
  coverage_map = map;
}

NOCOV void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop) {
  if (start == stop || *start) {
    return;
  }
  if (!coverage_map) {
    coverage_attach();
  }
  for (uint32_t *guard = start; guard < stop; guard++) {
    *guard = coverage_next_guard++;
  }
  if (coverage_map) {
    coverage_map->guards = coverage_next_guard - 1;
  }
}

NOCOV void __sanitizer_cov_trace_pc_guard(uint32_t *guard) {
  uint32_t id = *guard;
  if (coverage_map && id < coverage_map->capacity) {
    __atomic_store_n(&coverage_map->edges[id], 1, __ATOMIC_RELAXED);
  }
}
