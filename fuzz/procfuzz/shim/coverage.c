/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// SanitizerCoverage runtime for procfuzz: link it, uninstrumented, into the
// binary built with -fsanitize-coverage=trace-pc-guard. Edges go straight
// into the shared-memory segment the fuzzer names in PROCFUZZ_COVERAGE_SHM, so
// edges reached by a forked child or just before a crash are kept.
// `src/coverage.rs` reads the same layout.

#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define NOCOV __attribute__((no_sanitize("coverage")))

typedef struct {
  uint64_t capacity;
  uint64_t guards;
  uint8_t edges[];
} CoverageMap;

static CoverageMap *coverage_map;
static uint32_t coverage_next_guard = 1;

NOCOV static void coverage_attach(void) {
  const char *name = getenv("PROCFUZZ_COVERAGE_SHM");
  int fd = name ? shm_open(name, O_RDWR, 0) : -1;
  struct stat st;
  if (fd < 0 || fstat(fd, &st) != 0) {
    return;
  }
  void *mapped = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (mapped != MAP_FAILED) {
    coverage_map = mapped;
  }
}

NOCOV void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop) {
  if (start == stop || *start) {
    return;
  }
  coverage_attach();
  for (uint32_t *guard = start; guard < stop; guard++) {
    *guard = coverage_next_guard++;
  }
  if (coverage_map) {
    coverage_map->guards = coverage_next_guard - 1;
  }
}

NOCOV void __sanitizer_cov_trace_pc_guard(uint32_t *guard) {
  if (coverage_map && *guard < coverage_map->capacity) {
    __atomic_store_n(&coverage_map->edges[*guard], 1, __ATOMIC_RELAXED);
  }
}
