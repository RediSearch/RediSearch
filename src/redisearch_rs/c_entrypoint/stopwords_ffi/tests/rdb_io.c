/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

struct TestRdb {
  uint64_t count;
  int error;
  char level[16];
  char warning[128];
};

uint64_t StopwordsTest_LoadUnsigned(struct TestRdb* io) {
  return io->count;
}

char* StopwordsTest_LoadStringBuffer(struct TestRdb* io, size_t* len) {
  *len = 0;
  io->error = 1;
  return NULL;
}

int StopwordsTest_IsIOError(struct TestRdb* io) {
  return io->error;
}

/* Stable Rust cannot define the Redis API's variadic logging callback. */
void StopwordsTest_LogIOError(struct TestRdb* io, const char* level, const char* fmt, ...) {
  snprintf(io->level, sizeof(io->level), "%s", level);
  va_list args;
  va_start(args, fmt);
  vsnprintf(io->warning, sizeof(io->warning), fmt, args);
  va_end(args);
}
