/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <stdarg.h>

/* Fixed-arity implementation, defined in Rust (redis_mock::key). */
int RedisMock_HashGetFixed(void *key, int flags, void *field, void **value, const void *terminator);

/* Variadic entry point matching `RedisModule_HashGet(key, flags, field, &value, NULL)`
 * for a single field. See build.rs for why this lives in C. */
int RedisMock_HashGet(void *key, int flags, ...) {
  va_list ap;
  va_start(ap, flags);
  void *field = va_arg(ap, void *);
  void **value = va_arg(ap, void **);
  const void *terminator = va_arg(ap, const void *);
  va_end(ap);
  return RedisMock_HashGetFixed(key, flags, field, value, terminator);
}
