/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/* Defines the `RedisModule_*` API function-pointer table.
 *
 * Every file that includes `redismodule.h` gets `extern` declarations of the API
 * pointers, except the one that defines `REDISMODULE_MAIN` before including it:
 * there the declarations become the definitions. This file exists to be that one,
 * and to hold nothing else.
 *
 * The reason is linking, not style. Rust test and benchmark binaries link
 * `libredisearch_c_bundle.a` and reference these pointers (the `redis-module`
 * crate's bindings do), so the linker pulls in whichever archive member defines
 * them.
 * While that member was `module.c.o` it also dragged in the module's whole
 * command-dispatch layer, and with it every Rust FFI symbol that layer calls back
 * into -- symbols a single-crate test binary does not link, so the link failed on
 * a wall of undefined `QueryError_*` and `RLookup_*`. This file references nothing
 * else in RediSearch, so pulling it in costs nothing. (Sanitizer and coverage
 * builds add references to their own runtimes, which those builds always link.)
 *
 * Note the final artifacts hold more than one copy of the table: the `redis-module`
 * crate compiles its own from the header it vendors, which lacks the
 * `REDISMODULE_MAIN` gate and so defines the table in every file that includes it.
 * The copies merge rather than collide because both sides emit *common* symbols --
 * `redismodule.h` tags every pointer with `REDISMODULE_ATTR_COMMON`
 * (`__attribute__((__common__))`, on GNU-compatible C compilers) and the vendored
 * header does the same. Should both sides start emitting ordinary definitions, every
 * pointer the two headers have in common becomes a duplicate-definition error.
 */

#define REDISMODULE_MAIN
#include "redismodule.h"
