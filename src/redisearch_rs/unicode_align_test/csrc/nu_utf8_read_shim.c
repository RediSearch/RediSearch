/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/* libnu's `nu_utf8_read` is declared `static inline` in `utf8.h`, so it
 * has no externally callable symbol — it only inlines into whichever TU
 * happened to include the header. This tiny shim instantiates the inline
 * body inside a non-static wrapper so the Rust FFI layer in
 * `unicode_align_test` has a real symbol to bind against.
 *
 * Keep this wrapper a single delegation call: the test value is in
 * exercising libnu's decode bytes-to-codepoint logic, not anything we
 * could add here. */

#include "utf8.h"

const char *nu_utf8_read_shim(const char *utf8, uint32_t *unicode) {
    return nu_utf8_read(utf8, unicode);
}
