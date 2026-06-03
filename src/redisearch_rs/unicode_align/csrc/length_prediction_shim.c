/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/* Shims for libnu's length-prediction APIs (`nu_strlen`, `nu_strnlen`,
 * `nu_bytelen`, `nu_bytenlen`, `nu_strtransformnlen`).
 *
 * These functions take iterator and transform pointers as arguments. The
 * production iterator (`nu_utf8_read`) and transform-result iterator
 * (`nu_casemap_read`, which is `#define`d to `nu_udb_read` and ultimately
 * to `nu_utf8_read`) are both `static inline` — they have no externally
 * visible symbol, so Rust FFI cannot supply them as function pointers.
 *
 * Each wrapper here bakes the production pair (`nu_utf8_read` for decode,
 * `nu_utf8_write` for encode, `nu_tolower` + `nu_casemap_read` for
 * transformation) into a fixed-signature symbol that mirrors a live
 * RediSearch call shape:
 *
 * - `unicode_tolower()` in src/util/strconv.h: `nu_strtransformnlen` with
 *   (nu_utf8_read, nu_tolower, nu_casemap_read) — see the
 *   `nu_strtransformnlen_lower_shim` wrapper. The same site also calls
 *   `nu_bytenlen` and `nu_writenstr` with `nu_utf8_write`.
 * - `strToLowerRunes()` in src/trie/rune_util.c: same pattern.
 * - `runesToStr()` in src/trie/rune_util.c: `nu_bytelen` and `nu_writestr`
 *   with `nu_utf8_write`.
 * - `strToSingleCodepointFoldedRunes()` and `strToRunes()` in
 *   src/trie/rune_util.c: `nu_strlen` with `nu_utf8_read`.
 *
 * Keeping each wrapper a single delegation call ensures the test value is
 * in exercising libnu's predicted-length logic, not in anything the wrapper
 * adds. */

#include <stdint.h>
#include <sys/types.h>

#include "casemap.h"
#include "extra.h"
#include "strings.h"
#include "utf8.h"

ssize_t nu_strtransformnlen_lower_shim(const char *encoded, size_t max_len) {
    return nu_strtransformnlen(encoded, max_len, nu_utf8_read,
                               nu_tolower, nu_casemap_read);
}

ssize_t nu_strlen_shim(const char *encoded) {
    return nu_strlen(encoded, nu_utf8_read);
}

ssize_t nu_strnlen_shim(const char *encoded, size_t max_len) {
    return nu_strnlen(encoded, max_len, nu_utf8_read);
}

ssize_t nu_bytelen_shim(const uint32_t *unicode) {
    return nu_bytelen(unicode, nu_utf8_write);
}

ssize_t nu_bytenlen_shim(const uint32_t *unicode, size_t max_len) {
    return nu_bytenlen(unicode, max_len, nu_utf8_write);
}

int nu_writenstr_shim(const uint32_t *unicode, size_t max_len, char *encoded) {
    return nu_writenstr(unicode, max_len, encoded, nu_utf8_write);
}
