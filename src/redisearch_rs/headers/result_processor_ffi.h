#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Intentionally trigger a crash in Rust code,
 * to verify the crash handling mechanism.
 *
 * Used by the crash result processor.
 */
void CrashInRust(void);

/**
 * Crate a new heap-allocated `Counter` result processor
 *
 * # Safety
 *
 * - The caller must never move the allocated result processor from its original allocation.
 * - The caller must ensure to call the `Free` VTable function to properly destroy the type.
 */
ResultProcessor *RPCounter_New(void);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
