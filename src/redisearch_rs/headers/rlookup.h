#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Row data for a lookup key. This abstracts the question of if the data comes from a borrowed sorting vector slice
 * or from dynamic values stored in the row during processing.
 */
typedef struct RLookupRow RLookupRow;

/**
 * An append-only list of [`RLookupKey`]s.
 *
 * This type maintains a mapping from string names to [`RLookupKey`]s.
 */
typedef struct RLookup RLookup;

#ifndef SIZE_24_DEFINED
#define SIZE_24_DEFINED
/**
 * A type with size `N`.
 */
typedef uint8_t Size_24[24];
#endif /* SIZE_24_DEFINED */

#ifndef SIZE_40_DEFINED
#define SIZE_40_DEFINED
/**
 * A type with size `N`.
 */
typedef uint8_t Size_40[40];
#endif /* SIZE_40_DEFINED */

/**
 * An opaque lookup row which can be passed by value to C.
 *
 * The size and alignment of this struct must match the Rust `RLookupRow`
 * structure exactly.
 */
typedef struct RLookupRow {
  Size_24 m0;
} RLookupRow;

/**
 * An opaque lookup which can be passed by value to C.
 *
 * The size and alignment of this struct must match the Rust `RLookup`
 * structure exactly.
 */
typedef struct RLookup {
  Size_40 m0;
} RLookup;
