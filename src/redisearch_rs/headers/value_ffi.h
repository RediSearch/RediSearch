#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redismodule.h"
#include "query_error.h"
#include "hiredis/sds.h"

/**
 * Enumeration of the types an [`RsValue`] can be of.
 *
 * cbindgen:prefix-with-name
 */
typedef enum RSValueType {
  RSValueType_Undef = 0,
  RSValueType_Number = 1,
  RSValueType_String = 2,
  RSValueType_Null = 3,
  RSValueType_RedisString = 4,
  RSValueType_Array = 5,
  RSValueType_Reference = 6,
  RSValueType_Trio = 7,
  RSValueType_Map = 8,
} RSValueType;

/**
 * Opaque map structure used during map construction.
 * Holds uninitialized entries that are populated via [`RSValue_MapBuilderSetEntry`]
 * before being finalized into an [`RsValue::Map`] via [`RSValue_NewMapFromBuilder`].
 */
typedef struct RSValueMapBuilder RSValueMapBuilder;

/**
 * An actual [`RsValue`] object
 */
typedef struct RSValue RSValue;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Decrement the reference count of the provided [`RsValue`] object. If this was
 * the last available reference, it frees the data.
 *
 * # Safety
 *
 * 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function (it will be consumed).
 */
void RSValue_DecrRef(const struct RSValue *value);

/**
 * Computes a 64-bit FNV-1a hash of an [`RsValue`], using `hval` as the initial offset basis.
 *
 * The hashing is recursive for composite types (arrays, maps, references, trios).
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
uint64_t RSValue_Hash(const struct RSValue *value, uint64_t hval);

/**
 * Creates and returns a new **owned** [`RsValue::Undefined`].
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 */
struct RSValue *RSValue_NewUndefined(void);

/**
 * Allocates an array of null pointers with space for `len` [`RsValue`] pointers.
 *
 * The returned buffer must be populated and then passed to [`RSValue_NewArrayFromBuilder`]
 * to produce an array value.
 *
 * # Safety
 *
 * 1. The caller must eventually pass the returned pointer to [`RSValue_NewArrayFromBuilder`].
 */
struct RSValue * *RSValue_NewArrayBuilder(uint32_t len);

/**
 * Converts an [`RsValue`] to a number type in-place.
 *
 * This clears the existing value and sets it to Number with the given value.
 *
 * # Panic
 *
 * Panics if more than 1 reference exists to this [`RsValue`] object.
 *
 * # Safety
 *
 * 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function returning an owned [`RsValue`] object.
 */
void RSValue_SetNumber(struct RSValue *value, double n);

/**
 * Gets the numeric value from an [`RsValue`].
 *
 * # Panic
 *
 * Panics if the value is not an [`RsValue::Number`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
double RSValue_Number_Get(const struct RSValue *value);

/**
 * Writes the debug representation of an [`RsValue`] into an SDS string.
 *
 * If `value` is null, writes `"nil"`. Otherwise, formats the value using
 * [`DebugFormatter`](value::debug::DebugFormatter), optionally obfuscating
 * sensitive data when `obfuscate` is `true`.
 *
 * # Safety
 *
 * 1. If non-null, `value` must be a [valid] pointer to an [`RsValue`].
 * 2. `sds` must be a [valid], non-null SDS string allocated by the C SDS library.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
sds RSValue_DumpSds(const struct RSValue *value, sds sds, bool obfuscate);

/**
 * Convert the [`RsValue`] to a number. Returns `true` when this value is a number
 * or a numeric string that can be converted and writes the number to `d`. If
 * the value cannot be converted `false` is returned and nothing is written to `d`.
 *
 * # Safety
 *
 * 1. `value` must be either null or point to a valid [`RsValue`] obtained from
 *    an `RSValue_*` function.
 * 2. `d` must be a [valid], non-null pointer to a `c_double`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
bool RSValue_ToNumber(const struct RSValue *value, double *d);

/**
 * Compare two [`RsValue`]s, returning `-1` if `v1 < v2`, `0` if `v1 == v2`,
 * or `1` if `v1 > v2`.
 *
 * When `status` is null, mixed number/string comparisons fall back to
 * string-based comparison. When `status` is non-null and string-to-number
 * conversion fails, a [`QueryError`] is written to `status`.
 *
 * # Safety
 *
 * 1. `v1` and `v2` must be [valid] pointers to [`RsValue`]s.
 * 2. `status`, when non-null, must be a [valid], writable pointer to a [`QueryError`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
int RSValue_Cmp(const struct RSValue *v1, const struct RSValue *v2, struct QueryError *status);

/**
 * Allocates a new, uninitialized [`RSValueMapBuilder`] with space for `len` entries.
 *
 * The map entries are uninitialized and must be set using [`RSValue_MapBuilderSetEntry`]
 * before being finalized into an [`RsValue`] via [`RSValue_NewMapFromBuilder`].
 *
 * # Safety
 *
 * 1. All entries must be initialized via [`RSValue_MapBuilderSetEntry`] before
 *    passing the map to [`RSValue_NewMapFromBuilder`].
 */
struct RSValueMapBuilder *RSValue_NewMapBuilder(uint32_t len);

/**
 * Creates and returns a new **owned** [`RsValue::Null`].
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 */
struct RSValue *RSValue_NewNull(void);

/**
 * Follows [`RsValue::Ref`] indirections and returns a pointer to the
 * innermost non-[`Ref`](RsValue::Ref) [`RsValue`].
 *
 * The returned pointer borrows from the same allocation as `value`; no new
 * ownership is created.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
struct RSValue *RSValue_Dereference(const struct RSValue *value);

/**
 * Returns the type of the given [`RsValue`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
enum RSValueType RSValue_Type(const struct RSValue *value);

/**
 * Creates and returns a new **owned** [`RsValue::Number`]
 * containing the given numeric value.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 */
struct RSValue *RSValue_NewNumber(double value);

/**
 * Creates a heap-allocated array [`RsValue`] from existing values.
 *
 * Takes ownership of the `values` buffer and all [`RsValue`] pointers within it.
 * The values will be freed when the array is freed.
 *
 * # Safety
 *
 * 1. `values` must have been allocated via [`RSValue_NewArrayBuilder`] with
 *    a capacity equal to `len`.
 * 2. All `len` entries in `values` must have been filled with valid [`RsValue`] pointers.
 */
struct RSValue *RSValue_NewArrayFromBuilder(struct RSValue * *values, uint32_t len);

/**
 * Borrows an immutable reference to the left value of a trio.
 *
 * # Panic
 *
 * Panics if the value is not an [`RsValue::Trio`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
const struct RSValue *RSValue_Trio_GetLeft(const struct RSValue *value);

/**
 * Converts an [`RsValue`] to null type in-place.
 *
 * This clears the existing value and sets it to Null.
 *
 * # Panic
 *
 * Panics if more than 1 reference exists to this [`RsValue`] object.
 *
 * # Safety
 *
 * 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function returning an owned [`RsValue`] object.
 */
void RSValue_SetNull(struct RSValue *value);

/**
 * Like [`RSValue_Dereference`], but also follows [`RsValue::Trio`]
 * indirections by recursing into the left element of each trio.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
struct RSValue *RSValue_DereferenceRefAndTrio(const struct RSValue *value);

/**
 * Sets a key-value pair at a specific index in the map.
 *
 * Takes ownership of both the `key` and `value` [`RsValue`] pointers.
 *
 * # Safety
 *
 * 1. `map` must be a valid pointer to an [`RSValueMapBuilder`] created by
 *    [`RSValue_NewMapBuilder`].
 * 2. `key` and `value` must be valid pointers to [`RsValue`]
 *
 * # Panics
 *
 * Panics if `index` is greater than or equal to the map length.
 */
void RSValue_MapBuilderSetEntry(struct RSValueMapBuilder *map, size_t index, struct RSValue *key, struct RSValue *value);

/**
 * Check whether two [`RsValue`]s are equal, returning `true` if they are and
 * `false` otherwise.
 *
 * # Safety
 *
 * 1. `v1` and `v2` must be [valid] pointers to [`RsValue`]s.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
bool RSValue_Equal(const struct RSValue *v1, const struct RSValue *v2, struct QueryError *_status);

/**
 * Returns whether the given [`RsValue`] is a reference type, or `false` if `value` is NULL.
 *
 * # Safety
 *
 * 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
bool RSValue_IsReference(const struct RSValue *value);

/**
 * Creates and returns a new **owned** [`RsValue::Trio`] from three [`RsValue`]s.
 *
 * Takes ownership of all three arguments.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 *
 * # Safety
 *
 * 1. All three arguments must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function returning an owned [`RsValue`] object.
 */
struct RSValue *RSValue_NewTrio(struct RSValue *left, struct RSValue *middle, struct RSValue *right);

/**
 * Formats the numeric value of an [`RsValue::Number`] as a string into the
 * caller-provided buffer and returns the number of bytes written.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 * 2. `buf` must be a [valid] pointer to a writable buffer of at least 32 bytes.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 *
 * # Panic
 *
 * Panics if `value` is not an [`RsValue::Number`].
 */
size_t RSValue_NumToString(const struct RSValue *value, char *buf, size_t buflen);

/**
 * Borrows an immutable reference to the middle value of a trio.
 *
 * # Panic
 *
 * Panics if the value is not an [`RsValue::Trio`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
const struct RSValue *RSValue_Trio_GetMiddle(const struct RSValue *value);

/**
 * Resets `value` to [`RsValue::Undefined`], dropping whatever it previously held.
 *
 * # Panic
 *
 * Panics if more than 1 reference exists to this [`RsValue`] object.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
void RSValue_Clear(const struct RSValue *value);

/**
 * Returns the number of elements in an array [`RsValue`].
 *
 * If `value` is not an array, returns `0`.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
uint32_t RSValue_ArrayLen(const struct RSValue *value);

/**
 * Returns whether the given [`RsValue`] is a number type, or `false` if `value` is NULL.
 *
 * # Safety
 *
 * 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
bool RSValue_IsNumber(const struct RSValue *value);

/**
 * Creates a heap-allocated map [`RsValue`] from an [`RSValueMapBuilder`].
 *
 * Takes ownership of the map structure and all its entries. The [`RSValueMapBuilder`]
 * pointer is consumed and must not be used after this call.
 *
 * # Safety
 *
 * 1. `map` must be a valid pointer to an [`RSValueMapBuilder`] created by
 *    [`RSValue_NewMapBuilder`].
 * 2. All entries in the map must have been initialized via [`RSValue_MapBuilderSetEntry`].
 */
struct RSValue *RSValue_NewMapFromBuilder(struct RSValueMapBuilder *map);

/**
 * Converts an [`RsValue`] to a string type in-place, taking ownership of the given
 * `RedisModule_Alloc`-allocated buffer.
 *
 * This clears the existing value and sets it to an [`RsString`] of kind `RmAlloc`
 * with the given buffer.
 *
 * # Panic
 *
 * Panics if more than 1 reference exists to this [`RsValue`] object.
 *
 * # Safety
 *
 * 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function returning an owned [`RsValue`] object.
 * 2. `str` must be a [valid], non-null pointer to a buffer of `len+1` bytes
 *    allocated by `RedisModule_Alloc`.
 * 3. A nul-terminator is expected in memory at `str+len`.
 * 4. The size determined by `len` excludes the nul-terminator.
 * 5. `str` **must not** be used or freed after this function is called, as this function
 *    takes ownership of the allocation.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSValue_SetString(struct RSValue *value, char *str, uint32_t len);

/**
 * Test whether an [`RsValue`] is "truthy".
 *
 * Returns `true` for non-zero numbers, non-empty strings, and non-empty arrays.
 * All other variants (including [`RsValue::Null`] and [`RsValue::Map`])
 * evaluate to `false`. References are followed via
 * [`RsValue::fully_dereferenced_ref`].
 *
 * # Safety
 *
 * 1. `value` must be a [valid] pointer to an [`RsValue`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
bool RSValue_BoolTest(const struct RSValue *value);

/**
 * Increments the reference count of `value` and returns a new owned pointer
 * to the same allocation.
 *
 * The caller must ensure the returned pointer is eventually passed to
 * [`RSValue_DecrRef`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
struct RSValue *RSValue_IncrRef(const struct RSValue *value);

/**
 * Borrows an immutable reference to the right value of a trio.
 *
 * # Panic
 *
 * Panics if the value is not an [`RsValue::Trio`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
const struct RSValue *RSValue_Trio_GetRight(const struct RSValue *value);

/**
 * Returns whether the given [`RsValue`] is a string type (any string variant), or `false` if `value` is NULL.
 *
 * # Safety
 *
 * 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
bool RSValue_IsString(const struct RSValue *value);

/**
 * Returns a pointer to the element at `index` in an array [`RsValue`].
 *
 * If `value` is not an array, returns a null pointer. The returned pointer
 * is borrowed from the array and must not be freed by the caller.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 *
 * # Panics
 *
 * Panics if `index` greater than or equal to the array length.
 */
struct RSValue *RSValue_ArrayItem(const struct RSValue *value, uint32_t index);

/**
 * Creates and returns a new **owned** [`RsValue::String`],
 * taking ownership of the given `RedisModule_Alloc`-allocated buffer.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 *
 * # Safety
 *
 * 1. `str` must be a [valid], non-null pointer to a buffer of `len+1` bytes
 *    allocated by `RedisModule_Alloc`.
 * 2. A nul-terminator is expected in memory at `str+len`.
 * 3. The size determined by `len` excludes the nul-terminator.
 * 4. `str` **must not** be used or freed after this function is called, as this function
 *    takes ownership of the allocation.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
struct RSValue *RSValue_NewString(char *str, uint32_t len);

/**
 * Returns whether the given [`RsValue`] is an array type, or `false` if `value` is NULL.
 *
 * # Safety
 *
 * 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
bool RSValue_IsArray(const struct RSValue *value);

/**
 * Replaces the content of `dst` with an [`RsValue::Ref`] pointing to `src`.
 *
 * `src`'s reference count is incremented; `dst`'s previous content is dropped.
 *
 * # Panic
 *
 * Panics if more than 1 reference exists to the `dst` [`RsValue`] object.
 *
 * # Safety
 *
 * 1. `dst` and `src` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
void RSValue_MakeReference(const struct RSValue *dst, const struct RSValue *src);

/**
 * Returns the number of key-value pairs in a map [`RsValue`].
 *
 * # Safety
 *
 * 1. `map` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 *
 * # Panics
 *
 * Panics if `map` is not a map value.
 */
uint32_t RSValue_Map_Len(const struct RSValue *map);

/**
 * Converts an [`RsValue`] to a string type in-place, borrowing the given string buffer
 * without taking ownership.
 *
 * This clears the existing value and sets it to an [`RsString`] of kind `Const`
 * with the given buffer.
 *
 * # Panic
 *
 * Panics if more than 1 reference exists to this [`RsValue`] object.
 *
 * # Safety
 *
 * 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function returning an owned [`RsValue`] object.
 * 2. `str` must be a [valid], non-null pointer to a buffer of `len+1` bytes.
 * 3. A nul-terminator is expected in memory at `str+len`.
 * 4. The size determined by `len` excludes the nul-terminator.
 * 5. The memory pointed to by `str` must remain valid and not be mutated for the entire
 *    lifetime of the returned [`RsValue`] and any clones of it.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSValue_SetConstString(struct RSValue *value, const char *str, uint32_t len);

/**
 * Returns a pointer to the string data of an [`RsValue`] and optionally writes the string
 * length to `lenp`, if `lenp` is a non-null pointer.
 *
 * The returned pointer borrows from the [`RsValue`] and must not outlive it.
 *
 * # Panic
 *
 * Panics if the value is not an [`RsValue::String`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 * 2. `lenp` must be either null or a [valid], non-null pointer to a `u32`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
const char *RSValue_String_Get(const struct RSValue *value, uint32_t *lenp);

/**
 * Returns whether the given [`RsValue`] is a trio type, or `false` if `value` is NULL.
 *
 * # Safety
 *
 * 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
bool RSValue_IsTrio(const struct RSValue *value);

/**
 * Creates and returns a new **owned** [`RsValue::String`],
 * borrowing the given string buffer without taking ownership.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 *
 * # Safety
 *
 * 1. `str` must be a [valid], non-null pointer to a buffer of `len+1` bytes.
 * 2. A nul-terminator is expected in memory at `str+len`.
 * 3. The size determined by `len` excludes the nul-terminator.
 * 4. The memory pointed to by `str` must remain valid and not be mutated for the entire
 *    lifetime of the returned [`RsValue`] and any clones of it.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
struct RSValue *RSValue_NewBorrowedString(const char *str, uint32_t len);

/**
 * Like [`RSValue_MakeReference`], but **takes ownership** of `src` instead of
 * incrementing its reference count.
 *
 * After this call, `src` must not be used or freed by the caller.
 *
 * # Panic
 *
 * Panics if more than 1 reference exists to the `dst` [`RsValue`] object.
 *
 * # Safety
 *
 * 1. `dst` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 * 2. `src` must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function. Ownership is transferred to `dst`.
 */
void RSValue_MakeOwnReference(const struct RSValue *dst, const struct RSValue *src);

/**
 * Returns whether the given [`RsValue`] is a null pointer, a null type, or a reference to a null type.
 *
 * # Safety
 *
 * 1. If `value` is non-null, it must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
bool RSValue_IsNull(const struct RSValue *value);

/**
 * Retrieves a key-value pair from a map [`RsValue`] at a specific index.
 *
 * The returned key and value pointers are borrowed from the map and must
 * not be freed by the caller.
 *
 * # Safety
 *
 * 1. `map` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 * 2. `key` and `value` must be valid, non-null pointers to writable
 *    `*mut RsValue` locations.
 *
 * # Panics
 *
 * - Panics if `map` is not a map value.
 * - Panics if `index` is greater or equal to the map length.
 */
void RSValue_Map_GetEntry(const struct RSValue *map, uint32_t index, struct RSValue * *key, struct RSValue * *value);

/**
 * Returns a read only reference to the underlying [`RedisModuleString`] of an [`RsValue`].
 *
 * The returned reference borrows from the [`RsValue`] and must not outlive it.
 *
 * # Panic
 *
 * Panics if the value is not an [`RsValue::RedisString`].
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
const RedisModuleString *RSValue_RedisString_Get(const struct RSValue *value);

/**
 * Creates and returns a new **owned** [`RsValue::String`],
 * taking ownership of the given [`RedisModuleString`].
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 *
 * # Safety
 *
 * 1. `str` must be a [valid], non-null pointer to a [`RedisModuleString`].
 * 2. `str` **must not** be used or freed after this function is called, as this function
 *    takes ownership of the string.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
struct RSValue *RSValue_NewRedisString(RedisModuleString *str);

/**
 * Replaces the pointer at `*dstpp` with a new clone of `src`.
 *
 * The previous value at `*dstpp` is decremented (and potentially freed).
 * `src`'s reference count is incremented.
 *
 * # Safety
 *
 * 1. `dstpp` must be a valid, non-null pointer to a `*mut RsValue`.
 * 2. `*dstpp` must point to a valid **owned** [`RsValue`] obtained from an
 *    `RSValue_*` function (it will be consumed).
 * 3. `src` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
void RSValue_Replace(struct RSValue * *dstpp, const struct RSValue *src);

/**
 * Creates and returns a new **owned** [`RsValue::String`],
 * copying `len` bytes from the given string buffer into a new Rust-allocated [`Box<CStr>`].
 *
 * The caller retains ownership of `str`.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 *
 * # Safety
 *
 * 1. `str` must be a [valid], non-null pointer to a string buffer.
 * 2. `str` must be [valid] for reads of `len` bytes.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
struct RSValue *RSValue_NewCopiedString(const char *str, uint32_t len);

/**
 * Returns a pointer to the string data of an [`RsValue`] and optionally writes the string
 * length to `len_ptr`.
 *
 * Unlike [`RSValue_String_Get`], this function handles all string variants (including
 * `RedisString`) and automatically dereferences `Ref` values and follows through the left
 * element of `Trio` values. Returns null for non-string variants.
 *
 * The returned pointer borrows from the [`RsValue`] and must not outlive it.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 * 2. `len_ptr` must be either null or a [valid], non-null pointer to a `size_t`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
const char *RSValue_StringPtrLen(const struct RSValue *value, size_t *len_ptr);

/**
 * Returns the current reference count of `value`.
 *
 * # Safety
 *
 * 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
uint16_t RSValue_Refcount(const struct RSValue *value);

/**
 * Creates and returns a new **owned** [`RsValue::Number`] by parsing the given
 * string as a floating-point number. Returns a null pointer if the string
 * cannot be parsed.
 *
 * The caller retains ownership of `value`.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 *
 * # Safety
 *
 * 1. `value` must be a [valid], non-null pointer to a string buffer.
 * 2. `value` must be [valid] for reads of `len` bytes.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
struct RSValue *RSValue_NewParsedNumber(const char *value, size_t len);

/**
 * Creates and returns a new **owned** [`RsValue::Number`] from an `i64`.
 *
 * The `i64` is cast to `f64`, which may lose precision for values outside
 * the exact representable range of `f64`.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 */
struct RSValue *RSValue_NewNumberFromInt64(int64_t number);

/**
 * Creates and returns a new **owned** [`RsValue::Ref`] that points to `src`.
 *
 * `src`'s reference count is incremented; the caller retains ownership of `src`.
 *
 * The returned [`RsValue`] is heap-allocated. The caller must ensure it is eventually
 * passed to [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef). Ownership may be
 * transferred through other `RSValue_` functions before that happens.
 *
 * # Safety
 *
 * 1. `src` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
 */
struct RSValue *RSValue_NewReference(const struct RSValue *src);

/**
 * Returns a pointer to the static [`RsValue::Null`] sentinel.
 *
 * Unlike [`RSValue_NewNull`], this does **not** heap-allocate; it returns a
 * pointer to a shared static value managed by [`SharedRsValue::null_static`].
 * The returned pointer must still be passed to
 * [`RSValue_DecrRef`](crate::shared::RSValue_DecrRef) for symmetry.
 *
 * # Safety
 *
 * The returned pointer must not be mutated.
 */
struct RSValue *RSValue_NullStatic(void);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
