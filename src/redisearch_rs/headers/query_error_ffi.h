#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Returns the default [`QueryError`].
 */
struct QueryError QueryError_Default(void);

/**
 * Returns true if `query_error` has no error code set.
 *
 * # Safety
 *
 * `query_error` must have been created by [`QueryError_Default`].
 */
bool QueryError_IsOk(const struct QueryError *query_error);

/**
 * Returns true if `query_error` has an error code set.
 *
 * # Safety
 *
 * `query_error` must have been created by [`QueryError_Default`].
 */
bool QueryError_HasError(const struct QueryError *query_error);

/**
 * Returns the full default error string for a [`QueryErrorCode`] (prefix + message).
 *
 * This function should always return without a panic for any value provided.
 * It is unique among the `QueryError_*` API as the only function which allows
 * an invalid [`QueryErrorCode`] to be provided.
 */
const char *QueryError_Strerror(uint8_t maybe_code);

/**
 * Returns only the error prefix string for a [`QueryErrorCode`] (e.g. `"SEARCH_TIMEOUT: "`).
 *
 * Returns an empty string for `Ok` and `"Unknown status code"` for invalid codes.
 */
const char *QueryError_StrerrorPrefix(uint8_t maybe_code);

/**
 * Returns only the default message for a [`QueryErrorCode`] (without the prefix).
 *
 * Returns `"Unknown status code"` for invalid codes.
 */
const char *QueryError_StrerrorDefaultMessage(uint8_t maybe_code);

/**
 * Returns a human-readable string representing the provided [`QueryWarningCode`].
 *
 * This function should always return without a panic for any value provided.
 * It is unique among the `QueryWarning_*` API as the only function which allows
 * an invalid [`QueryWarningCode`] to be provided.
 */
const char *QueryWarning_Strwarning(uint8_t maybe_code);

/**
 * Returns the maximum valid numeric value for [`QueryErrorCode`].
 *
 * This is intended for C/C++ tests/tools that want to iterate over all codes without
 * hardcoding the current "last" variant.
 */
uint8_t QueryError_CodeMaxValue(void);

/**
 * Returns a [`QueryErrorCode`] given an error message.
 *
 * This only supports the query error codes [`QueryErrorCode::TimedOut`],
 * [`QueryErrorCode::OutOfMemory`], and [`QueryErrorCode::UnavailableSlots`].
 * If another message is provided, [`QueryErrorCode::Generic`] is returned.
 *
 *
 * # Safety
 *
 * - `message` must be a valid C string or a NULL pointer.
 */
QueryErrorCode QueryError_GetCodeFromMessage(const char *message);

/**
 * Sets the [`QueryErrorCode`] and error message for a [`QueryError`].
 *
 * The public message is stored as-is (for obfuscated display).
 * The private message is stored with the error code prefix prepended
 * (e.g. `"SEARCH_TIMEOUT: "` + message), so that Redis error stats
 * can track errors by their unique prefix.
 *
 * This does not mutate `query_error` if it already has an error set.
 *
 * # Panics
 *
 * - `code` must be a valid variant of [`QueryErrorCode`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 * - `message` must be a valid C string or a NULL pointer.
 */
void QueryError_SetError(struct QueryError *query_error, uint8_t code, const char *message);

/**
 * Sets the [`QueryErrorCode`] for a [`QueryError`].
 *
 * This does not mutate `query_error` if it already has an error set.
 *
 * # Panics
 *
 * - `code` must be a valid variant of [`QueryErrorCode`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
void QueryError_SetCode(struct QueryError *query_error, uint8_t code);

/**
 * Always sets the private message for a [`QueryError`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 * - `detail` must be a valid C string or a NULL pointer.
 */
void QueryError_SetDetail(struct QueryError *query_error, const char *detail);

/**
 * Clones the `src` [`QueryError`] into `dest`.
 *
 * This does nothing if `dest` already has an error set.
 *
 * # Safety
 *
 * - `src` must have been created by [`QueryError_Default`].
 * - `dest` must have been created by [`QueryError_Default`].
 */
void QueryError_CloneFrom(const struct QueryError *src, struct QueryError *dest);

/**
 * Returns the private message set for a [`QueryError`]. If no private message
 * is set, this returns the string error message for the code that is set,
 * like [`QueryError_Strerror`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
const char *QueryError_GetUserError(const struct QueryError *query_error);

/**
 * Returns an message of a [`QueryError`].
 *
 * This preferentially returns the private message if any, or the public
 * message if any, lastly defaulting to the error code's string error.
 *
 * If `obfuscate` is set, the private message is not returned. The public
 * message is returned, if any, defaulting to the error code's string error.
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
const char *QueryError_GetDisplayableError(const struct QueryError *query_error, bool obfuscate);

/**
 * Returns the [`QueryErrorCode`] set for a [`QueryError`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
QueryErrorCode QueryError_GetCode(const struct QueryError *query_error);

/**
 * Clears any error set on a [`QueryErrorCode`].
 *
 * This is equivalent to resetting `query_error` to the value returned by
 * [`QueryError_Default`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
void QueryError_ClearError(struct QueryError *query_error);

/**
 * Sets the [`QueryErrorCode`] for a [`QueryError`].
 *
 * This does not mutate `query_error` if it already has an error set, or
 * if the private message is set. This differs from [`QueryError_SetCode`],
 * as that function does not care if the private message is set.
 *
 * # Panics
 *
 * - `code` must be a valid variant of [`QueryErrorCode`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
void QueryError_MaybeSetCode(struct QueryError *query_error, uint8_t code);

/**
 * Returns whether the [`QueryError`] has the `reached_max_prefix_expansions`
 * warning set.
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
bool QueryError_HasReachedMaxPrefixExpansionsWarning(const struct QueryError *query_error);

/**
 * Sets the `reached_max_prefix_expansions` warning on the [`QueryError`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
void QueryError_SetReachedMaxPrefixExpansionsWarning(struct QueryError *query_error);

/**
 * Returns whether the [`QueryError`] has the `out_of_memory` warning set.
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
bool QueryError_HasQueryOOMWarning(const struct QueryError *query_error);

/**
 * Sets the `out_of_memory` warning on the [`QueryError`].
 *
 * # Safety
 *
 * - `query_error` must have been created by [`QueryError_Default`].
 */
void QueryError_SetQueryOOMWarning(struct QueryError *query_error);

/**
 * Returns a [`QueryWarningCode`] given an warnings message.
 *
 * This only supports the query error codes [`QueryWarningCode::TimedOut`], [`QueryWarningCode::ReachedMaxPrefixExpansions`],
 * [`QueryWarningCode::OutOfMemoryShard`] and [`QueryWarningCode::OutOfMemoryCoord`]. If another message is provided,
 * [`QueryWarningCode::Ok`] is returned.
 *
 * # Safety
 *
 * - `message` must be a valid C string or a NULL pointer.
 */
QueryWarningCode QueryWarningCode_GetCodeFromMessage(const char *message);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
