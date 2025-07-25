#pragma once

/* Warning, this file is autogenerated by cbindgen from `src/redisearch_rs/c_entrypoint/types_ffi/build.rs. Don't modify it manually. */

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
/**
 * Forward declarations which will be defined in `redisearch.h`
 */
typedef struct RSQueryTerm RSQueryTerm;
typedef struct RSDocumentMetadata_s RSDocumentMetadata;
typedef struct RSYieldableMetric RSYieldableMetric;
typedef uint64_t t_docId;

/* Copied from `redisearch.h` */
#if (defined(__x86_64__) || defined(__aarch64__) || defined(__arm64__)) && !defined(RS_NO_U128)
/* 64 bit architectures use 128 bit field masks and up to 128 fields */
typedef __uint128_t t_fieldMask;
#else
/* 32 bit architectures use 64 bits and 64 fields only */
typedef uint64_t t_fieldMask;
#endif


enum RSResultType
#ifdef __cplusplus
  : uint32_t
#endif // __cplusplus
 {
  RSResultType_Union = 1,
  RSResultType_Intersection = 2,
  RSResultType_Term = 4,
  RSResultType_Virtual = 8,
  RSResultType_Numeric = 16,
  RSResultType_Metric = 32,
  RSResultType_HybridMetric = 64,
};
#ifndef __cplusplus
typedef uint32_t RSResultType;
#endif // __cplusplus

/**
 * An iterator over the results in an [`RSAggregateResult`].
 */
typedef struct RSAggregateResultIter RSAggregateResultIter;

/**
 * Represents a set of flags of some type `T`.
 * `T` must have the `#[bitflags]` attribute applied.
 *
 * A `BitFlags<T>` is as large as the `T` itself,
 * and stores one flag per bit.
 *
 * ## Comparison operators, [`PartialOrd`] and [`Ord`]
 *
 * To make it possible to use `BitFlags` as the key of a
 * [`BTreeMap`][std::collections::BTreeMap], `BitFlags` implements
 * [`Ord`]. There is no meaningful total order for bitflags,
 * so the implementation simply compares the integer values of the bits.
 *
 * Unfortunately, this means that comparing `BitFlags` with an operator
 * like `<=` will compile, and return values that are probably useless
 * and not what you expect. In particular, `<=` does *not* check whether
 * one value is a subset of the other. Use [`BitFlags::contains`] for that.
 *
 * ## Customizing `Default`
 *
 * By default, creating an instance of `BitFlags<T>` with `Default` will result
 * in an empty set. If that's undesirable, you may customize this:
 *
 * ```
 * # use enumflags2::{BitFlags, bitflags};
 * #[bitflags(default = B | C)]
 * #[repr(u8)]
 * #[derive(Copy, Clone, Debug, PartialEq)]
 * enum MyFlag {
 *     A = 0b0001,
 *     B = 0b0010,
 *     C = 0b0100,
 *     D = 0b1000,
 * }
 *
 * assert_eq!(BitFlags::default(), MyFlag::B | MyFlag::C);
 * ```
 *
 * ## Memory layout
 *
 * `BitFlags<T>` is marked with the `#[repr(transparent)]` trait, meaning
 * it can be safely transmuted into the corresponding numeric type.
 *
 * Usually, the same can be achieved by using [`BitFlags::bits`] in one
 * direction, and [`BitFlags::from_bits`], [`BitFlags::from_bits_truncate`],
 * or [`BitFlags::from_bits_unchecked`] in the other direction. However,
 * transmuting might still be useful if, for example, you're dealing with
 * an entire array of `BitFlags`.
 *
 * When transmuting *into* a `BitFlags`, make sure that each set bit
 * corresponds to an existing flag
 * (cf. [`from_bits_unchecked`][BitFlags::from_bits_unchecked]).
 *
 * For example:
 *
 * ```
 * # use enumflags2::{BitFlags, bitflags};
 * #[bitflags]
 * #[repr(u8)] // <-- the repr determines the numeric type
 * #[derive(Copy, Clone)]
 * enum TransmuteMe {
 *     One = 1 << 0,
 *     Two = 1 << 1,
 * }
 *
 * # use std::slice;
 * // NOTE: we use a small, self-contained function to handle the slice
 * // conversion to make sure the lifetimes are right.
 * fn transmute_slice<'a>(input: &'a [BitFlags<TransmuteMe>]) -> &'a [u8] {
 *     unsafe {
 *         slice::from_raw_parts(input.as_ptr() as *const u8, input.len())
 *     }
 * }
 *
 * let many_flags = &[
 *     TransmuteMe::One.into(),
 *     TransmuteMe::One | TransmuteMe::Two,
 * ];
 *
 * let as_nums = transmute_slice(many_flags);
 * assert_eq!(as_nums, &[0b01, 0b11]);
 * ```
 *
 * ## Implementation notes
 *
 * You might expect this struct to be defined as
 *
 * ```ignore
 * struct BitFlags<T: BitFlag> {
 *     value: T::Numeric
 * }
 * ```
 *
 * Ideally, that would be the case. However, because `const fn`s cannot
 * have trait bounds in current Rust, this would prevent us from providing
 * most `const fn` APIs. As a workaround, we define `BitFlags` with two
 * type parameters, with a default for the second one:
 *
 * ```ignore
 * struct BitFlags<T, N = <T as BitFlag>::Numeric> {
 *     value: N,
 *     marker: PhantomData<T>,
 * }
 * ```
 *
 * Manually providing a type for the `N` type parameter shouldn't ever
 * be necessary.
 *
 * The types substituted for `T` and `N` must always match, creating a
 * `BitFlags` value where that isn't the case is only possible with
 * incorrect unsafe code.
 */
typedef uint32_t BitFlags_RSResultType__u32;

typedef BitFlags_RSResultType__u32 RSResultTypeMask;

/**
 * Represents an aggregate array of values in an index record.
 */
typedef struct RSAggregateResult {
  /**
   * The number of child records
   */
  int numChildren;
  /**
   * The capacity of the records array. Has no use for extensions
   */
  int childrenCap;
  /**
   * An array of records
   */
  struct RSIndexResult **children;
  /**
   * A map of the aggregate type of the underlying records
   */
  RSResultTypeMask typeMask;
} RSAggregateResult;

/**
 * Represents the encoded offsets of a term in a document. You can read the offsets by iterating
 * over it with RSOffsetVector_Iterator
 */
typedef struct RSOffsetVector {
  char *data;
  uint32_t len;
} RSOffsetVector;

/**
 * Represents a single record of a document inside a term in the inverted index
 */
typedef struct RSTermRecord {
  /**
   * The term that brought up this record
   */
  RSQueryTerm *term;
  /**
   * The encoded offsets in which the term appeared in the document
   */
  struct RSOffsetVector offsets;
} RSTermRecord;

/**
 * Represents a numeric value in an index record.
 */
typedef struct RSNumericRecord {
  double value;
} RSNumericRecord;

/**
 * Represents a virtual result in an index record.
 */
typedef struct RSVirtualResult {

} RSVirtualResult;

/**
 * Holds the actual data of an ['IndexResult']
 */
typedef union RSIndexResultData {
  struct RSAggregateResult agg;
  struct RSTermRecord term;
  struct RSNumericRecord num;
  struct RSVirtualResult virt;
} RSIndexResultData;

/**
 * The result of an inverted index
 */
typedef struct RSIndexResult {
  /**
   * The document ID of the result
   */
  t_docId docId;
  /**
   * Some metadata about the result document
   */
  const RSDocumentMetadata *dmd;
  /**
   * The aggregate field mask of all the records in this result
   */
  t_fieldMask fieldMask;
  /**
   * The total frequency of all the records in this result
   */
  uint32_t freq;
  /**
   * For term records only. This is used as an optimization, allowing the result to be loaded
   * directly into memory
   */
  uint32_t offsetsSz;
  union RSIndexResultData data;
  /**
   * The type of data stored at ['Self::data']
   */
  RSResultType type;
  /**
   * We mark copied results so we can treat them a bit differently on deletion, and pool them if
   * we want
   */
  bool isCopy;
  /**
   * Holds an array of metrics yielded by the different iterators in the AST
   */
  RSYieldableMetric *metrics;
  /**
   * Relative weight for scoring calculations. This is derived from the result's iterator weight
   */
  double weight;
} RSIndexResult;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Get the result at the specified index in the aggregate result. This will return a `NULL` pointer
 * if the index is out of bounds.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 * - The memory address at `index` should still be valid and not have been deallocated.
 */
const struct RSIndexResult *AggregateResult_Get(const struct RSAggregateResult *agg,
                                                uintptr_t index);

/**
 * Get the element count of the aggregate result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
uintptr_t AggregateResult_NumChildren(const struct RSAggregateResult *agg);

/**
 * Get the capacity of the aggregate result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
uintptr_t AggregateResult_Capacity(const struct RSAggregateResult *agg);

/**
 * Get the type mask of the aggregate result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
uint32_t AggregateResult_TypeMask(const struct RSAggregateResult *agg);

/**
 * Reset the aggregate result, clearing all children and resetting the type mask. This function
 * does not deallocate the children pointers, but rather resets the internal state of the
 * aggregate result. The owner of the children pointers is responsible for managing their lifetime.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
void AggregateResult_Reset(struct RSAggregateResult *agg);

/**
 * Create an iterator over the aggregate result. This iterator should be freed
 * using [`AggregateResultIter_Free`].
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
struct RSAggregateResultIter *AggregateResult_Iter(const struct RSAggregateResult *agg);

/**
 * Get the next item in the aggregate result iterator and put it into the provided `value`
 * pointer. This function will return `true` if there is a next item, or `false` if the iterator
 * is exhausted.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `iter` must point to a valid `RSAggregateResultIter` and cannot be NULL.
 * - `value` must point to a valid pointer where the next item will be stored.
 * - All the memory addresses of the `RSAggregateResult` should still be valid and not have
 *   been deallocated.
 */
bool AggregateResultIter_Next(struct RSAggregateResultIter *iter, struct RSIndexResult **value);

/**
 * Free the aggregate result iterator. This function will deallocate the memory used by the iterator.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `iter` must point to a valid `RSAggregateResultIter` and cannot be NULL.
 * - The iterator must have been created using [`AggregateResult_Iter`].
 */
void AggregateResultIter_Free(struct RSAggregateResultIter *iter);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
