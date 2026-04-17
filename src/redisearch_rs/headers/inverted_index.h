#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redisearch_types.h"

/**
 * Filter details to apply to numeric values
 * cbindgen:rename-all=CamelCase
 */
typedef struct NumericFilter NumericFilter;

/**
 * Borrowed view of the encoded offsets of a term in a document. You can read the offsets by
 * iterating over it with RSIndexResult_IterateOffsets.
 *
 * This is a borrowed, `Copy` type — it does not own the data and will not free it on drop.
 * Use [`RSOffsetVector`] for owned offset data.
 */
typedef struct RSOffsetVector RSOffsetVector;

/**
 * Result of scanning the index for garbage collection
 */
typedef struct InvertedIndexGcDelta InvertedIndexGcDelta;

/**
 * Summary information about the key metrics of a block in an inverted index
 */
typedef struct IIBlockSummary IIBlockSummary;

/**
 * Each `IndexBlock` contains a set of entries for a specific range of document IDs. The entries
 * are ordered by document ID, so the first entry in the block has the lowest document ID, and the
 * last entry has the highest document ID. The block also contains a buffer that is used to
 * store the encoded entries. The buffer is dynamically resized as needed when new entries are
 * added to the block.
 */
typedef struct IndexBlock IndexBlock;

/**
 * An opaque inverted index structure. The actual implementation is determined at runtime based on
 * the index flags provided when creating the index. This allows us to have a single interface for
 * all index types while still being able to optimize the storage and performance for each index
 * type.
 */
typedef struct InvertedIndex InvertedIndex;

/**
 * Owned encoded offsets of a term in a document.
 *
 * This type owns the data and will free it on drop. Use [`RSOffsetSlice`] for borrowed offset
 * data.
 *
 * The `#[repr(C)]` layout is identical to [`RSOffsetSlice`] (minus the zero-sized `PhantomData`),
 * so a `&RSOffsetVector` can be safely cast to `&RSOffsetSlice<'_>`.
 */
typedef struct RSOffsetVector RSOffsetVector;

/**
 * See the crate's top level documentation for a description of this type.
 */
typedef struct ThinVec_Box_RSIndexResult__u16 ThinVec_Box_RSIndexResult__u16;

/**
 * See the crate's top level documentation for a description of this type.
 */
typedef struct ThinVec_RSIndexResult__u16 ThinVec_RSIndexResult__u16;

/**
 * The result of an inverted index
 * cbindgen:rename-all=CamelCase
 */
typedef struct RSIndexResult RSIndexResult;

/**
 * The result of an inverted index
 * cbindgen:rename-all=CamelCase
 */
typedef struct RSIndexResult RSIndexResult;

/**
 * Summary information about an inverted index containing all key metrics
 */
typedef struct IISummary {
  uint32_t number_of_docs;
  uintptr_t number_of_entries;
  t_docId last_doc_id;
  uint64_t flags;
  uintptr_t number_of_blocks;
  double block_efficiency;
  bool has_efficiency;
} IISummary;

/**
 * Information about the result of applying a garbage collection scan to the index
 */
typedef struct II_GCScanStats {
  uintptr_t bytes_freed;
  uintptr_t bytes_allocated;
  uintptr_t entries_removed;
  bool ignored_last_block;
} II_GCScanStats;

/**
 * Filter to apply when reading from an index. Entries which don't match the filter will not be
 * returned by the reader.
 * cbindgen:prefix-with-name=true
 */
enum IndexDecoderCtx_Tag
#ifdef __cplusplus
  : uint8_t
#endif // __cplusplus
 {
  IndexDecoderCtx_None,
  IndexDecoderCtx_FieldMask,
  IndexDecoderCtx_Numeric,
};
#ifndef __cplusplus
typedef uint8_t IndexDecoderCtx_Tag;
#endif // __cplusplus

typedef union IndexDecoderCtx {
  IndexDecoderCtx_Tag tag;
  struct {
    IndexDecoderCtx_Tag fieldmask_tag;
    t_fieldMask fieldmask;
  };
  struct {
    IndexDecoderCtx_Tag numeric_tag;
    const struct NumericFilter *numeric;
  };
} IndexDecoderCtx;

#ifndef SMALLTHINVEC_BOX_RSINDEXRESULT_DEFINED
#define SMALLTHINVEC_BOX_RSINDEXRESULT_DEFINED
/**
 * A [`ThinVec`] with `u16` capacity, supporting up to 65,535 elements.
 *
 * This is useful when you know the vector will never exceed 65,535 elements
 * and want to minimize header overhead (4 bytes instead of 16).
 */
typedef struct ThinVec_Box_RSIndexResult__u16 SmallThinVec_Box_RSIndexResult;
#endif /* SMALLTHINVEC_BOX_RSINDEXRESULT_DEFINED */

#ifndef SMALLTHINVEC_RSINDEXRESULT_DEFINED
#define SMALLTHINVEC_RSINDEXRESULT_DEFINED
/**
 * A [`ThinVec`] with `u16` capacity, supporting up to 65,535 elements.
 *
 * This is useful when you know the vector will never exceed 65,535 elements
 * and want to minimize header overhead (4 bytes instead of 16).
 */
typedef struct ThinVec_RSIndexResult__u16 SmallThinVec_RSIndexResult;
#endif /* SMALLTHINVEC_RSINDEXRESULT_DEFINED */

#ifndef BITFLAGS_RSRESULTKIND__U8_DEFINED
#define BITFLAGS_RSRESULTKIND__U8_DEFINED
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
typedef uint8_t BitFlags_RSResultKind__u8;
#endif /* BITFLAGS_RSRESULTKIND__U8_DEFINED */

typedef BitFlags_RSResultKind__u8 RSResultKindMask;

/**
 * Represents an aggregate array of values in an index record.
 *
 * The C code should always use `AggregateResult_New` to construct a new instance of this type
 * using Rust since the internals cannot be constructed directly in C. The reason is because of
 * the `ThinVec` which needs to exist in Rust's memory space to ensure its memory is
 * managed correctly.
 * cbindgen:prefix-with-name=true
 */
enum RSAggregateResult_Tag
#ifdef __cplusplus
  : uint8_t
#endif // __cplusplus
 {
  RSAggregateResult_Borrowed,
  RSAggregateResult_Owned,
};
#ifndef __cplusplus
typedef uint8_t RSAggregateResult_Tag;
#endif // __cplusplus

typedef struct RSAggregateResult_Borrowed_Body {
  RSAggregateResult_Tag tag;
  SmallThinVec_RSIndexResult records;
  RSResultKindMask kind_mask;
} RSAggregateResult_Borrowed_Body;

typedef struct RSAggregateResult_Owned_Body {
  RSAggregateResult_Tag tag;
  SmallThinVec_Box_RSIndexResult records;
  RSResultKindMask kind_mask;
} RSAggregateResult_Owned_Body;

typedef union RSAggregateResult {
  RSAggregateResult_Tag tag;
  struct RSAggregateResult_Borrowed_Body borrowed;
  struct RSAggregateResult_Owned_Body owned;
} RSAggregateResult;

/**
 * Represents an aggregate array of values in an index record.
 *
 * The C code should always use `AggregateResult_New` to construct a new instance of this type
 * using Rust since the internals cannot be constructed directly in C. The reason is because of
 * the `ThinVec` which needs to exist in Rust's memory space to ensure its memory is
 * managed correctly.
 * cbindgen:prefix-with-name=true
 */
enum RSAggregateResult_Tag
#ifdef __cplusplus
  : uint8_t
#endif // __cplusplus
 {
  RSAggregateResult_Borrowed,
  RSAggregateResult_Owned,
};
#ifndef __cplusplus
typedef uint8_t RSAggregateResult_Tag;
#endif // __cplusplus

typedef struct RSAggregateResult_Borrowed_Body {
  RSAggregateResult_Tag tag;
  SmallThinVec_RSIndexResult records;
  RSResultKindMask kind_mask;
} RSAggregateResult_Borrowed_Body;

typedef struct RSAggregateResult_Owned_Body {
  RSAggregateResult_Tag tag;
  SmallThinVec_Box_RSIndexResult records;
  RSResultKindMask kind_mask;
} RSAggregateResult_Owned_Body;

typedef union RSAggregateResult {
  RSAggregateResult_Tag tag;
  struct RSAggregateResult_Borrowed_Body borrowed;
  struct RSAggregateResult_Owned_Body owned;
} RSAggregateResult;
