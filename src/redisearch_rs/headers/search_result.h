#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redisearch_types.h"
#include "document_metadata.h"
#include "rlookup.h"

/**
 * SearchResult - the object all the processing chain is working on.
 * It holds the [`RSIndexResult`] which is what the index scan brought - scores, vectors, flags, etc,
 * and a list of fields loaded by the chain
 */
typedef struct SearchResult SearchResult;

#ifndef BITFLAGS_SEARCHRESULTFLAG__U8_DEFINED
#define BITFLAGS_SEARCHRESULTFLAG__U8_DEFINED
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
typedef uint8_t BitFlags_SearchResultFlag__u8;
#endif /* BITFLAGS_SEARCHRESULTFLAG__U8_DEFINED */

typedef BitFlags_SearchResultFlag__u8 SearchResultFlags;

/**
 * SearchResult - the object all the processing chain is working on.
 * It holds the [`RSIndexResult`] which is what the index scan brought - scores, vectors, flags, etc,
 * and a list of fields loaded by the chain
 */
typedef struct SearchResult {
  t_docId _doc_id;
  double _score;
  RSScoreExplain *_score_explain;
  DocumentMetadata _document_metadata;
  const struct RSIndexResult *_index_result;
  struct RLookupRow _row_data;
  SearchResultFlags _flags;
} SearchResult;
