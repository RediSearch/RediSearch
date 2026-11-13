/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Decoding and validation of sparse vectors, shared by documents and queries.

use std::fmt;

use crate::SparseMetric;

/// Size of one `(u32 dim_id, f32 weight)` pair in the binary encoding.
pub const PAIR_SIZE: usize = 8;

/// Why a sparse vector was rejected.
#[derive(Debug, Clone, PartialEq)]
pub enum SparseVectorError {
    /// The blob length is not a multiple of [`PAIR_SIZE`].
    MalformedBlob {
        /// The blob length in bytes.
        len: usize,
    },
    /// `indices` and `values` have different lengths.
    LengthMismatch {
        /// The number of dimension IDs.
        indices: usize,
        /// The number of weights.
        values: usize,
    },
    /// A dimension ID is not greater than the one before it.
    NotIncreasing {
        /// The dimension ID out of order.
        dim_id: u32,
    },
    /// A dimension ID is outside the field's dimension space.
    DimensionOutOfRange {
        /// The offending dimension ID.
        dim_id: u32,
        /// The field's dimension count.
        dim: u64,
    },
    /// A weight is NaN or infinite.
    NonFinite {
        /// The dimension the weight belongs to.
        dim_id: u32,
    },
    /// Under [`SparseMetric::Cosine`], the vector has no non-zero weight.
    ZeroNorm,
}

impl fmt::Display for SparseVectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedBlob { len } => write!(
                f,
                "sparse vector blob size {len} is not a multiple of {PAIR_SIZE}"
            ),
            Self::LengthMismatch { indices, values } => {
                write!(f, "sparse vector has {indices} indices but {values} values")
            }
            Self::NotIncreasing { dim_id } => write!(
                f,
                "sparse vector dimension ids must be strictly increasing (at {dim_id})"
            ),
            Self::DimensionOutOfRange { dim_id, dim } => write!(
                f,
                "sparse vector dimension id {dim_id} is out of range (DIM is {dim})"
            ),
            Self::NonFinite { dim_id } => {
                write!(
                    f,
                    "sparse vector weight at dimension {dim_id} is not finite"
                )
            }
            Self::ZeroNorm => f.write_str("sparse vector has zero norm under COSINE"),
        }
    }
}

impl std::error::Error for SparseVectorError {}

/// Decode a little-endian blob of `(u32 dim_id, f32 weight)` pairs.
///
/// Only the encoding is checked here; [`prepare`] checks the values.
pub fn parse_blob(blob: &[u8]) -> Result<Vec<(u32, f32)>, SparseVectorError> {
    if !blob.len().is_multiple_of(PAIR_SIZE) {
        return Err(SparseVectorError::MalformedBlob { len: blob.len() });
    }
    Ok(blob
        .chunks_exact(PAIR_SIZE)
        .map(|pair| {
            let (id, weight) = pair.split_at(4);
            (
                u32::from_le_bytes(id.try_into().expect("split at 4 of an 8-byte chunk")),
                f32::from_le_bytes(weight.try_into().expect("split at 4 of an 8-byte chunk")),
            )
        })
        .collect())
}

/// Validate a vector against a field of `dim` dimensions and bring it to the
/// form that is written or queried.
///
/// Rejects dimension IDs that are not strictly increasing or not below `dim`,
/// and weights that are not finite. Drops zero weights. Under
/// [`SparseMetric::Cosine`], divides every weight by the vector's L2 norm, and
/// rejects a vector whose norm is zero.
pub fn prepare(
    mut entries: Vec<(u32, f32)>,
    dim: u64,
    metric: SparseMetric,
) -> Result<Vec<(u32, f32)>, SparseVectorError> {
    let mut previous: Option<u32> = None;
    for &(dim_id, weight) in &entries {
        if previous.is_some_and(|p| dim_id <= p) {
            return Err(SparseVectorError::NotIncreasing { dim_id });
        }
        if u64::from(dim_id) >= dim {
            return Err(SparseVectorError::DimensionOutOfRange { dim_id, dim });
        }
        if !weight.is_finite() {
            return Err(SparseVectorError::NonFinite { dim_id });
        }
        previous = Some(dim_id);
    }

    entries.retain(|&(_, weight)| weight != 0.0);

    if metric == SparseMetric::Cosine {
        // Accumulated in `f64` so that a long vector of small `f32` weights
        // neither underflows nor loses precision before the square root.
        let norm = entries
            .iter()
            .map(|&(_, w)| f64::from(w) * f64::from(w))
            .sum::<f64>()
            .sqrt();
        if norm == 0.0 {
            return Err(SparseVectorError::ZeroNorm);
        }
        for (_, weight) in &mut entries {
            *weight = (f64::from(*weight) / norm) as f32;
        }
    }

    Ok(entries)
}
