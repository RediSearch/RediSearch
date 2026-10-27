/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{GeoDistance, GeoFilter};

/// Cursor access needed for legacy geo parsing.
///
/// Numeric conversion stays with the caller: Redis strings and internal C/SDS
/// strings have different numeric grammars. Errors must use the cursor's error
/// text. Accessors do not advance the cursor.
pub trait LegacyGeoArgs {
    /// Total argument count.
    fn len(&self) -> usize;

    /// Whether the cursor has no arguments.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes and logical length of the argument at `index`.
    fn string(&self, index: usize) -> Result<&[u8], &'static str>;

    /// Convert the argument at `index` using the cursor's own numeric rules.
    fn number(&self, index: usize) -> Result<f64, &'static str>;
}

/// A legacy parse failure, to be mapped to a parse-arguments query error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LegacyGeoParseError {
    /// Not enough arguments remain.
    #[error("GEOFILTER requires 5 arguments")]
    MissingArguments,
    /// An argument could not be read or converted.
    #[error("Bad arguments for <{argument}>: {reason}")]
    BadArgument {
        /// Argument label used by the C parser.
        argument: &'static str,
        /// Cursor conversion error text.
        reason: &'static str,
    },
    /// An unknown unit, retaining its C-string bytes for the error adapter.
    #[error("Unknown distance unit {}", String::from_utf8_lossy(.0))]
    UnknownUnit(Vec<u8>),
}

/// Legacy syntax retains the field name until schema resolution.
///
/// Field bytes are owned and freed by ordinary Rust drop, rather than a C hidden
/// string allocation. Numeric-filter cache ownership belongs to the iterator.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyGeoFilter {
    /// Filter geometry, not yet validated.
    pub base: GeoFilter,
    /// Field name, truncated at NUL as by the legacy C parser's `strlen`.
    pub field: Vec<u8>,
}

impl LegacyGeoFilter {
    /// Parse field, longitude, latitude, radius, and unit at `offset`.
    ///
    /// Advances `offset` exactly as the legacy cursor does, including on failure.
    /// Only failed numeric conversions of logically empty tokens coalesce to zero
    /// and set `has_empty_value`; this flag is never cleared. Validation and dialect
    /// rejection of empty values remain the caller's responsibility.
    ///
    /// # Panics
    ///
    /// Panics if `offset` exceeds [`LegacyGeoArgs::len`].
    pub fn parse(
        args: &impl LegacyGeoArgs,
        offset: &mut usize,
        has_empty_value: &mut bool,
    ) -> Result<Self, LegacyGeoParseError> {
        assert!(*offset <= args.len());
        if args.len() - *offset < 5 {
            return Err(LegacyGeoParseError::MissingArguments);
        }
        let field = args
            .string(*offset)
            .map_err(|reason| LegacyGeoParseError::BadArgument {
                argument: "geo property",
                reason,
            })?;
        *offset += 1;

        let mut values = [0.0; 3];
        for (target, argument) in values.iter_mut().zip(["lon", "lat", "radius"]) {
            *target = match args.number(*offset) {
                Ok(value) => value,
                Err(reason) => {
                    if args.string(*offset).is_ok_and(<[u8]>::is_empty) {
                        *has_empty_value = true;
                        0.0
                    } else {
                        return Err(LegacyGeoParseError::BadArgument { argument, reason });
                    }
                }
            };
            *offset += 1;
        }

        let unit_bytes =
            args.string(*offset)
                .map_err(|reason| LegacyGeoParseError::BadArgument {
                    argument: "unit",
                    reason,
                })?;
        *offset += 1;
        let unit = GeoDistance::parse(unit_bytes);
        if unit == GeoDistance::Invalid {
            return Err(LegacyGeoParseError::UnknownUnit(
                unit_bytes
                    .split(|&b| b == 0)
                    .next()
                    .unwrap_or_default()
                    .to_vec(),
            ));
        }
        let mut base = GeoFilter::new(values[0], values[1], values[2], None);
        base.unit = unit;
        Ok(Self {
            base,
            field: field.split(|&b| b == 0).next().unwrap_or_default().to_vec(),
        })
    }
}
