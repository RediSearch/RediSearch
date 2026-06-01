/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iterator profile printing for Redis PROFILE output.
//!
//! This module contains the logic for rendering iterator profile trees as
//! Redis protocol replies. It defines the [`ProfilePrint`] trait, implemented
//! by iterators.

use std::{borrow::Cow, ffi::CStr};

use redis_reply::MapBuilder;

use crate::profile::ProfileCounters;

/// Trait for iterator types that can print their profile.
pub trait ProfilePrint {
    /// Print this iterator's profile as a Redis reply.
    ///
    /// The caller opens the map; the implementation fills it.
    fn print_profile(&self, map: &mut MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>);
}

/// Context passed through recursive profile printing calls.
pub struct ProfilePrintCtx<'iter> {
    /// Whether to limit union child output. When `true`, non-`UNION` union
    /// iterators collapse their children into a single summary string
    /// instead of printing each child individually. Corresponds to
    /// `FT.PROFILE ... LIMITED`.
    pub limited: bool,
    /// Whether to include wall-clock timing (`"Time"`) in profile entries.
    /// Corresponds to the `PROFILE_VERBOSE` flag.
    pub print_profile_clock: bool,
    /// [`ProfileCounters`] from the parent [`Profile`](crate::profile::Profile)
    /// wrapper, or [`None`] if the iterator is not profile-wrapped.
    pub counters: Option<&'iter ProfileCounters>,
    /// Wall time in nanoseconds from the parent
    /// [`Profile`](crate::profile::Profile) wrapper.
    pub wall_time_ns: u64,
}

impl ProfilePrintCtx<'_> {
    /// Create a new top-level context.
    pub const fn new(limited: bool, print_profile_clock: bool) -> Self {
        Self {
            limited,
            print_profile_clock,
            counters: None,
            wall_time_ns: 0,
        }
    }

    /// Create a child context with specific counters and timing (for
    /// [`Profile`](crate::profile::Profile) wrapper).
    pub const fn with_counters<'b>(
        &'b self,
        counters: &'b ProfileCounters,
        wall_time_ns: u64,
    ) -> ProfilePrintCtx<'b> {
        ProfilePrintCtx {
            limited: self.limited,
            print_profile_clock: self.print_profile_clock,
            counters: Some(counters),
            wall_time_ns,
        }
    }

    /// Create a child context with no counters (for recursive child printing).
    pub const fn child_ctx(&self) -> ProfilePrintCtx<'static> {
        ProfilePrintCtx {
            limited: self.limited,
            print_profile_clock: self.print_profile_clock,
            counters: None,
            wall_time_ns: 0,
        }
    }

    /// Print time (if verbose) and counters (if available) for this context.
    pub fn print_optional_counters(&self, map: &mut MapBuilder<'_>) {
        if self.print_profile_clock {
            map.kv_double(c"Time", self.wall_time_ns as f64 / 1_000_000.0);
        }
        if let Some(counters) = self.counters {
            map.kv_long_long(
                c"Number of reading operations",
                counters.num_reading_operations() as i64,
            );
        }
    }

    /// Print a leaf iterator (no children) profile.
    pub fn print_leaf(&self, type_name: &CStr, map: &mut MapBuilder<'_>) {
        map.kv_simple_string(c"Type", type_name);
        self.print_optional_counters(map);
    }

    /// Print profile for a single-child iterator.
    pub fn print_single_child<C: ProfilePrint>(
        &self,
        type_name: &CStr,
        child: Option<&C>,
        map: &mut MapBuilder<'_>,
    ) {
        map.kv_simple_string(c"Type", type_name);
        self.print_optional_counters(map);

        if let Some(child) = child {
            let mut child_map = map.kv_map(c"Child iterator");
            let mut child_ctx = self.child_ctx();
            child.print_profile(&mut child_map, &mut child_ctx);
        }
    }
}

/// Format a float like C's `%g` (6 significant digits, trailing zeros trimmed).
pub fn format_g(value: f64) -> Cow<'static, str> {
    if value.is_nan() {
        let s = if value.is_sign_negative() {
            "-nan"
        } else {
            "nan"
        };
        return Cow::Borrowed(s);
    }
    if value.is_infinite() {
        let s = if value.is_sign_negative() {
            "-inf"
        } else {
            "inf"
        };
        return Cow::Borrowed(s);
    }
    // C's %g uses 6 significant digits by default, switches to scientific
    // notation when exponent < -4 or >= 6, and trims trailing zeros.
    // .5e gives 5 decimal places after the leading digit = 6 significant digits.
    let s = format!("{value:.5e}");
    // Parse mantissa and exponent from scientific notation.
    let (mantissa, exp) = s.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();

    if (-4..6).contains(&exp) {
        // Use fixed-point notation with enough decimal places for 6 sig digits.
        let decimal_places = (5 - exp).max(0) as usize;
        let fixed = format!("{value:.decimal_places$}");
        // Trim trailing zeros after decimal point.
        if fixed.contains('.') {
            let trimmed = fixed.trim_end_matches('0').trim_end_matches('.');
            Cow::Owned(trimmed.to_string())
        } else {
            Cow::Owned(fixed)
        }
    } else {
        // Use scientific notation: trim trailing zeros from mantissa.
        let trimmed = mantissa.trim_end_matches('0').trim_end_matches('.');
        Cow::Owned(format!("{trimmed}e{exp:+03}"))
    }
}
