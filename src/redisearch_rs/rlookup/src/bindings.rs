/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bindings and wrapper types for as-of-yet unported types and modules. This makes the actual RLookup code cleaner and safer
//! isolating much of the unsafe FFI code.

mod field_spec;
mod hidden_string_ref;
mod index_spec;
mod index_spec_cache;
mod rm_array;
mod rs_array;
mod schema_rule;

#[cfg(test)]
#[cfg_attr(miri, allow(unused))]
pub use field_spec::FieldSpecBuilder;
pub use field_spec::{FieldSpec, FieldSpecOption, FieldSpecOptions, FieldSpecType, FieldSpecTypes};
pub use hidden_string_ref::HiddenStringRef;
pub use index_spec::IndexSpec;
pub use index_spec_cache::IndexSpecCache;
pub use rm_array::RmArray;
#[cfg(test)]
pub use rs_array::rs_array;
pub use schema_rule::SchemaRule;
