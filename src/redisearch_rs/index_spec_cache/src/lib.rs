/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A snapshot of an index schema that queries resolve field names against.

use std::ffi::{CStr, CString};
use std::fmt;

/// An immutable snapshot of an index schema's fields and of its rule's
/// special field names (language, score and payload).
///
/// The index and every query that resolves fields against it share the
/// snapshot as an [`Arc`](std::sync::Arc). The index builds a new snapshot
/// whenever its fields or those names change instead of modifying the shared
/// one, so a running query keeps the schema it started with.
pub struct IndexSpecCache {
    fields: Box<[CachedField]>,
    rule_special_fields: Box<[CString]>,
}

impl IndexSpecCache {
    /// Creates a snapshot of `fields`, in schema order.
    ///
    /// `rule_special_fields` holds whichever of the schema rule's language,
    /// score and payload field names are set; their roles are not recorded,
    /// as readers only test membership (see [`Self::is_rule_special_field`]).
    pub fn new<'a>(
        fields: impl IntoIterator<Item = CachedField>,
        rule_special_fields: impl IntoIterator<Item = &'a CStr>,
    ) -> Self {
        Self {
            fields: fields.into_iter().collect(),
            rule_special_fields: rule_special_fields
                .into_iter()
                .map(CStr::to_owned)
                .collect(),
        }
    }

    /// Returns the first field whose [name](CachedField::name) is `name`.
    pub fn find_field(&self, name: &CStr) -> Option<&CachedField> {
        self.fields
            .iter()
            .find(|field| field.name() == name.to_bytes())
    }

    /// Returns `true` if `name` is one of the schema rule's special fields.
    pub fn is_rule_special_field(&self, name: &CStr) -> bool {
        self.rule_special_fields
            .iter()
            .any(|special| special.as_c_str() == name)
    }
}

impl fmt::Debug for IndexSpecCache {
    /// Field names are user data, so this lists the fields without them.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexSpecCache")
            .field("fields", &self.fields)
            .finish_non_exhaustive()
    }
}

/// The parts of a schema field that queries read.
///
/// The name and path are byte strings matched by length and content, as the
/// schema's `HiddenString`s are, so a name with an interior NUL matches no
/// lookup name. `types` and `options` are the C bit sets as raw bits: each
/// reader converts them where it uses them.
pub struct CachedField {
    /// The name followed by a NUL terminator, so that [`Self::path`] can lend
    /// it when the path is the name.
    name: Box<[u8]>,
    /// The path followed by a NUL terminator, or `None` when the path is the
    /// name.
    path: Option<Box<[u8]>>,
    types: u32,
    options: u32,
    sort_idx: i16,
    ft_id: u16,
}

impl CachedField {
    /// A field named `name`, whose path is its name and whose other
    /// properties are all zero. The `with_*` methods set them.
    pub fn new(name: &[u8]) -> Self {
        Self {
            name: with_nul(name),
            path: None,
            types: 0,
            options: 0,
            sort_idx: 0,
            ft_id: 0,
        }
    }

    /// Gives the field a path of its own, in place of its name.
    pub fn with_path(mut self, path: &[u8]) -> Self {
        self.path = Some(with_nul(path));
        self
    }

    /// Sets the C `FieldSpec.types` bits.
    pub const fn with_types(mut self, types: u32) -> Self {
        self.types = types;
        self
    }

    /// Sets the C `FieldSpec.options` bits.
    pub const fn with_options(mut self, options: u32) -> Self {
        self.options = options;
        self
    }

    /// Sets the field's index in the sorting vector.
    pub const fn with_sort_idx(mut self, sort_idx: i16) -> Self {
        self.sort_idx = sort_idx;
        self
    }

    /// Sets the field's full-text field id.
    pub const fn with_ft_id(mut self, ft_id: u16) -> Self {
        self.ft_id = ft_id;
        self
    }

    /// The field's name, without a terminator.
    pub fn name(&self) -> &[u8] {
        let (_nul, name) = self.name.split_last().expect("the name is NUL-terminated");
        name
    }

    /// The field's path, or its name when it has no path of its own, up to the
    /// first NUL: what C code reading it as a C string sees.
    pub fn path(&self) -> &CStr {
        let bytes = self.path.as_deref().unwrap_or(&self.name);
        CStr::from_bytes_until_nul(bytes).expect("the path is NUL-terminated")
    }

    /// The C `FieldSpec.types` bits.
    pub const fn types(&self) -> u32 {
        self.types
    }

    /// The C `FieldSpec.options` bits.
    pub const fn options(&self) -> u32 {
        self.options
    }

    /// The field's index in the sorting vector; meaningful only for sortable
    /// fields.
    pub const fn sort_idx(&self) -> i16 {
        self.sort_idx
    }

    /// The field's full-text field id; meaningful only for full-text fields.
    pub const fn ft_id(&self) -> u16 {
        self.ft_id
    }
}

impl fmt::Debug for CachedField {
    /// Leaves out the name and path; see [`IndexSpecCache`]'s [`Debug`] impl.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedField")
            .field("types", &self.types)
            .field("options", &self.options)
            .field("sort_idx", &self.sort_idx)
            .field("ft_id", &self.ft_id)
            .finish_non_exhaustive()
    }
}

fn with_nul(bytes: &[u8]) -> Box<[u8]> {
    let mut buf = Vec::with_capacity(bytes.len() + 1);
    buf.extend_from_slice(bytes);
    buf.push(0);
    buf.into_boxed_slice()
}
