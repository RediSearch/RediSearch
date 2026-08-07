/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Field/index type definitions shared between the AST and the lowering pass.
//! The mutable "live model" used while lowering lives in `lower.rs`.

/// Vector element type. Kept in the live model because lowering needs the byte
/// size when rendering a document's vector blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VecType {
    Float32,
    Float64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Text,
    Numeric,
    Tag,
    Geo,
    Vector { data: VecType, dim: usize },
}

impl FieldKind {
    pub fn same_kind(&self, other: &FieldKind) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// A live index in the model: enough to resolve query references and render
/// type-appropriate values.
#[derive(Debug, Clone)]
pub struct IndexModel {
    pub name: String,
    pub on_json: bool,
    pub prefix: String,
    pub fields: Vec<(String, FieldKind)>,
}

impl IndexModel {
    /// Names of all fields of the given kind.
    pub fn fields_of(&self, kind: FieldKind) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|(_, k)| k.same_kind(&kind))
            .map(|(n, _)| n.as_str())
            .collect()
    }

    pub fn vector_field(&self) -> Option<(&str, VecType, usize)> {
        self.fields.iter().find_map(|(n, k)| match k {
            FieldKind::Vector { data, dim } => Some((n.as_str(), *data, *dim)),
            _ => None,
        })
    }
}
