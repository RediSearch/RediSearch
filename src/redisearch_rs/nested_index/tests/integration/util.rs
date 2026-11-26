/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::num::NonZeroUsize;

use nested_index::{
    Element, FloatCompression, IndexParams, NestedIndex, TagLevel, TerminalKind, TerminalValue,
};
use tag_index::Tag;

pub const PARAMS: IndexParams = IndexParams { max_depth_range: 0 };

pub const fn tag(value: &str) -> &[u8] {
    value.as_bytes()
}

pub fn tag_value(value: &str) -> TerminalValue<'_> {
    TerminalValue::Tag(Tag::new(value.as_bytes()).unwrap())
}

/// Two tag levels, `color` then `size`, over a numeric `price` terminal.
pub fn priced(max_sizes: Option<usize>) -> NestedIndex {
    let size = match max_sizes {
        Some(max) => TagLevel::with_max_cardinality(NonZeroUsize::new(max).unwrap()),
        None => TagLevel::new(),
    };
    NestedIndex::new(
        vec![TagLevel::new(), size],
        TerminalKind::Numeric(FloatCompression::Off),
    )
    .unwrap()
}

/// A `color` tag level over a tag `material` terminal.
pub fn materials() -> NestedIndex {
    NestedIndex::new(vec![TagLevel::new()], TerminalKind::Tag).unwrap()
}

/// An element `(color, size) → price` of a [`priced`] index.
pub struct Item {
    pub path: [&'static [u8]; 2],
    pub price: f64,
}

pub const fn item(color: &'static str, size: &'static str, price: f64) -> Item {
    Item {
        path: [tag(color), tag(size)],
        price,
    }
}

pub fn elements(items: &[Item]) -> Vec<Element<'_>> {
    items
        .iter()
        .map(|item| Element {
            path: &item.path,
            value: TerminalValue::Numeric(item.price),
        })
        .collect()
}
