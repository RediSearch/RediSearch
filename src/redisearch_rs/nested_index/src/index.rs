/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{CompoundIndex, InsertError, Key, Level, SchemaError};
use rqe_core::DocId;

use crate::{IndexError, Terminal, TerminalKind, TerminalValue};

/// One element of a document, as the index stores it.
#[derive(Debug, Clone, Copy)]
pub struct Element<'a> {
    /// The element's values on the routing levels, one [`Key`] per level.
    pub path: &'a [Key<'a>],
    /// The element's value for the last attribute.
    pub value: TerminalValue<'a>,
}

/// Index-wide settings an insert needs, read from the module configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexParams {
    /// The deepest level at which a numeric range tree keeps ranges on its inner nodes, as for a
    /// NUMERIC field (`NUMERIC_TREE_MAX_DEPTH_RANGE`).
    pub max_depth_range: usize,
}

/// What indexing one document changed, in the units a top-level field reports to the spec's
/// statistics, so the caller folds it in the same way.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexDelta {
    /// Terminals created, one per new path.
    pub new_terminals: usize,
    /// The change in the number of records, as the spec counts them. For a numeric terminal this
    /// is the tree's own [`AddResult::num_records_delta`], as for a NUMERIC field, so one add can
    /// raise or lower it. For a tag terminal a document counts once per distinct (path, value)
    /// pair, as for a TAG field.
    ///
    /// [`AddResult::num_records_delta`]: numeric_range_tree::AddResult::num_records_delta
    pub records: i64,
    /// The change, in bytes, of the postings' memory, including the initial range of each new
    /// numeric terminal.
    pub size_delta: i64,
    /// The change in the number of posting-list blocks.
    pub block_delta: i64,
}

/// The index of one NESTED field. See the [crate documentation](crate).
pub struct NestedIndex {
    routing: CompoundIndex<Terminal>,
    terminal_kind: TerminalKind,
    num_records: usize,
    /// The last document indexed, `0` before the first one; doc ids start at 1.
    last_doc_id: DocId,
}

impl NestedIndex {
    /// An empty index with the given routing levels and terminal kind.
    pub fn new(levels: Vec<Level>, terminal_kind: TerminalKind) -> Result<Self, SchemaError> {
        Ok(Self {
            routing: CompoundIndex::new(levels)?,
            terminal_kind,
            num_records: 0,
            last_doc_id: 0,
        })
    }

    /// The routing levels and their terminals, for lookups and query walks.
    pub const fn routing(&self) -> &CompoundIndex<Terminal> {
        &self.routing
    }

    /// The kind of every terminal.
    pub const fn terminal_kind(&self) -> TerminalKind {
        self.terminal_kind
    }

    /// The records written and not yet collected, counted as [`IndexDelta::records`] counts them.
    pub const fn num_records(&self) -> usize {
        self.num_records
    }

    /// Indexes every element of the document `doc_id`.
    ///
    /// Everything is checked first: `doc_id` must be greater than every doc id indexed before, as
    /// for any posting list; each element's value must have the terminal's type and not be NaN;
    /// and all the document's paths must route and fit the cardinality limits together. Only then
    /// is anything written, so an error leaves the index unchanged.
    ///
    /// Several elements of one document may share a path; their values all land in the same
    /// terminal.
    pub fn index_document(
        &mut self,
        doc_id: DocId,
        elements: &[Element<'_>],
        params: IndexParams,
    ) -> Result<IndexDelta, IndexError> {
        debug_assert_ne!(doc_id, 0, "doc ids start at 1");
        if doc_id <= self.last_doc_id {
            return Err(IndexError::DocIdNotIncreasing {
                doc_id,
                last: self.last_doc_id,
            });
        }
        for (position, element) in elements.iter().enumerate() {
            if !self.terminal_kind.accepts(&element.value) {
                return Err(IndexError::ValueMismatch { element: position });
            }
            if let TerminalValue::Numeric(value) = element.value
                && value.is_nan()
            {
                return Err(IndexError::NotANumber { element: position });
            }
        }
        // Checked one by one first, so a path error names its element.
        for (position, element) in elements.iter().enumerate() {
            self.routing
                .get(element.path)
                .map_err(|error| IndexError::Path {
                    element: position,
                    error,
                })?;
        }
        let paths: Vec<&[Key<'_>]> = elements.iter().map(|element| element.path).collect();
        self.routing
            .check_paths(&paths)
            .map_err(|error| match error {
                InsertError::CardinalityExceeded { level, max } => {
                    IndexError::CardinalityExceeded { level, max }
                }
                InsertError::Path(_) => unreachable!("every path was routed above"),
            })?;

        let kind = self.terminal_kind;
        let mut delta = IndexDelta::default();
        for element in elements {
            let mut created = false;
            let terminal = self
                .routing
                .get_or_insert_with(element.path, || {
                    created = true;
                    Terminal::new(kind)
                })
                .expect("check_paths accepted every path of the document together");
            if created {
                delta.new_terminals += 1;
            }
            match (terminal, element.value) {
                (Terminal::Numeric(tree), TerminalValue::Numeric(value)) => {
                    if created {
                        // A new tree starts with one empty range, which a NUMERIC field counts
                        // when it creates its tree.
                        delta.size_delta += tree.inverted_indexes_size() as i64;
                    }
                    // A NESTED element has no field expiration. Several elements of one document
                    // may share a terminal, which makes the document multi-valued there; doc ids
                    // increase across documents, checked above, so accepting a repeated doc id
                    // admits nothing else.
                    let added = tree.add(doc_id, value, false, true, params.max_depth_range);
                    delta.records += i64::from(added.num_records_delta);
                    delta.size_delta += added.size_delta;
                    delta.block_delta += i64::from(added.block_count_delta);
                }
                (Terminal::Tag(tags), TerminalValue::Tag(tag)) => {
                    let written = tags.index(&[tag], doc_id, false);
                    tags.commit(&[tag]);
                    delta.records += i64::from(written.num_records);
                    delta.size_delta += written.size_delta as i64;
                    delta.block_delta += i64::from(written.blocks_added);
                }
                _ => unreachable!("value types were checked against the terminal kind"),
            }
        }
        // The tree's own records never drop below zero, so neither does their sum.
        self.num_records = self
            .num_records
            .checked_add_signed(delta.records as isize)
            .expect("the record count stays non-negative");
        self.last_doc_id = doc_id;
        Ok(delta)
    }
}
