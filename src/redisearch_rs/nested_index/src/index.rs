/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{CompoundIndex, Key, Level, SchemaError};
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

/// What indexing one document changed, for the spec's statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexDelta {
    /// Terminals created, one per new path.
    pub new_terminals: usize,
    /// Postings written: one per numeric value, one per distinct tag value per terminal.
    pub records: usize,
    /// The change, in bytes, of the postings' memory.
    pub size_delta: i64,
}

/// The index of one NESTED field. See the [crate documentation](crate).
pub struct NestedIndex {
    routing: CompoundIndex<Terminal>,
    terminal_kind: TerminalKind,
    num_records: usize,
}

impl NestedIndex {
    /// An empty index with the given routing levels and terminal kind.
    pub fn new(levels: Vec<Level>, terminal_kind: TerminalKind) -> Result<Self, SchemaError> {
        Ok(Self {
            routing: CompoundIndex::new(levels)?,
            terminal_kind,
            num_records: 0,
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

    /// The postings written and not yet collected.
    pub const fn num_records(&self) -> usize {
        self.num_records
    }

    /// Indexes every element of the document `doc_id`.
    ///
    /// Every element is checked first: its terminal value type, NaN, its path, and the cardinality
    /// limits for all the document's paths together. Only then is anything written, so an error
    /// leaves the index unchanged.
    ///
    /// `doc_id` must be greater than every doc id indexed before, as for any posting list. Several
    /// elements of one document may share a path; their values all land in the same terminal.
    pub fn index_document(
        &mut self,
        doc_id: DocId,
        elements: &[Element<'_>],
        params: IndexParams,
    ) -> Result<IndexDelta, IndexError> {
        for (position, element) in elements.iter().enumerate() {
            if element.value.kind() != self.terminal_kind.tag() {
                return Err(IndexError::ValueMismatch { element: position });
            }
            if let TerminalValue::Numeric(value) = element.value
                && value.is_nan()
            {
                return Err(IndexError::NotANumber { element: position });
            }
        }
        let paths: Vec<&[Key<'_>]> = elements.iter().map(|element| element.path).collect();
        self.routing.check_paths(&paths)?;

        let terminals_before = self.routing.num_terminals();
        let kind = self.terminal_kind;
        let mut delta = IndexDelta::default();
        for element in elements {
            // Cannot fail: `check_paths` accepted every path together.
            let terminal = self
                .routing
                .terminal_or_insert_with(element.path, || Terminal::new(kind))?;
            match (terminal, element.value) {
                (Terminal::Numeric(tree), TerminalValue::Numeric(value)) => {
                    // A NESTED element has no field expiration, and a document with several
                    // elements on one path is multi-valued in that terminal.
                    let added = tree.add(doc_id, value, false, true, params.max_depth_range);
                    delta.records += 1;
                    delta.size_delta += added.size_delta;
                }
                (Terminal::Tag(tags), TerminalValue::Tag(tag)) => {
                    let written = tags.index(&[tag], doc_id, false);
                    tags.commit(&[tag]);
                    delta.records += written.num_records as usize;
                    delta.size_delta += written.size_delta as i64;
                }
                _ => unreachable!("value kinds were checked against the terminal kind"),
            }
        }
        delta.new_terminals = self.routing.num_terminals() - terminals_before;
        self.num_records += delta.records;
        Ok(delta)
    }
}
