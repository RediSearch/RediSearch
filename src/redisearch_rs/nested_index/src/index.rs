/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::BTreeSet;

use compound_index::{CardinalityExceeded, CompoundIndex, SchemaError, TagLevel};
use rqe_core::DocId;

use crate::terminal::PostingsMut;
use crate::{IndexError, Terminal, TerminalKind, TerminalValue};

/// One element of a document, as the index stores it.
#[derive(Debug, Clone, Copy)]
pub struct Element<'a> {
    /// The element's tag values on the routing levels, one per level, already normalised by the
    /// caller (case folding).
    pub path: &'a [&'a [u8]],
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
    /// The terminals a collector reached and has not finished; see
    /// [`NestedIndex::finish_collection`].
    collected: BTreeSet<Vec<Vec<u8>>>,
}

/// What [`NestedIndex::finish_collection`] freed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PrunedTerminals {
    /// The terminals dropped.
    pub terminals: usize,
    /// The bytes their emptied postings still held, counted as [`IndexDelta::size_delta`]
    /// counted them when they were written.
    pub bytes: usize,
    /// The blocks their emptied postings still held.
    pub blocks: usize,
}

impl NestedIndex {
    /// An empty index with the given routing levels and terminal kind.
    pub fn new(levels: Vec<TagLevel>, terminal_kind: TerminalKind) -> Result<Self, SchemaError> {
        Ok(Self {
            routing: CompoundIndex::new(levels)?,
            terminal_kind,
            num_records: 0,
            collected: BTreeSet::new(),
        })
    }

    /// Lets a garbage collector remove postings from the terminal at `path`, as
    /// [`CompoundIndex::for_each_terminal`] reported it: `collect` returns its result and the
    /// records it removed, which leave [`NestedIndex::num_records`]. The terminal is kept even
    /// if emptied, so a collector working through it in several passes still finds it;
    /// [`NestedIndex::finish_collection`] drops it. `None` when no terminal is at `path`.
    pub fn collect_terminal<R>(
        &mut self,
        path: &[Vec<u8>],
        collect: impl FnOnce(&mut Terminal) -> (R, usize),
    ) -> Option<R> {
        let terminal = self.routing.terminal_at_mut(path)?;
        let (result, records_removed) = collect(terminal);
        self.forget_records(records_removed);
        self.collected.insert(path.to_vec());
        Some(result)
    }

    /// Whether terminals collected through [`NestedIndex::collect_terminal`] wait for
    /// [`NestedIndex::finish_collection`].
    pub fn has_collected(&self) -> bool {
        !self.collected.is_empty()
    }

    /// Ends the collection of up to `max` of the terminals [`NestedIndex::collect_terminal`]
    /// reached: `finish` sees each, for instance to compact it, then the ones left without
    /// records are dropped, with the routing nodes left without terminals. Each dropped terminal
    /// revokes its [`Terminal::alive`] token, so iterators reading it stop. Bounding the work
    /// lets a collector spread it over several lock holds, calling this again while
    /// [`NestedIndex::has_collected`].
    ///
    /// The terminals reached stay listed until finished, so a collection interrupted before its
    /// finish is finished by the next one.
    pub fn finish_collection(
        &mut self,
        max: usize,
        mut finish: impl FnMut(&mut Terminal),
    ) -> PrunedTerminals {
        let mut pruned = PrunedTerminals::default();
        for _ in 0..max {
            let Some(path) = self.collected.first().cloned() else {
                break;
            };
            let removed = self.routing.remove_terminal_if(&path, |terminal| {
                finish(terminal);
                if !terminal.is_empty() {
                    return false;
                }
                pruned.bytes += terminal.postings_size();
                pruned.blocks += terminal.num_blocks();
                true
            });
            pruned.terminals += usize::from(removed);
            // Only now, so a `finish` that panics leaves the terminal listed for the next call.
            self.collected.remove(&path);
        }
        pruned
    }

    /// The terminal at `path`, for a test to write to behind the index's back, as a writer
    /// racing a collector would.
    #[cfg(feature = "unittest")]
    pub fn terminal_at_mut(&mut self, path: &[Vec<u8>]) -> Option<&mut Terminal> {
        self.routing.terminal_at_mut(path)
    }

    /// Takes `records` off [`NestedIndex::num_records`], once a garbage collector removed them.
    ///
    /// A collector removes only records that were written, so taking off more is an accounting
    /// bug. It fails a debug build; a release build, where it runs inside the server, logs it and
    /// stops at zero rather than crash over a statistic.
    fn forget_records(&mut self, records: usize) {
        debug_assert!(
            records <= self.num_records,
            "a collector removes only records that were written"
        );
        self.num_records = self.num_records.checked_sub(records).unwrap_or_else(|| {
            tracing::warn!(
                removed = records,
                counted = self.num_records,
                "a NESTED field's GC removed more records than it counted"
            );
            0
        });
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

    /// Checks every element of a document without writing anything: each element's value must
    /// have the terminal's type and not be NaN, and all the document's paths must fit the
    /// cardinality limits together.
    ///
    /// [`NestedIndex::index_document`] runs the same checks, so a document this accepts is
    /// written whole, as long as nothing is indexed in between. Without a cardinality limit the
    /// checks read the levels only, never the indexed paths, so the outcome holds whatever is
    /// indexed in between. A caller that must reject a document before writing its other fields
    /// calls this first.
    ///
    /// # Panics
    ///
    /// If an element's path does not have one value per routing level.
    pub fn check_document(&self, elements: &[Element<'_>]) -> Result<(), IndexError> {
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
        let paths: Vec<&[&[u8]]> = elements.iter().map(|element| element.path).collect();
        self.routing
            .check_paths(&paths)
            .map_err(
                |CardinalityExceeded { level, max }| IndexError::CardinalityExceeded { level, max },
            )
    }

    /// Indexes every element of the document `doc_id`.
    ///
    /// `doc_id` must be greater than every doc id indexed before, as for any posting list; the
    /// doc table guarantees it. Everything else is checked first: the elements must pass
    /// [`NestedIndex::check_document`]. Only then is anything written, so an error leaves the
    /// index unchanged.
    ///
    /// Several elements of one document may share a path; their values all land in the same
    /// terminal.
    ///
    /// # Panics
    ///
    /// If an element's path does not have one value per routing level.
    pub fn index_document(
        &mut self,
        doc_id: DocId,
        elements: &[Element<'_>],
        params: IndexParams,
    ) -> Result<IndexDelta, IndexError> {
        self.check_document(elements)?;

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
            match (terminal.postings_mut(), element.value) {
                (PostingsMut::Numeric(tree), TerminalValue::Numeric(value)) => {
                    if created {
                        // A new tree starts with one empty range, which a NUMERIC field counts
                        // when it creates its tree.
                        delta.size_delta += tree.inverted_indexes_size() as i64;
                    }
                    // A NESTED element has no field expiration. Several elements of one document
                    // may share a terminal, which makes the document multi-valued there; doc ids
                    // increase across documents, so accepting a repeated doc id
                    // admits nothing else.
                    let added = tree.add(doc_id, value, false, true, params.max_depth_range);
                    delta.records += i64::from(added.num_records_delta);
                    delta.size_delta += added.size_delta;
                    delta.block_delta += i64::from(added.block_count_delta);
                }
                (PostingsMut::Tag(tags), TerminalValue::Tag(tag)) => {
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
        Ok(delta)
    }
}
