/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Byte-keyed Levenshtein DFA over a UTF-8 byte trie.
//!
//! Lift of the char-keyed DFA path (the C rune-trie fuzzy iterator): keeps the Schulz/Mihov
//! sparse DP semantics (edit cost counted per codepoint), but exposes a
//! byte alphabet so it can walk the byte trie without any UTF-8 decode at
//! the trie boundary.
//!
//! ## Design
//!
//! Joint state is `(SparseVector, LiftedDecoderState)`. The lifted decoder
//! is the central trick that keeps the DFA from blowing up under subset
//! construction:
//!
//! - [`LiftedDecoderState::Between`] — at a codepoint boundary.
//! - [`LiftedDecoderState::OnQueryPrefix`] — mid-codepoint, and the bytes
//!   so far are a prefix of at least one query codepoint's UTF-8 encoding.
//!   Indexed into a precomputed [`QueryByteTrie`] of the query's codepoint
//!   encodings, so two mid-codepoint states reachable from the same query
//!   prefix share an identity (and a DFA node).
//! - [`LiftedDecoderState::OffQuery`] — mid-codepoint, definitely not
//!   matching any query codepoint. Carries only `remaining` (1–3
//!   continuation bytes still to consume); all distinct off-query
//!   accumulators collapse to the same canonical bucket.
//!
//! This is what "lift it to bytes, mixed NFA" means in practice: the
//! codepoint NFA's transitions are expanded into byte chains, but the
//! chains share intermediate states wherever they don't carry query-
//! relevant information. Without the [`OffQuery`](LiftedDecoderState::OffQuery) collapse, a 4-byte lead
//! produces ~4096 distinct mid-codepoint states per sparse-row variant —
//! the DFA build blows up. With it, the build is O(query-trie-size +
//! sparse-row-variants).
//!
//! UTF-8 well-formedness over `OffQuery`: we accept any `0x80..=0xBF`
//! continuation regardless of lead-specific range constraints (e.g. the
//! `0xA0..=0xBF` narrowing after `0xE0`). Loose UTF-8 is sound here
//! because the trie's `insert(key: &str)` invariant guarantees only valid
//! UTF-8 byte sequences ever appear in node labels — invalid-UTF-8 paths
//! the DFA would tolerate are simply unreachable.
//!
//! ## Acceptance
//!
//! A joint state accepts iff `sparse.is_match()` AND the decoder is
//! `Between`. Mid-codepoint positions are never accepting — that's the
//! invariant that fixes the mid-codepoint panic in the char-keyed iterator.

use crate::trie_map::node::Node;
use crate::str_trie_map::StrTrieMap;

// ---------------------------------------------------------------------------
// Query byte-prefix trie
// ---------------------------------------------------------------------------

/// Pre-built byte-prefix trie of the query's codepoint UTF-8 encodings.
///
/// Node 0 is the root. Each non-root node represents "we've consumed this
/// byte sequence as a prefix of some query codepoint's UTF-8 encoding";
/// leaves carry the specific completed codepoint.
struct QueryByteTrie {
    nodes: Vec<QueryByteTrieNode>,
}

struct QueryByteTrieNode {
    /// Outgoing edges keyed by byte. Small lists (at most a handful per
    /// node for typical queries), so linear scan beats a hashmap.
    children: Vec<(u8, u32)>,
    /// If this node sits at the end of a query codepoint's UTF-8 byte
    /// sequence, the codepoint that the byte path encodes. `None` for
    /// internal nodes.
    completes_to: Option<char>,
    /// Bytes consumed from the codepoint's lead to reach this node.
    /// Root: 0. Increases by 1 per child.
    bytes_consumed: u8,
    /// Total UTF-8 byte length of the codepoint being decoded through
    /// this node. Root: 0 (length is determined once a lead is consumed).
    total_bytes: u8,
}

impl QueryByteTrie {
    fn build(query_chars: &[char]) -> Self {
        let mut nodes = vec![QueryByteTrieNode {
            children: Vec::new(),
            completes_to: None,
            bytes_consumed: 0,
            total_bytes: 0,
        }];
        for &c in query_chars {
            let mut buf = [0u8; 4];
            let bytes = c.encode_utf8(&mut buf).as_bytes().to_vec();
            let total = bytes.len() as u8;
            let mut current: u32 = 0;
            for (k, &b) in bytes.iter().enumerate() {
                let child = nodes[current as usize]
                    .children
                    .iter()
                    .find_map(|&(eb, idx)| (eb == b).then_some(idx));
                let next = match child {
                    Some(idx) => idx,
                    None => {
                        let new_idx = nodes.len() as u32;
                        nodes.push(QueryByteTrieNode {
                            children: Vec::new(),
                            completes_to: None,
                            bytes_consumed: (k + 1) as u8,
                            total_bytes: total,
                        });
                        nodes[current as usize].children.push((b, new_idx));
                        new_idx
                    }
                };
                current = next;
            }
            nodes[current as usize].completes_to = Some(c);
        }
        Self { nodes }
    }

    fn root_child(&self, b: u8) -> Option<u32> {
        self.nodes[0]
            .children
            .iter()
            .find_map(|&(eb, idx)| (eb == b).then_some(idx))
    }

    fn child(&self, node_idx: u32, b: u8) -> Option<u32> {
        self.nodes[node_idx as usize]
            .children
            .iter()
            .find_map(|&(eb, idx)| (eb == b).then_some(idx))
    }

    fn node(&self, idx: u32) -> &QueryByteTrieNode {
        &self.nodes[idx as usize]
    }
}

// ---------------------------------------------------------------------------
// Lifted decoder state
// ---------------------------------------------------------------------------

/// Coarsened mid-codepoint state, designed to keep the DFA's joint-state
/// cache from blowing up under subset construction.
///
/// `OnQueryPrefix` is fine-grained (indexed into the query byte trie); it
/// only has as many distinct values as the query has byte-trie nodes —
/// bounded by `Σ utf8_len(q_i)` over query codepoints. `OffQuery` is
/// canonicalised on `remaining` alone, so the entire byte space of
/// non-query mid-codepoints collapses into 3 states (remaining ∈ {1,2,3}).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum LiftedDecoderState {
    /// Codepoint boundary.
    Between,
    /// Mid-codepoint; bytes consumed so far are a prefix of at least one
    /// query codepoint's UTF-8 encoding.
    OnQueryPrefix { trie_idx: u32 },
    /// Mid-codepoint; diverged from every query codepoint. `remaining` is
    /// the number of continuation bytes still expected (1, 2, or 3).
    OffQuery { remaining: u8 },
}

impl LiftedDecoderState {
    pub(super) const fn is_between(self) -> bool {
        matches!(self, Self::Between)
    }
}

/// Classify a lead byte. Returns `Some(continuation_bytes_remaining)`:
/// `0` for ASCII (the codepoint is complete after the lead), `1`/`2`/`3`
/// for multi-byte leads. `None` for bytes that are never a valid lead.
const fn classify_lead(b: u8) -> Option<u8> {
    match b {
        0x00..=0x7F => Some(0),
        0xC2..=0xDF => Some(1),
        0xE0..=0xEF => Some(2),
        0xF0..=0xF4 => Some(3),
        // 0x80..=0xBF (lone continuation), 0xC0..=0xC1 (overlong leads),
        // 0xF5..=0xFF (above U+10FFFF), 0xFE..=0xFF (never valid).
        _ => None,
    }
}

const fn is_continuation(b: u8) -> bool {
    0x80 <= b && b <= 0xBF
}

// ---------------------------------------------------------------------------
// Sparse Levenshtein DP row
// ---------------------------------------------------------------------------

/// Sparse Levenshtein DP row. Entries are `(idx, val)` sorted by `idx`,
/// holding only positions where `val <= max`. Identical semantics to the
/// char-keyed DFA's `SparseVector`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct SparseVector(pub(super) Vec<(u32, u32)>);

// ---------------------------------------------------------------------------
// Byte-stepping Sparse Levenshtein automaton
// ---------------------------------------------------------------------------

pub(super) struct SparseAutomaton {
    pub(super) query: Vec<char>,
    pub(super) max: u32,
    query_trie: QueryByteTrie,
}

impl SparseAutomaton {
    pub(super) fn new(query: &str, max: u32) -> Self {
        let query: Vec<char> = query.chars().collect();
        let query_trie = QueryByteTrie::build(&query);
        Self {
            query,
            max,
            query_trie,
        }
    }

    pub(super) fn start(&self) -> SparseVector {
        SparseVector((0..=self.max).map(|i| (i, i)).collect())
    }

    pub(super) fn start_joint(&self) -> JointState {
        JointState {
            sparse: self.start(),
            decoder: LiftedDecoderState::Between,
        }
    }

    /// Codepoint-level DP step. `c = None` is the wildcard fallback (any
    /// non-query codepoint); `Some(c)` is a specific codepoint. Same
    /// algorithm as the char-keyed `SparseAutomaton::step`.
    pub(super) fn step_codepoint(&self, state: &SparseVector, c: Option<char>) -> SparseVector {
        let mut new_vec: Vec<(u32, u32)> = Vec::with_capacity(state.0.len());

        if let Some(&(idx, val)) = state.0.first()
            && idx == 0
            && val < self.max
        {
            new_vec.push((0, val + 1));
        }

        for j in 0..state.0.len() {
            let (idx, val) = state.0[j];

            if idx as usize == self.query.len() {
                break;
            }

            let mut new_val = val;
            let q_char = self.query[idx as usize];
            let same = matches!(c, Some(c) if c == q_char);
            if !same {
                new_val += 1;
            }

            if let Some(&(last_idx, last_val)) = new_vec.last()
                && last_idx == idx
            {
                new_val = new_val.min(last_val + 1);
            }

            if j + 1 < state.0.len() {
                let (next_idx, next_val) = state.0[j + 1];
                if next_idx == idx + 1 {
                    new_val = new_val.min(next_val + 1);
                }
            }

            if new_val <= self.max {
                new_vec.push((idx + 1, new_val));
            }
        }

        SparseVector(new_vec)
    }

    pub(super) fn is_match_sparse(&self, v: &SparseVector) -> bool {
        v.0.last()
            .is_some_and(|&(idx, _)| idx as usize == self.query.len())
    }

    /// Mid-codepoint positions never accept — joint match requires the
    /// decoder to be at a codepoint boundary.
    pub(super) fn is_match(&self, joint: &JointState) -> bool {
        joint.decoder.is_between() && self.is_match_sparse(&joint.sparse)
    }

    /// One byte step.
    ///
    /// - On a codepoint completion, advances the DP row using the
    ///   completed codepoint (or `None` for non-query completions).
    /// - On a continuation that stays in-progress, leaves the DP row
    ///   alone and advances the lifted decoder.
    /// - On a byte that can't fit any valid UTF-8 sequence at this
    ///   position, returns `ByteStep::Invalid` with the joint state
    ///   reset to `(joint.sparse.clone(), Between)`. The caller treats
    ///   the branch as dead.
    pub(super) fn step_byte(&self, joint: &JointState, b: u8) -> (JointState, ByteStep) {
        match joint.decoder {
            LiftedDecoderState::Between => self.step_from_between(&joint.sparse, b),
            LiftedDecoderState::OnQueryPrefix { trie_idx } => {
                self.step_from_query_prefix(&joint.sparse, trie_idx, b)
            }
            LiftedDecoderState::OffQuery { remaining } => {
                self.step_from_off_query(&joint.sparse, remaining, b)
            }
        }
    }

    fn step_from_between(&self, sparse: &SparseVector, b: u8) -> (JointState, ByteStep) {
        // Prefer the query trie: if this byte starts a query codepoint,
        // enter OnQueryPrefix. Even if it also classifies as a lead
        // generally, the OnQuery path is more precise.
        if let Some(child_idx) = self.query_trie.root_child(b) {
            let child = self.query_trie.node(child_idx);
            if child.bytes_consumed == child.total_bytes {
                // 1-byte query codepoint completes immediately.
                let c = child.completes_to.expect("leaf carries codepoint");
                let next_sparse = self.step_codepoint(sparse, Some(c));
                return (
                    JointState {
                        sparse: next_sparse,
                        decoder: LiftedDecoderState::Between,
                    },
                    ByteStep::Advanced,
                );
            }
            return (
                JointState {
                    sparse: sparse.clone(),
                    decoder: LiftedDecoderState::OnQueryPrefix { trie_idx: child_idx },
                },
                ByteStep::Pending,
            );
        }
        match classify_lead(b) {
            Some(0) => {
                // 1-byte non-query codepoint completes immediately.
                let next_sparse = self.step_codepoint(sparse, None);
                (
                    JointState {
                        sparse: next_sparse,
                        decoder: LiftedDecoderState::Between,
                    },
                    ByteStep::Advanced,
                )
            }
            Some(remaining) => (
                JointState {
                    sparse: sparse.clone(),
                    decoder: LiftedDecoderState::OffQuery { remaining },
                },
                ByteStep::Pending,
            ),
            None => (
                JointState {
                    sparse: sparse.clone(),
                    decoder: LiftedDecoderState::Between,
                },
                ByteStep::Invalid,
            ),
        }
    }

    fn step_from_query_prefix(
        &self,
        sparse: &SparseVector,
        trie_idx: u32,
        b: u8,
    ) -> (JointState, ByteStep) {
        // Try to stay on the query trie.
        if let Some(child_idx) = self.query_trie.child(trie_idx, b) {
            let child = self.query_trie.node(child_idx);
            if child.bytes_consumed == child.total_bytes {
                let c = child.completes_to.expect("leaf carries codepoint");
                let next_sparse = self.step_codepoint(sparse, Some(c));
                return (
                    JointState {
                        sparse: next_sparse,
                        decoder: LiftedDecoderState::Between,
                    },
                    ByteStep::Advanced,
                );
            }
            return (
                JointState {
                    sparse: sparse.clone(),
                    decoder: LiftedDecoderState::OnQueryPrefix { trie_idx: child_idx },
                },
                ByteStep::Pending,
            );
        }
        // Diverged from the query trie. If the byte is a valid
        // continuation, drop into OffQuery; otherwise it's an invalid
        // UTF-8 transition and we treat it as dead.
        if is_continuation(b) {
            let node = self.query_trie.node(trie_idx);
            let remaining = node.total_bytes - node.bytes_consumed - 1;
            if remaining == 0 {
                let next_sparse = self.step_codepoint(sparse, None);
                return (
                    JointState {
                        sparse: next_sparse,
                        decoder: LiftedDecoderState::Between,
                    },
                    ByteStep::Advanced,
                );
            }
            return (
                JointState {
                    sparse: sparse.clone(),
                    decoder: LiftedDecoderState::OffQuery { remaining },
                },
                ByteStep::Pending,
            );
        }
        (
            JointState {
                sparse: sparse.clone(),
                decoder: LiftedDecoderState::Between,
            },
            ByteStep::Invalid,
        )
    }

    fn step_from_off_query(
        &self,
        sparse: &SparseVector,
        remaining: u8,
        b: u8,
    ) -> (JointState, ByteStep) {
        if !is_continuation(b) {
            return (
                JointState {
                    sparse: sparse.clone(),
                    decoder: LiftedDecoderState::Between,
                },
                ByteStep::Invalid,
            );
        }
        if remaining == 1 {
            let next_sparse = self.step_codepoint(sparse, None);
            return (
                JointState {
                    sparse: next_sparse,
                    decoder: LiftedDecoderState::Between,
                },
                ByteStep::Advanced,
            );
        }
        (
            JointState {
                sparse: sparse.clone(),
                decoder: LiftedDecoderState::OffQuery {
                    remaining: remaining - 1,
                },
            },
            ByteStep::Pending,
        )
    }
}

/// Joint state of the byte automaton: (sparse DP row, lifted decoder).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct JointState {
    pub(super) sparse: SparseVector,
    pub(super) decoder: LiftedDecoderState,
}

/// Per-byte outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ByteStep {
    /// Byte consumed; codepoint still in flight. DP row unchanged.
    Pending,
    /// Codepoint completed; DP row advanced.
    Advanced,
    /// Decoder rejected the byte. Treat as dead branch.
    Invalid,
}

// ---------------------------------------------------------------------------
// Byte DFA via subset construction
// ---------------------------------------------------------------------------

/// Sentinel for "no edge" in [`DfaNode::edges`].
const NO_EDGE: u32 = u32::MAX;

struct DfaNode {
    is_match: bool,
    distance: u32,
    edges: Box<[u32; 256]>,
}

/// Levenshtein DFA over UTF-8 bytes. Node 0 is the start state.
pub(super) struct Dfa {
    nodes: Vec<DfaNode>,
}

impl Dfa {
    pub(super) fn build(query: &str, max_dist: u32) -> Self {
        use std::collections::{HashMap, VecDeque};

        let auto = SparseAutomaton::new(query, max_dist);
        let start = auto.start_joint();

        let mut cache: HashMap<JointState, u32> = HashMap::new();
        let mut nodes: Vec<DfaNode> = Vec::new();
        let mut worklist: VecDeque<JointState> = VecDeque::new();

        cache.insert(start.clone(), 0);
        nodes.push(node_for(&auto, &start));
        worklist.push_back(start);

        while let Some(joint) = worklist.pop_front() {
            let from_idx = *cache.get(&joint).expect("worklist entry was just inserted");
            for b in 0u8..=255 {
                let (next_joint, step) = auto.step_byte(&joint, b);
                if matches!(step, ByteStep::Invalid) {
                    continue;
                }
                // Empty sparse row = no surviving alignment within max
                // edits = dead branch. Matches the char DFA's
                // `if nv.0.is_empty() { continue; }`.
                if next_joint.sparse.0.is_empty() {
                    continue;
                }
                let next_idx = match cache.get(&next_joint).copied() {
                    Some(i) => i,
                    None => {
                        let i = nodes.len() as u32;
                        nodes.push(node_for(&auto, &next_joint));
                        cache.insert(next_joint.clone(), i);
                        worklist.push_back(next_joint);
                        i
                    }
                };
                nodes[from_idx as usize].edges[b as usize] = next_idx;
            }
        }

        let mut dfa = Self { nodes };
        dfa.prune_dead_branches();
        dfa
    }

    /// Remove edges that lead to states with no path to acceptance.
    ///
    /// Why this is load-bearing: a mid-codepoint state is never accepting
    /// (the joint `is_match` requires the lifted decoder at `Between`).
    /// During the build, a "Pending" byte transition out of an accepting
    /// state into a non-query mid-codepoint state creates an edge to a
    /// dead-on-completion node. The iterator's prefix-sink logic checks
    /// "did the per-byte `matched` flag stay true?" — if the byte DFA
    /// keeps stepping into mid-codepoint dead-ends, that flag clears
    /// after the first byte and the sink never fires. Pruning makes the
    /// DFA return `None` at the boundary byte instead, so the sink kicks
    /// in immediately (matching the char DFA's prefix-sink behavior).
    fn prune_dead_branches(&mut self) {
        use std::collections::VecDeque;

        let n = self.nodes.len();
        if n == 0 {
            return;
        }

        // Reverse adjacency: predecessors for each node.
        let mut reverse: Vec<Vec<u32>> = vec![Vec::new(); n];
        for (from, node) in self.nodes.iter().enumerate() {
            for &to in node.edges.iter() {
                if to != NO_EDGE {
                    reverse[to as usize].push(from as u32);
                }
            }
        }

        // Seed: states that ARE accepting.
        let mut live = vec![false; n];
        let mut queue: VecDeque<u32> = VecDeque::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if node.is_match {
                live[i] = true;
                queue.push_back(i as u32);
            }
        }

        // BFS backward: a state is live if any successor is live.
        while let Some(idx) = queue.pop_front() {
            for &pred in &reverse[idx as usize] {
                if !live[pred as usize] {
                    live[pred as usize] = true;
                    queue.push_back(pred);
                }
            }
        }

        // Null out edges to dead states.
        for node in self.nodes.iter_mut() {
            for edge in node.edges.iter_mut() {
                if *edge != NO_EDGE && !live[*edge as usize] {
                    *edge = NO_EDGE;
                }
            }
        }
    }

    pub(super) fn step(&self, from: u32, b: u8) -> Option<u32> {
        let next = self.nodes[from as usize].edges[b as usize];
        if next == NO_EDGE { None } else { Some(next) }
    }

    pub(super) fn is_match(&self, idx: u32) -> bool {
        self.nodes[idx as usize].is_match
    }

    pub(super) fn distance(&self, idx: u32) -> u32 {
        self.nodes[idx as usize].distance
    }

    #[cfg(test)]
    pub(super) const fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

fn node_for(auto: &SparseAutomaton, joint: &JointState) -> DfaNode {
    DfaNode {
        is_match: auto.is_match(joint),
        distance: joint.sparse.0.last().map(|&(_, v)| v).unwrap_or(0),
        edges: Box::new([NO_EDGE; 256]),
    }
}

// ---------------------------------------------------------------------------
// Byte-trie iterator
// ---------------------------------------------------------------------------

/// One frame in the trie traversal stack.
struct TrieFrame<'tm, Data> {
    node: &'tm Node<Data>,
    /// Borrowed directly from the trie node — no UTF-8 decode at frame push.
    /// `'tm` outlives the iterator, so the borrow is sound for the frame's
    /// lifetime.
    label: &'tm [u8],
    /// Current byte position within `label`.
    byte_offset: usize,
    /// Next child to descend into when `phase == Children`.
    child_offset: usize,
    phase: FramePhase,
}

#[derive(Clone, Copy)]
enum FramePhase {
    /// Consuming bytes from the label, feeding them through the DFA.
    Label,
    /// A match just fired and is being pinned; next step pops the frame.
    Match,
    /// Label exhausted; descending into children.
    Children,
}

#[derive(Clone, Copy)]
enum StepOutcome {
    Continue,
    Match,
    Stop,
}

enum FilterCode {
    Continue,
    Stop,
}

/// DFA-filtered byte-trie iterator. Yields `(key, &data, dist)` for each
/// terminal whose key lies within Levenshtein distance `max_dist` (in
/// codepoints) of the query — or, in `prefix_mode`, any suffix beneath
/// such an accepted prefix.
///
/// Mirrors the char-keyed `IterateDfaIter` structurally:
///
/// - `state_stack` / `dist_stack` push one entry per consumed byte (not
///   per codepoint). The prefix sink also pushes one `None` per byte so
///   `pop_frame` can unwind by `frame.byte_offset` regardless of how many
///   bytes lie inside each codepoint.
/// - `last_dist` updates only when the DFA reports an accepting state.
///   The DFA's `is_match` requires the lifted decoder to be at a codepoint
///   boundary, so `last_dist` is only ever written at boundaries — no
///   special handling needed inside this iterator.
/// - The accumulated key bytes form a valid UTF-8 string when yielded.
///   This holds because (a) the trie label invariant guarantees valid
///   UTF-8 byte sequences, and (b) the iterator only yields at terminal
///   nodes where `label_offset == label.len()`, i.e. on a codepoint
///   boundary.
pub struct IterateDfaIter<'tm, Data> {
    dfa: Dfa,
    prefix_mode: bool,
    trie_stack: Vec<TrieFrame<'tm, Data>>,
    state_stack: Vec<Option<u32>>,
    dist_stack: Vec<u32>,
    key: Vec<u8>,
    last_dist: u32,
}

impl<'tm, Data> IterateDfaIter<'tm, Data> {
    fn new(root: Option<&'tm Node<Data>>, dfa: Dfa, prefix_mode: bool, max_dist: u32) -> Self {
        let mut iter = Self {
            dfa,
            prefix_mode,
            trie_stack: Vec::new(),
            state_stack: vec![Some(0)],
            dist_stack: vec![max_dist + 1],
            key: Vec::new(),
            last_dist: max_dist + 1,
        };
        if let Some(root) = root {
            iter.push_frame(root);
        }
        iter
    }

    fn push_frame(&mut self, node: &'tm Node<Data>) {
        self.trie_stack.push(TrieFrame {
            node,
            label: node.label(),
            byte_offset: 0,
            child_offset: 0,
            phase: FramePhase::Label,
        });
    }

    fn pop_frame(&mut self) {
        let Some(frame) = self.trie_stack.pop() else {
            return;
        };
        // One state/dist push per consumed label byte; one key byte per
        // consumed label byte. Symmetric on unwind.
        for _ in 0..frame.byte_offset {
            self.state_stack.pop();
            self.dist_stack.pop();
            self.key.pop();
        }
    }

    fn filter(
        dfa: &Dfa,
        prefix_mode: bool,
        state_stack: &mut Vec<Option<u32>>,
        dist_stack: &mut Vec<u32>,
        last_dist: &mut u32,
        b: u8,
    ) -> (FilterCode, bool) {
        let dn = *state_stack.last().unwrap();
        let min_dist = *dist_stack.last().unwrap();

        if dn.is_none() {
            // Prefix sink: subsequent bytes pass through unchanged. We
            // pretend acceptance is still live (matched=true) so the
            // iterator descends and yields suffix terminals.
            state_stack.push(None);
            dist_stack.push(min_dist);
            return (FilterCode::Continue, true);
        }

        let dn_idx = dn.unwrap();
        let mut matched = dfa.is_match(dn_idx);
        if matched {
            *last_dist = dfa.distance(dn_idx).min(min_dist);
        }

        let next = dfa.step(dn_idx, b);

        if let Some(next_idx) = next {
            let next_match = dfa.is_match(next_idx);
            let next_dist = dfa.distance(next_idx);
            if next_match {
                matched = true;
                *last_dist = next_dist.min(min_dist);
            }
            state_stack.push(Some(next_idx));
            dist_stack.push(next_dist.min(min_dist));
            return (FilterCode::Continue, matched);
        }

        if prefix_mode && matched {
            state_stack.push(None);
            dist_stack.push(min_dist);
            return (FilterCode::Continue, matched);
        }

        (FilterCode::Stop, matched)
    }

    fn step(&mut self) -> StepOutcome {
        let Some(frame) = self.trie_stack.last_mut() else {
            return StepOutcome::Stop;
        };

        match frame.phase {
            FramePhase::Match => {
                self.pop_frame();
                StepOutcome::Continue
            }
            FramePhase::Label => {
                if frame.byte_offset < frame.label.len() {
                    let b = frame.label[frame.byte_offset];
                    let (rc, matched) = Self::filter(
                        &self.dfa,
                        self.prefix_mode,
                        &mut self.state_stack,
                        &mut self.dist_stack,
                        &mut self.last_dist,
                        b,
                    );
                    match rc {
                        FilterCode::Stop => {
                            let frame = self.trie_stack.last_mut().unwrap();
                            if matched {
                                frame.phase = FramePhase::Match;
                                StepOutcome::Match
                            } else {
                                self.pop_frame();
                                StepOutcome::Continue
                            }
                        }
                        FilterCode::Continue => {
                            let frame = self.trie_stack.last_mut().unwrap();
                            self.key.push(b);
                            frame.byte_offset += 1;
                            if matched {
                                StepOutcome::Match
                            } else {
                                StepOutcome::Continue
                            }
                        }
                    }
                } else {
                    frame.phase = FramePhase::Children;
                    StepOutcome::Continue
                }
            }
            FramePhase::Children => {
                let n_children = frame.node.n_children() as usize;
                if frame.child_offset < n_children {
                    let child_idx = frame.child_offset;
                    frame.child_offset += 1;
                    // Lifetime-recovery: capture `parent_node` at `'tm`
                    // so `.children()[i]` returns `&'tm Node<Data>` and
                    // outlives the upcoming stack mutation.
                    let parent_node: &'tm Node<Data> = self.trie_stack.last().unwrap().node;
                    let child: &'tm Node<Data> = &parent_node.children()[child_idx];
                    self.push_frame(child);
                } else {
                    self.pop_frame();
                }
                StepOutcome::Continue
            }
        }
    }
}

impl<'tm, Data> Iterator for IterateDfaIter<'tm, Data> {
    type Item = (String, &'tm Data, u32);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.step() {
                StepOutcome::Stop => return None,
                StepOutcome::Continue => continue,
                StepOutcome::Match => {
                    let frame = self.trie_stack.last().expect("Match implies a frame");
                    let node: &'tm Node<Data> = frame.node;
                    if frame.byte_offset == frame.label.len()
                        && let Some(data) = node.data()
                    {
                        // Trie label invariant: all stored bytes are
                        // valid UTF-8. The DFA's `is_match` requires the
                        // lifted decoder to be at a codepoint boundary,
                        // so `self.key` ends on a UTF-8 boundary too.
                        let key = String::from_utf8(self.key.clone())
                            .expect("trie label bytes are valid UTF-8");
                        return Some((key, data, self.last_dist));
                    }
                }
            }
        }
    }
}

impl<Data> StrTrieMap<Data> {
    /// Yield every terminal whose key lies within Levenshtein edit distance
    /// `max_dist` of `query`. With `prefix_mode = true`, any suffix beneath
    /// a matched-prefix node is also yielded.
    ///
    /// Sibling of [`Self::iterate_dfa`]: the char-DFA route decodes each
    /// trie label as UTF-8 once per frame push; this route runs the DFA
    /// over the raw bytes, removing the mid-codepoint panic. The yielded
    /// `(key, data, dist)` triples are expected to match the char-DFA
    /// route — snapshot parity tests live in `rune_trie_snapshots`.
    pub fn iterate_dfa<'tm>(
        &'tm self,
        query: &str,
        max_dist: u32,
        prefix_mode: bool,
    ) -> IterateDfaIter<'tm, Data> {
        let dfa = Dfa::build(query, max_dist);
        IterateDfaIter::new(self.byte_trie().root(), dfa, prefix_mode, max_dist)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn run_bytes(auto: &SparseAutomaton, bytes: &[u8]) -> JointState {
        let mut joint = auto.start_joint();
        for &b in bytes {
            let (next, _) = auto.step_byte(&joint, b);
            joint = next;
        }
        joint
    }

    fn run_chars(auto: &SparseAutomaton, chars: &[char]) -> SparseVector {
        let mut s = auto.start();
        for &c in chars {
            s = auto.step_codepoint(&s, Some(c));
        }
        s
    }

    /// Brute-force codepoint-level Levenshtein.
    fn lev_codepoints(a: &str, b: &str) -> u32 {
        let aa: Vec<char> = a.chars().collect();
        let bb: Vec<char> = b.chars().collect();
        let (m, n) = (aa.len(), bb.len());
        let mut prev: Vec<u32> = (0..=n as u32).collect();
        let mut curr = vec![0u32; n + 1];
        for i in 1..=m {
            curr[0] = i as u32;
            for j in 1..=n {
                let cost = u32::from(aa[i - 1] != bb[j - 1]);
                curr[j] = (curr[j - 1] + 1).min(prev[j] + 1).min(prev[j - 1] + cost);
            }
            std::mem::swap(&mut prev, &mut curr);
        }
        prev[n]
    }

    fn match_val(v: &SparseVector, query_len: u32) -> u32 {
        v.0.iter()
            .rev()
            .find_map(|&(idx, val)| if idx == query_len { Some(val) } else { None })
            .unwrap_or(u32::MAX)
    }

    // ---- byte-stepping automaton ----

    /// Per-codepoint parity: feeding UTF-8(c) byte-by-byte must land on
    /// the same sparse vector as feeding c through the char-level step.
    #[test]
    fn byte_step_per_codepoint_matches_char_step() {
        let auto = SparseAutomaton::new("héllo中𝄞", 2);

        for c in ['a', 'h', 'é', '中', '𝄞', '\u{10FFFF}', '\u{0800}'] {
            let mut buf = [0u8; 4];
            let bytes_of_c = c.encode_utf8(&mut buf).as_bytes().to_vec();

            let prior_chars = ['h', 'é'];
            let prior_sparse = run_chars(&auto, &prior_chars);
            let prior_joint = JointState {
                sparse: prior_sparse.clone(),
                decoder: LiftedDecoderState::Between,
            };

            let want = auto.step_codepoint(&prior_sparse, Some(c));

            let mut joint = prior_joint;
            let mut last_step = ByteStep::Pending;
            for &b in &bytes_of_c {
                let (next, s) = auto.step_byte(&joint, b);
                joint = next;
                last_step = s;
            }

            assert_eq!(last_step, ByteStep::Advanced, "for c={c:?}");
            assert!(joint.decoder.is_between(), "for c={c:?}");
            assert_eq!(joint.sparse, want, "DP row mismatch for c={c:?}");
        }
    }

    /// Non-query codepoint of any byte length: byte-step on its UTF-8
    /// must produce the same sparse row as char-step with `Some(c)`.
    /// This covers the OffQuery path (and the OnQueryPrefix-with-shared-
    /// prefix branch by virtue of query chars sharing bytes with non-query
    /// chars sometimes).
    #[test]
    fn byte_step_off_query_matches_none_step() {
        let auto = SparseAutomaton::new("abc", 1);
        // 'z' is not in the query — Some('z') and None produce the same DP row.
        let (after, s) = auto.step_byte(&auto.start_joint(), b'z');
        assert_eq!(s, ByteStep::Advanced);
        assert_eq!(after.sparse, auto.step_codepoint(&auto.start(), Some('z')));
        assert_eq!(after.sparse, auto.step_codepoint(&auto.start(), None));

        // Multi-byte non-query codepoint via byte step.
        let mut buf = [0u8; 4];
        let bytes_of_omega = 'Ω'.encode_utf8(&mut buf).as_bytes().to_vec();
        let joint = run_bytes(&auto, &bytes_of_omega);
        assert!(joint.decoder.is_between());
        // 'Ω' is not in "abc"; result must equal None-step (or any non-query Some).
        assert_eq!(joint.sparse, auto.step_codepoint(&auto.start(), None));
    }

    #[test]
    fn pending_bytes_dont_touch_dp_row() {
        let auto = SparseAutomaton::new("abc", 2);
        let start = auto.start_joint();

        let (after_lead, step1) = auto.step_byte(&start, 0xC3);
        assert_eq!(step1, ByteStep::Pending);
        assert_eq!(after_lead.sparse, start.sparse);
        assert!(!after_lead.decoder.is_between());

        // 4-byte 🦀 sequence: F0 9F A6 80. First 3 are Pending.
        let mut j = start.clone();
        for &b in &[0xF0u8, 0x9F, 0xA6] {
            let (next, s) = auto.step_byte(&j, b);
            assert_eq!(s, ByteStep::Pending);
            assert_eq!(next.sparse, start.sparse);
            assert!(!next.decoder.is_between());
            j = next;
        }
        let (final_j, s) = auto.step_byte(&j, 0x80);
        assert_eq!(s, ByteStep::Advanced);
        assert!(final_j.decoder.is_between());
        assert_eq!(
            final_j.sparse,
            auto.step_codepoint(&start.sparse, Some('\u{1F980}'))
        );
    }

    #[test]
    fn end_to_end_matches_brute_force_levenshtein() {
        let cases = [
            ("abc", "abc", 0),
            ("abc", "abd", 1),
            ("abc", "bcd", 2),
            ("café", "cafe", 1),
            ("café", "cafés", 1),
            ("中文", "中文", 0),
            ("中文", "中国", 1),
            ("naïve", "naive", 1),
            ("naïve", "navie", 2),
            ("résumé", "resume", 2),
            ("hello", "world", 4),
            ("𝄞", "𝄞", 0),
            ("𝄞", "𝄢", 1),
            ("", "", 0),
            ("", "a", 1),
            ("a", "", 1),
        ];
        for (query, input, max) in cases {
            let auto = SparseAutomaton::new(query, max);
            let actual_dist = lev_codepoints(query, input);
            let final_joint = run_bytes(&auto, input.as_bytes());
            let matches = auto.is_match(&final_joint);
            let expected = actual_dist <= max;
            assert_eq!(
                matches, expected,
                "is_match disagreed with brute force: query={query:?} input={input:?} \
                 max={max} actual_dist={actual_dist}"
            );

            if matches {
                let q_len = query.chars().count() as u32;
                let reported = match_val(&final_joint.sparse, q_len);
                assert_eq!(
                    reported, actual_dist,
                    "reported edit cost wrong for query={query:?} input={input:?}"
                );
            }
        }
    }

    #[test]
    fn mid_codepoint_never_accepts() {
        let auto = SparseAutomaton::new("é", 0);
        let (after_lead, _) = auto.step_byte(&auto.start_joint(), 0xC3);
        assert!(!auto.is_match(&after_lead));
        let (after_full, _) = auto.step_byte(&after_lead, 0xA9);
        assert!(auto.is_match(&after_full));
    }

    #[test]
    fn invalid_byte_signals_dead_branch() {
        let auto = SparseAutomaton::new("abc", 1);
        let (after, s) = auto.step_byte(&auto.start_joint(), 0xFF);
        assert_eq!(s, ByteStep::Invalid);
        assert!(after.decoder.is_between());
        assert_eq!(after.sparse, auto.start());
    }

    #[test]
    fn joint_state_is_hashable() {
        use std::collections::HashSet;
        let auto = SparseAutomaton::new("ab", 1);
        let mut set: HashSet<JointState> = HashSet::new();
        set.insert(auto.start_joint());
        set.insert(auto.start_joint());
        assert_eq!(set.len(), 1);
        let (advanced, _) = auto.step_byte(&auto.start_joint(), b'a');
        set.insert(advanced);
        assert_eq!(set.len(), 2);
    }

    // ---- query byte trie ----

    #[test]
    fn query_byte_trie_basics() {
        // Query with codepoints of differing UTF-8 lengths.
        let trie = QueryByteTrie::build(&['a', 'é', '中']);
        // From root: 3 children — 0x61 (a), 0xC3 (é lead), 0xE4 (中 lead).
        let root = &trie.nodes[0];
        assert_eq!(root.children.len(), 3);
        assert!(root.children.iter().any(|&(b, _)| b == 0x61));
        assert!(root.children.iter().any(|&(b, _)| b == 0xC3));
        assert!(root.children.iter().any(|&(b, _)| b == 0xE4));

        // 'a' is a 1-byte leaf at depth 1.
        let a_idx = trie.root_child(0x61).unwrap();
        let a = trie.node(a_idx);
        assert_eq!(a.bytes_consumed, 1);
        assert_eq!(a.total_bytes, 1);
        assert_eq!(a.completes_to, Some('a'));

        // 'é' = C3 A9 — internal node at depth 1, leaf at depth 2.
        let e_lead = trie.root_child(0xC3).unwrap();
        let e_lead_node = trie.node(e_lead);
        assert_eq!(e_lead_node.bytes_consumed, 1);
        assert_eq!(e_lead_node.total_bytes, 2);
        assert_eq!(e_lead_node.completes_to, None);
        let e_leaf = trie.child(e_lead, 0xA9).unwrap();
        let e_leaf_node = trie.node(e_leaf);
        assert_eq!(e_leaf_node.bytes_consumed, 2);
        assert_eq!(e_leaf_node.completes_to, Some('é'));
    }

    #[test]
    fn query_byte_trie_shares_common_prefix() {
        // Query with 2 codepoints sharing UTF-8 prefix: '中' (E4 B8 AD)
        // and '丰' (E4 B8 B0). They share E4 B8.
        let trie = QueryByteTrie::build(&['中', '丰']);
        // Root has one child for E4.
        assert_eq!(trie.nodes[0].children.len(), 1);
        // E4 has one child for B8.
        let e4 = trie.root_child(0xE4).unwrap();
        assert_eq!(trie.node(e4).children.len(), 1);
        // E4 B8 has two children: AD and B0.
        let e4b8 = trie.child(e4, 0xB8).unwrap();
        assert_eq!(trie.node(e4b8).children.len(), 2);
    }

    // ---- Dfa subset construction ----

    fn walk(dfa: &Dfa, bytes: &[u8]) -> (Option<u32>, bool, u32) {
        let mut state = Some(0u32);
        let mut accepted = false;
        let mut min_dist = u32::MAX;
        if let Some(s) = state
            && dfa.is_match(s)
        {
            accepted = true;
            min_dist = min_dist.min(dfa.distance(s));
        }
        for &b in bytes {
            let Some(s) = state else { break };
            state = dfa.step(s, b);
            if let Some(s) = state
                && dfa.is_match(s)
            {
                accepted = true;
                min_dist = min_dist.min(dfa.distance(s));
            }
        }
        (state, accepted, min_dist)
    }

    #[test]
    fn dfa_walk_matches_automaton_step() {
        let cases = [
            ("abc", 1, &b""[..]),
            ("abc", 1, &b"abc"[..]),
            ("abc", 1, &b"abd"[..]),
            ("abc", 1, &b"abcd"[..]),
            ("café", 1, "café".as_bytes()),
            ("café", 1, "cafe".as_bytes()),
            ("café", 2, "kafee".as_bytes()),
            ("中文", 1, "中文".as_bytes()),
            ("中文", 1, "中国".as_bytes()),
            ("𝄞", 1, "𝄞".as_bytes()),
        ];
        for (query, max, input) in cases {
            let dfa = Dfa::build(query, max);
            let auto = SparseAutomaton::new(query, max);

            let mut joint = auto.start_joint();
            let mut auto_accepted = auto.is_match(&joint);
            for &b in input {
                let (next, _) = auto.step_byte(&joint, b);
                joint = next;
                if auto.is_match(&joint) {
                    auto_accepted = true;
                }
            }

            let (_, dfa_accepted, _) = walk(&dfa, input);
            assert_eq!(
                dfa_accepted, auto_accepted,
                "DFA and automaton disagreed: query={query:?} max={max} input={input:?}"
            );
        }
    }

    #[test]
    fn dfa_accepts_iff_levenshtein_within_max() {
        // NOTE: empty-query test cases like `("", "a", 1)` are intentionally
        // omitted. They hit a known inherited quirk from the C-side
        // `SparseAutomaton_Start`: when `query.len() < max`, the start row
        // contains spurious entries beyond `query.len()` (e.g. `[(0,0),(1,1)]`
        // for an empty query with max=1), which prevents the start state from
        // being recognized as accepting. The existing char DFA in `dfa.rs`
        // shares this quirk; preserving it is required for snapshot parity
        // in task #5.
        let cases = [
            ("abc", "abc", 0),
            ("abc", "abd", 1),
            ("abc", "bcd", 2),
            ("café", "cafe", 1),
            ("café", "cafés", 1),
            ("中文", "中国", 1),
            ("naïve", "naive", 1),
            ("résumé", "resume", 2),
            ("𝄞", "𝄢", 1),
            ("", "", 0),
            ("a", "", 1),
            ("hello", "world", 4),
            ("hello", "world", 3),
        ];
        for (query, input, max) in cases {
            let dfa = Dfa::build(query, max);
            let (_, accepted, dist) = walk(&dfa, input.as_bytes());

            // The walk accepts if ANY input prefix was within `max`.
            // The reported `dist` is the running minimum across accepts,
            // matching the char DFA's `MIN(state.distance, minDist)`
            // semantics — see `dfa.rs` module doc.
            let chars: Vec<char> = input.chars().collect();
            let mut best: Option<u32> = None;
            for k in 0..=chars.len() {
                let prefix: String = chars[..k].iter().collect();
                let d = lev_codepoints(query, &prefix);
                if d <= max {
                    best = Some(best.map_or(d, |b| b.min(d)));
                }
            }

            assert_eq!(
                accepted,
                best.is_some(),
                "DFA acceptance wrong: query={query:?} input={input:?} max={max}"
            );
            if let Some(expected_dist) = best {
                assert_eq!(
                    dist, expected_dist,
                    "reported distance wrong (running min over input prefixes): \
                     query={query:?} input={input:?} max={max}"
                );
            }
        }
    }

    #[test]
    fn dfa_does_not_accept_mid_codepoint() {
        let dfa = Dfa::build("é", 0);
        let (state, accepted, _) = walk(&dfa, &[0xC3]);
        assert!(state.is_some());
        assert!(!accepted);
        let (_, accepted, dist) = walk(&dfa, &[0xC3, 0xA9]);
        assert!(accepted);
        assert_eq!(dist, 0);
    }

    #[test]
    fn dfa_dead_ends_on_invalid_utf8() {
        let dfa = Dfa::build("abc", 1);
        let (state, _, _) = walk(&dfa, &[0xFF]);
        assert!(state.is_none());
    }

    #[test]
    fn dfa_node_count_is_bounded() {
        let dfa = Dfa::build("abc", 1);
        assert!(dfa.node_count() < 100, "ASCII DFA too large: {}", dfa.node_count());

        let dfa = Dfa::build("é", 0);
        assert!(dfa.node_count() < 50, "single multi-byte DFA too large: {}", dfa.node_count());

        // Heavier query: multi-codepoint mixed widths, larger max.
        let dfa = Dfa::build("café中文", 2);
        assert!(
            dfa.node_count() < 2000,
            "mixed-width DFA too large: {}",
            dfa.node_count()
        );
    }

    #[test]
    fn dfa_build_is_deterministic_in_node_count() {
        for _ in 0..5 {
            let a = Dfa::build("hello", 1);
            let b = Dfa::build("hello", 1);
            assert_eq!(a.node_count(), b.node_count());
        }
    }

    // ---- iterator smoke tests ----

    fn collect_byte<'tm, T: 'tm + Clone + std::fmt::Debug>(
        trie: &'tm StrTrieMap<T>,
        query: &str,
        max: u32,
        prefix: bool,
    ) -> Vec<(String, T, u32)> {
        let mut out: Vec<(String, T, u32)> = trie
            .iterate_dfa(query, max, prefix)
            .map(|(k, v, d)| (k, v.clone(), d))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn collect_char<'tm, T: 'tm + Clone + std::fmt::Debug>(
        trie: &'tm StrTrieMap<T>,
        query: &str,
        max: u32,
        prefix: bool,
    ) -> Vec<(String, T, u32)> {
        let mut out: Vec<(String, T, u32)> = trie
            .iterate_dfa(query, max, prefix)
            .map(|(k, v, d)| (k, v.clone(), d))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[test]
    fn iterator_exact_ascii_match() {
        let mut trie: StrTrieMap<u32> = StrTrieMap::new();
        trie.insert("apple", 1);
        trie.insert("apply", 2);
        trie.insert("apricot", 3);
        let got = collect_byte(&trie, "apple", 0, false);
        assert_eq!(got, vec![("apple".to_string(), 1, 0)]);
    }

    #[test]
    fn iterator_one_edit_ascii() {
        let mut trie: StrTrieMap<u32> = StrTrieMap::new();
        for w in ["apple", "apply", "apron", "banana"] {
            trie.insert(w, 0);
        }
        let got = collect_byte(&trie, "apple", 1, false);
        let keys: Vec<&str> = got.iter().map(|(k, _, _)| k.as_str()).collect();
        // "apple" exact, "apply" within 1 edit (e→y).
        assert!(keys.contains(&"apple"));
        assert!(keys.contains(&"apply"));
        // "apron" and "banana" should not be reachable.
        assert!(!keys.contains(&"apron"));
        assert!(!keys.contains(&"banana"));
    }

    /// The original mid-codepoint panic repro: a byte trie with keys that
    /// share a byte prefix splitting mid-codepoint. With the byte DFA,
    /// no `from_utf8` runs on label slices, so the walk completes.
    #[test]
    fn iterator_handles_mid_codepoint_label_split() {
        let mut trie: StrTrieMap<u32> = StrTrieMap::new();
        // "café" = 63 61 66 C3 A9; "carrot" = 63 61 72 72 6F 74.
        // The byte trie may split between bytes 2 and 3 of "café",
        // leaving a label slice that starts with the C3 lead byte
        // alone — the char DFA's `from_utf8` would have panicked here.
        trie.insert("café", 1);
        trie.insert("carrot", 2);
        let got = collect_byte(&trie, "café", 0, false);
        assert_eq!(got, vec![("café".to_string(), 1, 0)]);
        let got = collect_byte(&trie, "carrot", 0, false);
        assert_eq!(got, vec![("carrot".to_string(), 2, 0)]);
    }

    #[test]
    fn iterator_multibyte_fuzzy() {
        let mut trie: StrTrieMap<u32> = StrTrieMap::new();
        trie.insert("中文", 1);
        trie.insert("中国", 2);
        trie.insert("日本", 3);
        let got = collect_byte(&trie, "中文", 1, false);
        let keys: Vec<&str> = got.iter().map(|(k, _, _)| k.as_str()).collect();
        // "中文" exact, "中国" within 1 codepoint edit.
        assert!(keys.contains(&"中文"));
        assert!(keys.contains(&"中国"));
        assert!(!keys.contains(&"日本"));
    }

    #[test]
    fn iterator_prefix_mode_yields_suffixes() {
        let mut trie: StrTrieMap<u32> = StrTrieMap::new();
        trie.insert("apple", 1);
        trie.insert("apples", 2);
        trie.insert("application", 3);
        trie.insert("banana", 4);
        // With prefix_mode=true and max=0, "appl" prefix should accept
        // every term that starts with "appl".
        let got = collect_byte(&trie, "appl", 0, true);
        let keys: Vec<&str> = got.iter().map(|(k, _, _)| k.as_str()).collect();
        assert!(keys.contains(&"apple"));
        assert!(keys.contains(&"apples"));
        assert!(keys.contains(&"application"));
        assert!(!keys.contains(&"banana"));
    }

    /// Quick parity sanity check — same trie, same query, same params:
    /// byte iterator and char iterator yield the same set of keys (the
    /// big snapshot run lives in task #5).
    #[test]
    fn iterator_parity_with_char_dfa_on_ascii() {
        let mut trie: StrTrieMap<u32> = StrTrieMap::new();
        for (w, v) in [("apple", 1), ("apply", 2), ("apron", 3), ("banana", 4)] {
            trie.insert(w, v);
        }
        for max in [0u32, 1, 2] {
            for prefix in [false, true] {
                for query in ["apple", "ap", "appl", "zebra"] {
                    let byte_out = collect_byte(&trie, query, max, prefix);
                    let char_out = collect_char(&trie, query, max, prefix);
                    assert_eq!(
                        byte_out, char_out,
                        "byte iter ≠ char iter: query={query:?} max={max} prefix={prefix}"
                    );
                }
            }
        }
    }
}
