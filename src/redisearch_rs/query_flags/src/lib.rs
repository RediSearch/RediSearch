/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bitflags controlling query execution behavior.

use enumflags2::bitflags;

pub type QEFlags = enumflags2::BitFlags<QEFlag>;

#[bitflags]
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cheadergen::config(export, prefix_with_name)]
pub enum QEFlag {
    /// Is an aggregate command.
    IsAggregate = 0x01,
    /// Output: send scores with each result.
    SendScores = 0x02,
    /// Send the key used for sorting, for each result.
    SendSortkeys = 0x04,
    /// Don't send the contents of the fields.
    SendNoFields = 0x08,
    /// Send the payload set with ADD.
    SendPayloads = 0x10,
    /// Is a cursor-type query.
    IsCursor = 0x20,
    /// Send multiple required fields.
    RequiredFields = 0x40,
    /// Do not create the root result processor. Only process those
    /// components which process fully-formed, fully-scored results.
    /// This also means that a scorer is not created. It will also not
    /// initialize the first step or the initial lookup table.
    BuildPipelineNoRoot = 0x80,
    /// Run the query in a multi-threaded environment.
    RunInBackground = 0x100,
    /// The query is a search command.
    IsSearch = 0x200,
    /// Highlight/summarize options are active.
    SendHighlight = 0x400,
    /// Do not emit any rows, only the number of query results.
    NoRows = 0x800,
    /// Do not stringify result values. Send them in their proper types.
    Typed = 0x1000,
    /// Send raw document IDs alongside key names. Used for debugging.
    SendRawIds = 0x2000,
    /// Flag for scorer function to create explanation strings.
    SendScoreExplain = 0x4000,
    /// Profile command.
    Profile = 0x8000,
    /// Profile command (limited mode).
    ProfileLimited = 0x10000,
    /// FT.AGGREGATE load all fields.
    AggLoadAll = 0x20000,
    /// Optimize query.
    Optimize = 0x40000,
    /// Compound values are expanded (RESP3 w/JSON).
    FormatExpand = 0x80000,
    /// Compound values are returned serialized (RESP2 or HASH) or
    /// expanded (RESP3 w/JSON).
    FormatDefault = 0x100000,
    /// Set the score of the doc to an RLookupKey in the result.
    SendScoresAsField = 0x200000,
    /// The query is internal (responding to a command from the
    /// coordinator).
    Internal = 0x400000,
    /// The query is a hybrid request.
    IsHybridTail = 0x800000,
    /// The query is a search subquery of a hybrid request.
    IsHybridSearchSubquery = 0x1000000,
    /// The query is a vector subquery of a hybrid request (aggregate
    /// equivalent).
    IsHybridVectorAggregateSubquery = 0x2000000,
    /// The query has an explicit SORT BY 0 step -- no sorting at all.
    /// Currently only used when [`IsHybridTail`](Self::IsHybridTail) is
    /// set.
    NoSort = 0x4000000,
    /// The query has an explicit SORTBY x -- sort by a field.
    HasSortby = 0x8000000,
    /// The query should use a depleter in the pipeline (for
    /// FT.AGGREGATE).
    HasDepleter = 0x10000000,
    /// The query has an explicit WITHCOUNT (for FT.AGGREGATE).
    HasWithcount = 0x20000000,
    /// The query has an explicit GROUPBY (for FT.AGGREGATE).
    HasGroupby = 0x40000000,
    /// The query is for debugging. Note that this is the last bit of
    /// u32.
    Debug = 0x80000000,
}
