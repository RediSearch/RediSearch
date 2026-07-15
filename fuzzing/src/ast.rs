/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The command grammar as a declarative AST. Every type derives `Arbitrary`, so
//! a byte buffer (from LibAFL) decodes straight into a structurally-valid
//! sequence — the enum/struct shapes *are* the grammar. References to indexes
//! and fields are abstract integers here; `lower.rs` resolves them against the
//! live model so queries hit things that actually exist (deep exploration)
//! while this file stays readable (no imperative generation).

use arbitrary::Arbitrary;

/// A structural corruption applied to an already-rendered command, so the
/// argument parsers (count-prefixes, arity, keyword expectations) get malformed
/// input they normally never see. Applied by `lower.rs` after rendering, at a
/// tunable rate, so most commands stay well-formed and deep state still builds.
#[derive(Arbitrary, Debug, Clone)]
pub enum Malform {
    /// Remove the argument at `idx % len`.
    DropArg(u16),
    /// Duplicate the argument at `idx % len`.
    DupArg(u16),
    /// Keep only the first `max(1, idx % len)` arguments.
    Truncate(u16),
    /// Insert a garbage token before `idx % (len+1)`.
    InsertGarbage(u16, Garbage),
    /// Replace a numeric-looking argument (e.g. a `PREFIX`/`PARAMS`/`DIM` count)
    /// with an extreme integer, so count-prefixes disagree with what follows.
    BadCount(u16, Num),
    /// Swap the arguments at `idx` and `idx+1`.
    SwapAdjacent(u16),
    /// Append the whole argument list again.
    RepeatAll,
    /// Append extra trailing garbage arguments.
    ExtraArgs(Vec<Garbage>),
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Command {
    Create(CreateSpec),
    Alter {
        index: u16,
        field: FieldSpec,
    },
    Hset {
        index: u16,
        key: u8,
        values: Vec<ValueSpec>,
    },
    JsonSet {
        index: u16,
        key: u8,
    },
    Del {
        key: u8,
    },
    FtDel {
        index: u16,
        key: u8,
    },
    Search(SearchSpec),
    Aggregate(AggSpec),
    CursorRead {
        index: u16,
        cursor: u16,
    },
    CursorDel {
        index: u16,
        cursor: u16,
    },
    DictAdd {
        dict: u8,
        terms: Vec<Word>,
    },
    SynUpdate {
        index: u16,
        group: u8,
        terms: Vec<Word>,
    },
    DropIndex {
        index: u16,
        keep_docs: bool,
    },
    AliasAdd {
        alias: u8,
        index: u16,
    },
    AliasUpdate {
        alias: u8,
        index: u16,
    },
    AliasDel {
        alias: u8,
    },
    TagVals {
        index: u16,
        field: u16,
    },
    Explain {
        index: u16,
        query: Query,
    },
    Profile {
        index: u16,
        aggregate: bool,
        query: Query,
    },
    SpellCheck {
        index: u16,
        query: Query,
        distance: u8,
    },
    SugAdd {
        key: u8,
        term: Word,
        score: u8,
    },
    SugGet {
        key: u8,
        prefix: Word,
        fuzzy: bool,
    },
    SugLen {
        key: u8,
    },
    SugDel {
        key: u8,
        term: Word,
    },
    Mget {
        index: u16,
        keys: Vec<u8>,
    },
    DictDel {
        dict: u8,
        terms: Vec<Word>,
    },
    SynDump {
        index: u16,
    },
    ConfigSet {
        param: ConfigParam,
        value: Num,
    },
    ConfigGet {
        param: ConfigParam,
    },
    Debug {
        index: u16,
        sub: DebugSub,
    },
}

/// `FT.DEBUG` subcommands that exercise GC and internal index traversal — high
/// crash surface, and the same paths a structural-integrity oracle would use.
/// Rendered as `FT.DEBUG <KEYWORD> <index> [field|term|id|key]`.
#[derive(Arbitrary, Debug, Clone)]
pub enum DebugSub {
    GcForceInvoke,
    GcForceBgInvoke,
    GcCleanNumeric,
    GcWaitForJobs,
    GcStopSchedule,
    GcContinueSchedule,
    DumpInvidx { term: Word },
    DumpNumidx { field: u16 },
    DumpNumidxTree { field: u16 },
    DumpTagidx { field: u16 },
    DumpTerms,
    DumpHnsw { field: u16 },
    DumpGeomidx { field: u16 },
    DumpPrefixTrie,
    DumpSuffixTrie,
    DumpPhoneticHash { term: Word },
    DumpSchema,
    DumpDeletedIds,
    InvidxSummary { term: Word },
    NumidxSummary { field: u16 },
    InfoTagidx { field: u16 },
    IdToDocid { id: Num },
    DocidToId { key: u8 },
    DocInfo { key: u8 },
    GetMaxDocId,
}

/// Runtime config parameters worth flipping mid-sequence — GC tuning and query
/// limits that change execution behavior.
#[derive(Arbitrary, Debug, Clone)]
pub enum ConfigParam {
    Timeout,
    MinPrefix,
    MaxExpansions,
    MaxPrefixExpansions,
    DefaultDialect,
    ForkGcRunInterval,
    ForkGcCleanThreshold,
    MaxAggregateResults,
    MaxDocTableSize,
    OnTimeout,
}

impl ConfigParam {
    /// Modern (`search-*`) config name, driven via `CONFIG GET/SET` rather than
    /// the deprecated `FT.CONFIG` (which logs a warning per call). RediSearch's
    /// per-param validation/apply callbacks — the surface worth fuzzing — run on
    /// this path too.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigParam::Timeout => "search-timeout",
            ConfigParam::MinPrefix => "search-min-prefix",
            ConfigParam::MaxExpansions => "search-max-prefix-expansions",
            ConfigParam::MaxPrefixExpansions => "search-max-prefix-expansions",
            ConfigParam::DefaultDialect => "search-default-dialect",
            ConfigParam::ForkGcRunInterval => "search-fork-gc-run-interval",
            ConfigParam::ForkGcCleanThreshold => "search-fork-gc-clean-threshold",
            ConfigParam::MaxAggregateResults => "search-max-aggregate-results",
            ConfigParam::MaxDocTableSize => "search-max-doctablesize",
            ConfigParam::OnTimeout => "search-on-timeout",
        }
    }
}

// ---- FT.CREATE / FT.ALTER --------------------------------------------------

#[derive(Arbitrary, Debug, Clone)]
pub struct CreateSpec {
    pub on_json: bool,
    pub prefixes: Vec<Word>,
    pub fields: Vec<FieldSpec>,
    pub options: Vec<CreateOpt>,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum CreateOpt {
    MaxTextFields,
    NoOffsets,
    NoHighlight,
    NoFields,
    NoFreqs,
    SkipInitialScan,
    StopWords(Vec<Word>),
    Score(f32),
}

#[derive(Arbitrary, Debug, Clone)]
pub struct FieldSpec {
    pub name: FieldName,
    pub kind: FieldTypeSpec,
    pub sortable: bool,
    pub noindex: bool,
    pub index_missing: bool,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum FieldTypeSpec {
    Text {
        weight: Option<u8>,
        nostem: bool,
        phonetic: bool,
    },
    Numeric,
    Tag {
        separator: Option<Separator>,
        casesensitive: bool,
    },
    Geo,
    Vector(VectorSpec),
}

#[derive(Arbitrary, Debug, Clone)]
pub struct VectorSpec {
    pub algo: Algo,
    pub data: DataType,
    pub dim: Dim,
    pub metric: Metric,
    pub m: Option<u8>,
    pub ef_construction: Option<u8>,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Algo {
    Flat,
    Hnsw,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum DataType {
    Float32,
    Float64,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Metric {
    L2,
    Ip,
    Cosine,
}

/// Vector dimension, biased to small values plus the invalid `0` edge case.
#[derive(Arbitrary, Debug, Clone, Copy)]
pub enum Dim {
    Zero,
    One,
    Two,
    Four,
    Eight,
    Sixteen,
    Big,
}

impl Dim {
    pub fn value(self) -> usize {
        match self {
            Dim::Zero => 0,
            Dim::One => 1,
            Dim::Two => 2,
            Dim::Four => 4,
            Dim::Eight => 8,
            Dim::Sixteen => 16,
            Dim::Big => 128,
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Separator {
    Comma,
    Semicolon,
    Pipe,
    Space,
}

impl Separator {
    pub fn ch(&self) -> char {
        match self {
            Separator::Comma => ',',
            Separator::Semicolon => ';',
            Separator::Pipe => '|',
            Separator::Space => ' ',
        }
    }
}

// ---- documents -------------------------------------------------------------

/// A value for one field of a document. It carries a candidate for every field
/// kind; lowering picks the one matching the resolved field's actual kind.
#[derive(Arbitrary, Debug, Clone)]
pub struct ValueSpec {
    pub field: u16,
    pub word: Word,
    pub num: Num,
    pub geo: GeoSpec,
    pub vector: Vec<VecScalar>,
    pub multi_tag: bool,
}

/// One vector component, biased toward the interesting float edge cases.
#[derive(Arbitrary, Debug, Clone)]
pub enum VecScalar {
    Zero,
    Nan,
    Inf,
    NegInf,
    Big,
    Val(f32),
}

impl VecScalar {
    pub fn value(&self) -> f64 {
        match self {
            VecScalar::Zero => 0.0,
            VecScalar::Nan => f64::NAN,
            VecScalar::Inf => f64::INFINITY,
            VecScalar::NegInf => f64::NEG_INFINITY,
            VecScalar::Big => 1e38,
            VecScalar::Val(v) => *v as f64,
        }
    }
}

// ---- FT.SEARCH -------------------------------------------------------------

#[derive(Arbitrary, Debug, Clone)]
pub struct SearchSpec {
    pub index: u16,
    pub query: Query,
    pub options: Vec<SearchOpt>,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum SearchOpt {
    Dialect(Dialect),
    Limit(Num, Num),
    NoContent,
    WithScores,
    WithSortKeys,
    InFields(u16),
    Return(u16),
    SortBy(u16, Order),
    GeoFilter(u16, GeoSpec),
    Slop(Num),
    InOrder,
    Timeout(Num),
    Scorer(Scorer),
    Verbatim,
    WithPayloads,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Dialect {
    One,
    Two,
    Three,
    Four,
    Bogus,
}

impl Dialect {
    pub fn as_str(&self) -> &'static str {
        match self {
            Dialect::One => "1",
            Dialect::Two => "2",
            Dialect::Three => "3",
            Dialect::Four => "4",
            Dialect::Bogus => "99",
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Scorer {
    Bm25,
    Bm25Std,
    TfIdf,
    DisMax,
    DocScore,
}

impl Scorer {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scorer::Bm25 => "BM25",
            Scorer::Bm25Std => "BM25STD",
            Scorer::TfIdf => "TFIDF",
            Scorer::DisMax => "DISMAX",
            Scorer::DocScore => "DOCSCORE",
        }
    }
}

// ---- query grammar (flat; recursion avoided for stable decoding) -----------

#[derive(Arbitrary, Debug, Clone)]
pub enum Query {
    Plain { head: Atom, rest: Vec<(Join, Atom)> },
    Knn(KnnSpec),
    VectorRange { radius: f32 },
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Join {
    And,
    Or,
    Not,
    Then,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Atom {
    Everything,
    Term(Word),
    TextField {
        field: u16,
        word: Word,
    },
    Phrase(Word, Word),
    Optional(Word),
    // `coerce` occasionally resolves the field ref against *all* fields instead
    // of the type-matching set, deliberately producing a type-mismatched query
    // (e.g. tag syntax on a TEXT field) to exercise error/assertion paths.
    Numeric {
        field: u16,
        coerce: bool,
        range: NumRange,
    },
    Tag {
        field: u16,
        coerce: bool,
        values: Vec<Word>,
    },
    Geo {
        field: u16,
        coerce: bool,
        spec: GeoSpec,
    },
    Affix(Affix, Word),
    Garbage(Garbage),
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Affix {
    Prefix,
    Suffix,
    Contains,
    Fuzzy,
    Wildcard,
}

#[derive(Arbitrary, Debug, Clone)]
pub struct KnnSpec {
    pub prefilter: Prefilter,
    pub k: Num,
    pub ef_runtime: Option<u8>,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Prefilter {
    Everything,
    Text(Word),
    Numeric(NumRange),
}

#[derive(Arbitrary, Debug, Clone)]
pub enum NumRange {
    Closed(Num, Num),
    Open(Num, Num),
    NegInfTo(Num),
    ToPosInf(Num),
    Bogus,
}

#[derive(Arbitrary, Debug, Clone)]
pub struct GeoSpec {
    pub lon: f32,
    pub lat: f32,
    pub radius: f32,
    pub unit: GeoUnit,
    pub out_of_range: bool,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum GeoUnit {
    Km,
    M,
    Mi,
    Ft,
}

impl GeoUnit {
    pub fn as_str(&self) -> &'static str {
        match self {
            GeoUnit::Km => "km",
            GeoUnit::M => "m",
            GeoUnit::Mi => "mi",
            GeoUnit::Ft => "ft",
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Garbage {
    At,
    OpenBracket,
    OpenParens,
    DanglingKnn,
    Quote,
    Pipe,
    EmptyTag,
    EmptyRange,
    Binary(Vec<u8>),
}

// ---- FT.AGGREGATE ----------------------------------------------------------

#[derive(Arbitrary, Debug, Clone)]
pub struct AggSpec {
    pub index: u16,
    pub query: Query,
    pub steps: Vec<AggStep>,
    pub load: Vec<u16>,
    pub cursor: Option<Cursor>,
    pub dialect: Dialect,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum AggStep {
    Apply {
        expr: Expr,
        alias: u8,
    },
    Filter(Expr),
    GroupBy {
        fields: Vec<u16>,
        reducers: Vec<Reducer>,
    },
    SortBy(Vec<(u16, Order)>),
    Limit(Num, Num),
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Reducer {
    Count,
    Sum(u16),
    Avg(u16),
    Min(u16),
    Max(u16),
    ToList(u16),
    CountDistinct(u16),
    Quantile(u16, Quant),
    StdDev(u16),
    FirstValue(u16),
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Quant {
    P50,
    P90,
    P99,
    Bogus,
}

impl Quant {
    pub fn as_str(&self) -> &'static str {
        match self {
            Quant::P50 => "0.5",
            Quant::P90 => "0.9",
            Quant::P99 => "0.99",
            Quant::Bogus => "2.0",
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub enum Expr {
    FieldPlus(u16, Num),
    FieldTimesTwo(u16),
    Sqrt(u16),
    UpperFormat(u16),
    FloorMod,
    Div(u16, Num),
    Split,
    Greater(u16, Num),
    EqSelf(u16),
    Exists(u16),
    Garbage,
}

#[derive(Arbitrary, Debug, Clone)]
pub struct Cursor {
    pub count: Option<Num>,
    pub max_idle: Option<Num>,
}

// ---- leaf value types ------------------------------------------------------

/// An integer argument, biased toward parser-stressing edge values.
#[derive(Arbitrary, Debug, Clone)]
pub enum Num {
    Zero,
    One,
    NegOne,
    I32Max,
    I32MaxPlus,
    I32Min,
    I64Max,
    I64Min,
    Overflow,
    U64Max,
    Small(u16),
}

impl Num {
    pub fn as_str(&self) -> String {
        match self {
            Num::Zero => "0".into(),
            Num::One => "1".into(),
            Num::NegOne => "-1".into(),
            Num::I32Max => "2147483647".into(),
            Num::I32MaxPlus => "2147483648".into(),
            Num::I32Min => "-2147483648".into(),
            Num::I64Max => "9223372036854775807".into(),
            Num::I64Min => "-9223372036854775808".into(),
            Num::Overflow => "9999999999999999999999".into(),
            Num::U64Max => "18446744073709551615".into(),
            Num::Small(n) => n.to_string(),
        }
    }
}

/// A word drawn from a small dictionary (readable, and a keyword dictionary for
/// the query parser).
#[derive(Arbitrary, Debug, Clone)]
pub enum Word {
    Foo,
    Bar,
    Baz,
    Hello,
    World,
    Alpha,
    Beta,
    Redis,
    Search,
    Quick,
    Red,
    Green,
    Blue,
}

impl Word {
    pub fn as_str(&self) -> &'static str {
        match self {
            Word::Foo => "foo",
            Word::Bar => "bar",
            Word::Baz => "baz",
            Word::Hello => "hello",
            Word::World => "world",
            Word::Alpha => "alpha",
            Word::Beta => "beta",
            Word::Redis => "redis",
            Word::Search => "search",
            Word::Quick => "quick",
            Word::Red => "red",
            Word::Green => "green",
            Word::Blue => "blue",
        }
    }
}

/// Field names: original pool plus names mined from tests/pytests.
#[derive(Arbitrary, Debug, Clone)]
pub enum FieldName {
    Title,
    Body,
    Price,
    Qty,
    Tags,
    Category,
    Name,
    Score,
    Loc,
    Vec,
    Text,
    Num,
    Val,
    Description,
}

impl FieldName {
    pub fn as_str(&self) -> &'static str {
        match self {
            FieldName::Title => "title",
            FieldName::Body => "body",
            FieldName::Price => "price",
            FieldName::Qty => "qty",
            FieldName::Tags => "tags",
            FieldName::Category => "category",
            FieldName::Name => "name",
            FieldName::Score => "score",
            FieldName::Loc => "loc",
            FieldName::Vec => "vec",
            FieldName::Text => "text",
            FieldName::Num => "num",
            FieldName::Val => "val",
            FieldName::Description => "description",
        }
    }
}
