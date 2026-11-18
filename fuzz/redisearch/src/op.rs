/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RediSearch's ops, and the commands they send.

use procfuzz::draw::Draw;
use procfuzz::service::Input;
use procfuzz_redis::resp::{Argv, Bytes, argv};
use serde::{Deserialize, Serialize};

use crate::effect::{Effect, describe};
use crate::state::State;

pub const WORDS: [&str; 14] = [
    "hello",
    "world",
    "hell",
    "running",
    "runs",
    "red",
    "green",
    "blue",
    "the",
    "a",
    "x-y",
    "hello world",
    "é",
    "日本語",
];
pub const NUMBERS: [i64; 10] = [0, 1, -1, 7, 100, -100, 1 << 31, 1 << 53, i64::MAX, i64::MIN];
const SMALL_NUMBERS: u64 = 10_000;
/// Vector components are tenths between -10 and 10.
const VECTOR_TENTHS: u64 = 201;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Op {
    /// The connection it is sent on.
    #[serde(default, skip_serializing_if = "is_default")]
    pub conn: u8,
    /// Leaves the reply to be read when the connection is next used. An op
    /// whose reply names a cursor is always awaited, so that later ops can
    /// use the cursor.
    #[serde(default, skip_serializing_if = "is_default")]
    pub defer: bool,
    #[serde(flatten)]
    pub action: Action,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Action {
    Create {
        index: Bytes,
        #[serde(default, skip_serializing_if = "is_default")]
        on: DataType,
        prefix: Bytes,
        fields: Vec<Field>,
    },
    Alter {
        index: Bytes,
        #[serde(default, skip_serializing_if = "is_default")]
        on: DataType,
        field: Field,
    },
    Query {
        index: Bytes,
        query: Query,
        #[serde(default)]
        options: QueryOptions,
    },
    /// `FT.AGGREGATE` with `pipeline` after the query, keeping a cursor in
    /// `cursor` if it has one.
    Aggregate {
        index: Bytes,
        query: Query,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pipeline: Argv,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<CursorSlot>,
    },
    /// Reads the next page of the cursor kept in `slot`.
    CursorRead {
        slot: u8,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<u32>,
    },
    CursorDel {
        slot: u8,
    },
    /// Writes keys `<prefix>0` to `<prefix><n-1>`, with values drawn from
    /// `seed`.
    Populate {
        #[serde(default, skip_serializing_if = "is_default")]
        on: DataType,
        prefix: Bytes,
        fields: Vec<Field>,
        n: u32,
        seed: u64,
    },
    /// Waits until the server has no background work left.
    Settle,
    Command {
        argv: Argv,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CursorSlot {
    pub slot: u8,
    pub count: u32,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    #[default]
    Hash,
    Json,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    pub name: Bytes,
    #[serde(flatten)]
    pub kind: FieldKind,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sortable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldKind {
    Text,
    Numeric,
    Tag,
    Geo,
    Vector {
        algorithm: Algorithm,
        dim: u16,
        metric: Metric,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    Flat,
    Hnsw,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    L2,
    Ip,
    Cosine,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Query {
    All,
    Term(Bytes),
    InField {
        field: Bytes,
        term: Bytes,
    },
    Tag {
        field: Bytes,
        values: Vec<Bytes>,
    },
    Range {
        field: Bytes,
        min: Bound,
        max: Bound,
    },
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
    /// Valid only as the whole query.
    Knn {
        filter: Box<Query>,
        field: Bytes,
        k: u32,
        /// FLOAT32 components, little-endian.
        vector: Bytes,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bound {
    Unbounded,
    Inclusive(i64),
    Exclusive(i64),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryOptions {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_content: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<(u32, u32)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<u8>,
}

impl Op {
    pub fn new(action: Action) -> Op {
        Op {
            conn: 0,
            defer: false,
            action,
        }
    }

    /// The input it sends, with cursor ids from `state`.
    pub fn lower(&self, state: &State) -> Input<Argv> {
        match &self.action {
            Action::Settle => Input::Settle,
            action => Input::Send {
                conn: self.conn,
                wait: !self.defer || action.names_a_cursor(),
                requests: action.commands(state),
            },
        }
    }
}

impl Action {
    pub fn command(words: &[&str]) -> Action {
        Action::Command { argv: argv(words) }
    }

    /// What it does to search results.
    pub fn effect(&self) -> Effect {
        match self {
            Action::Settle
            | Action::Query { .. }
            | Action::Aggregate { .. }
            | Action::CursorRead { .. }
            | Action::CursorDel { .. } => Effect::Reads,
            Action::Create { .. } | Action::Alter { .. } | Action::Populate { .. } => {
                Effect::Writes
            }
            Action::Command { argv } => describe(argv).0,
        }
    }

    fn names_a_cursor(&self) -> bool {
        matches!(
            self,
            Action::Aggregate {
                cursor: Some(_),
                ..
            } | Action::CursorRead { .. }
        )
    }

    /// Whether it keeps, reads, or deletes a cursor.
    pub fn uses_a_cursor(&self) -> bool {
        self.names_a_cursor() || matches!(self, Action::CursorDel { .. })
    }

    /// The commands it sends; none for a settle.
    pub fn commands(&self, state: &State) -> Vec<Argv> {
        match self {
            Action::Settle => Vec::new(),
            Action::Command { argv } => vec![argv.clone()],
            Action::Create {
                index,
                on,
                prefix,
                fields,
            } => {
                let mut command = vec!["FT.CREATE".into(), index.clone()];
                command.extend(argv(&["ON", on.name(), "PREFIX", "1"]));
                command.extend([prefix.clone(), "SCHEMA".into()]);
                for field in fields {
                    command.extend(schema_field(*on, field));
                }
                vec![command]
            }
            Action::Alter { index, on, field } => {
                let mut command = vec!["FT.ALTER".into(), index.clone()];
                command.extend(argv(&["SCHEMA", "ADD"]));
                command.extend(schema_field(*on, field));
                vec![command]
            }
            Action::Query {
                index,
                query,
                options,
            } => {
                let (text, params) = rendered(query);
                let mut command = vec!["FT.SEARCH".into(), index.clone(), text];
                if options.no_content {
                    command.push("NOCONTENT".into());
                }
                if let Some((offset, count)) = options.limit {
                    command.extend(argv(&["LIMIT", &offset.to_string(), &count.to_string()]));
                }
                let dialect = options.dialect.or((!params.is_empty()).then_some(2));
                command.extend(params);
                if let Some(dialect) = dialect {
                    command.extend(argv(&["DIALECT", &dialect.to_string()]));
                }
                vec![command]
            }
            Action::Aggregate {
                index,
                query,
                pipeline,
                cursor,
            } => {
                let (text, params) = rendered(query);
                let mut command = vec!["FT.AGGREGATE".into(), index.clone(), text];
                command.extend(pipeline.iter().cloned());
                if let Some(cursor) = cursor {
                    command.extend(argv(&["WITHCURSOR", "COUNT", &cursor.count.to_string()]));
                }
                if !params.is_empty() {
                    command.extend(params);
                    command.extend(argv(&["DIALECT", "2"]));
                }
                vec![command]
            }
            Action::CursorRead { slot, count } => {
                let mut command = cursor_command("READ", state, *slot);
                if let Some(count) = count {
                    command.extend(argv(&["COUNT", &count.to_string()]));
                }
                vec![command]
            }
            Action::CursorDel { slot } => vec![cursor_command("DEL", state, *slot)],
            Action::Populate {
                on,
                prefix,
                fields,
                n,
                seed,
            } => {
                let mut draw = Draw::new(*seed);
                (0..*n)
                    .map(|i| {
                        let values: Vec<(&Field, Bytes)> = fields
                            .iter()
                            .map(|field| (field, value(&field.kind, &mut draw)))
                            .collect();
                        write(*on, key(prefix, i), &values)
                    })
                    .collect()
            }
        }
    }
}

/// `FT.CURSOR <action> <index> <id>` for the cursor kept in `slot`, or for
/// cursor 0 if there is none.
fn cursor_command(action: &str, state: &State, slot: u8) -> Argv {
    let cursor = state.cursors.get(&slot);
    let index = cursor.map_or_else(|| Bytes::from("i0"), |cursor| cursor.index.clone());
    let id = cursor.and_then(|cursor| cursor.id).unwrap_or(0);
    vec![
        "FT.CURSOR".into(),
        action.into(),
        index,
        id.to_string().into(),
    ]
}

impl DataType {
    fn name(self) -> &'static str {
        match self {
            DataType::Hash => "HASH",
            DataType::Json => "JSON",
        }
    }
}

pub fn key(prefix: &Bytes, i: u32) -> Bytes {
    let mut key = prefix.clone();
    key.0.extend_from_slice(i.to_string().as_bytes());
    key
}

/// `HSET`, or `JSON.SET` of an object in which tags are arrays of strings
/// and vectors arrays of numbers.
pub fn write(on: DataType, key: Bytes, values: &[(&Field, Bytes)]) -> Argv {
    match on {
        DataType::Hash => {
            let mut command = vec!["HSET".into(), key];
            for (field, value) in values {
                command.extend([field.name.clone(), value.clone()]);
            }
            command
        }
        DataType::Json => {
            let members: Vec<String> = values
                .iter()
                .map(|(field, value)| {
                    let name = String::from_utf8_lossy(field.name.as_slice());
                    format!("{}:{}", json_string(&name), json_value(&field.kind, value))
                })
                .collect();
            let object = format!("{{{}}}", members.join(","));
            vec!["JSON.SET".into(), key, "$".into(), object.into()]
        }
    }
}

fn json_value(kind: &FieldKind, value: &Bytes) -> String {
    let text = String::from_utf8_lossy(value.as_slice());
    match kind {
        FieldKind::Numeric => text.into_owned(),
        FieldKind::Vector { .. } => {
            let floats: Vec<String> = value
                .as_slice()
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]).to_string())
                .collect();
            format!("[{}]", floats.join(","))
        }
        FieldKind::Tag => {
            let tags: Vec<String> = text.split(',').map(json_string).collect();
            format!("[{}]", tags.join(","))
        }
        FieldKind::Text | FieldKind::Geo => json_string(&text),
    }
}

fn json_string(text: &str) -> String {
    serde_json::to_string(text).expect("strings serialize")
}

fn schema_field(on: DataType, field: &Field) -> Argv {
    let mut out = Vec::new();
    if on == DataType::Json {
        let mut path = Bytes::from("$.");
        path.0.extend_from_slice(field.name.as_slice());
        out.extend([path, "AS".into()]);
    }
    out.push(field.name.clone());
    out.extend(match &field.kind {
        FieldKind::Text => argv(&["TEXT"]),
        FieldKind::Numeric => argv(&["NUMERIC"]),
        FieldKind::Tag => argv(&["TAG"]),
        FieldKind::Geo => argv(&["GEO"]),
        FieldKind::Vector {
            algorithm,
            dim,
            metric,
        } => argv(&[
            "VECTOR",
            match algorithm {
                Algorithm::Flat => "FLAT",
                Algorithm::Hnsw => "HNSW",
            },
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            &dim.to_string(),
            "DISTANCE_METRIC",
            match metric {
                Metric::L2 => "L2",
                Metric::Ip => "IP",
                Metric::Cosine => "COSINE",
            },
        ]),
    });
    if field.sortable {
        out.push("SORTABLE".into());
    }
    out
}

/// The query text, and the `PARAMS` arguments its vectors need, if any.
pub fn rendered(query: &Query) -> (Bytes, Argv) {
    let mut text = Vec::new();
    let mut vectors = Vec::new();
    render(query, &mut text, &mut vectors);
    if vectors.is_empty() {
        return (Bytes(text), Vec::new());
    }
    let mut params = argv(&["PARAMS", &(2 * vectors.len()).to_string()]);
    for (i, vector) in vectors.into_iter().enumerate() {
        params.extend([format!("v{i}").into(), vector]);
    }
    (Bytes(text), params)
}

fn render(query: &Query, out: &mut Vec<u8>, vectors: &mut Vec<Bytes>) {
    match query {
        Query::All => out.push(b'*'),
        Query::Term(term) => escape(term, out),
        Query::InField { field, term } => {
            field_prefix(field, out);
            escape(term, out);
        }
        Query::Tag { field, values } => {
            field_prefix(field, out);
            out.push(b'{');
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b" | ");
                }
                escape(value, out);
            }
            out.push(b'}');
        }
        Query::Range { field, min, max } => {
            field_prefix(field, out);
            out.push(b'[');
            bound(*min, b"-inf", out);
            out.push(b' ');
            bound(*max, b"+inf", out);
            out.push(b']');
        }
        Query::And(children) => group(children, b" ", out, vectors),
        Query::Or(children) => group(children, b" | ", out, vectors),
        // Dialect 1 parses `-(a) b` as `-(a b)`.
        Query::Not(child) => {
            out.extend_from_slice(b"(-(");
            render(child, out, vectors);
            out.extend_from_slice(b"))");
        }
        Query::Knn {
            filter,
            field,
            k,
            vector,
        } => {
            if **filter == Query::All {
                out.push(b'*');
            } else {
                out.push(b'(');
                render(filter, out, vectors);
                out.push(b')');
            }
            out.extend_from_slice(format!("=>[KNN {k} @").as_bytes());
            escape(field, out);
            out.extend_from_slice(format!(" $v{}]", vectors.len()).as_bytes());
            vectors.push(vector.clone());
        }
    }
}

fn group(children: &[Query], separator: &[u8], out: &mut Vec<u8>, vectors: &mut Vec<Bytes>) {
    if children.is_empty() {
        out.push(b'*');
        return;
    }
    out.push(b'(');
    for (i, child) in children.iter().enumerate() {
        if i > 0 {
            out.extend_from_slice(separator);
        }
        render(child, out, vectors);
    }
    out.push(b')');
}

fn field_prefix(field: &Bytes, out: &mut Vec<u8>) {
    out.push(b'@');
    escape(field, out);
    out.push(b':');
}

fn bound(bound: Bound, unbounded: &[u8], out: &mut Vec<u8>) {
    match bound {
        Bound::Unbounded => out.extend_from_slice(unbounded),
        Bound::Inclusive(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Bound::Exclusive(n) => out.extend_from_slice(format!("({n}").as_bytes()),
    }
}

fn escape(token: &Bytes, out: &mut Vec<u8>) {
    for &b in token.as_slice() {
        if !(b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80) {
            out.push(b'\\');
        }
        out.push(b);
    }
}

pub fn word(draw: &mut Draw) -> Bytes {
    (*draw.pick(&WORDS).expect("never empty")).into()
}

pub fn number(draw: &mut Draw) -> i64 {
    *draw.pick(&NUMBERS).expect("never empty")
}

/// A value for a field of `kind`, as `HSET` takes it. Tags are separated by
/// commas.
pub fn value(kind: &FieldKind, draw: &mut Draw) -> Bytes {
    let words = |separator: &str, most: u64, draw: &mut Draw| -> Bytes {
        let count = 1 + draw.below(most);
        let words: Vec<String> = (0..count)
            .map(|_| String::from_utf8_lossy(word(draw).as_slice()).into_owned())
            .collect();
        words.join(separator).into()
    };
    match kind {
        FieldKind::Text => words(" ", 3, draw),
        FieldKind::Tag => words(",", 2, draw),
        FieldKind::Numeric if draw.below(2) == 0 => number(draw).to_string().into(),
        FieldKind::Numeric => draw.below(SMALL_NUMBERS).to_string().into(),
        FieldKind::Geo => {
            let mut coordinate = |degrees: u64| {
                let whole = i128::from(draw.below(2 * degrees + 1)) - i128::from(degrees);
                format!("{whole}.{:04}", draw.below(10_000))
            };
            let longitude = coordinate(179);
            format!("{longitude},{}", coordinate(84)).into()
        }
        FieldKind::Vector { dim, .. } => vector(*dim, draw),
    }
}

/// `dim` FLOAT32 components between -10 and 10, in steps of a tenth.
pub fn vector(dim: u16, draw: &mut Draw) -> Bytes {
    Bytes(
        (0..dim)
            .flat_map(|_| {
                let tenths = u8::try_from(draw.below(VECTOR_TENTHS)).expect("at most 200");
                ((f32::from(tenths) - 100.0) / 10.0).to_le_bytes()
            })
            .collect(),
    )
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, kind: FieldKind) -> Field {
        Field {
            name: name.into(),
            kind,
            sortable: false,
        }
    }

    fn words(line: &str) -> Argv {
        line.split(';').map(Bytes::from).collect()
    }

    #[test]
    fn a_populate_expansion_never_changes() {
        let op = Action::Populate {
            on: DataType::Hash,
            prefix: "k:".into(),
            fields: vec![
                field("t", FieldKind::Text),
                field("n", FieldKind::Numeric),
                field("g", FieldKind::Tag),
                field("p", FieldKind::Geo),
            ],
            n: 3,
            seed: 7,
        };
        assert_eq!(
            op.commands(&State::default()),
            [
                words("HSET;k:0;t;x-y;n;7;g;日本語;p;-84.9182,66.4425"),
                words("HSET;k:1;t;the green;n;0;g;red;p;9.9797,-54.3743"),
                words("HSET;k:2;t;running running hello;n;906;g;red,日本語;p;68.1820,13.7618"),
            ]
        );
    }

    #[test]
    fn ops_lower_to_redisearch_syntax() {
        let n = field("n", FieldKind::Numeric);
        let v = field(
            "v",
            FieldKind::Vector {
                algorithm: Algorithm::Flat,
                dim: 1,
                metric: Metric::L2,
            },
        );
        let query = Query::Knn {
            filter: Box::new(Query::And(vec![
                Query::Tag {
                    field: "g".into(),
                    values: vec!["red".into(), "a b".into()],
                },
                Query::Or(vec![Query::Term("a-b".into()), Query::Term("c".into())]),
                Query::Not(Box::new(Query::Range {
                    field: "n".into(),
                    min: Bound::Exclusive(-3),
                    max: Bound::Unbounded,
                })),
            ])),
            field: "v".into(),
            k: 3,
            vector: Bytes(1.5f32.to_le_bytes().to_vec()),
        };
        let mut search = words(
            r"FT.SEARCH;i;((@g:{red | a\ b} (a\-b | c) (-(@n:[(-3 +inf]))))=>[KNN 3 @v $v0];PARAMS;2;v0",
        );
        search.extend([
            Bytes(1.5f32.to_le_bytes().to_vec()),
            "DIALECT".into(),
            "2".into(),
        ]);
        let ops = [
            (
                Action::Query {
                    index: "i".into(),
                    query,
                    options: QueryOptions::default(),
                },
                search,
            ),
            (
                Action::Create {
                    index: "i".into(),
                    on: DataType::Json,
                    prefix: "j:".into(),
                    fields: vec![n.clone(), v.clone()],
                },
                words(
                    "FT.CREATE;i;ON;JSON;PREFIX;1;j:;SCHEMA;$.n;AS;n;NUMERIC;$.v;AS;v;VECTOR;FLAT;6;TYPE;FLOAT32;DIM;1;DISTANCE_METRIC;L2",
                ),
            ),
        ];
        for (op, expected) in ops {
            assert_eq!(op.commands(&State::default()), [expected]);
        }
        let tag = field("g", FieldKind::Tag);
        let values = [
            (&tag, Bytes::from("red,\"x\"")),
            (&n, Bytes::from("-7")),
            (&v, Bytes(1.5f32.to_le_bytes().to_vec())),
        ];
        assert_eq!(
            write(DataType::Json, "j:1".into(), &values),
            words(r#"JSON.SET;j:1;$;{"g":["red","\"x\""],"n":-7,"v":[1.5]}"#)
        );
    }

    #[test]
    fn an_op_that_names_a_cursor_is_awaited_even_when_deferred() {
        let aggregate = Op {
            conn: 1,
            defer: true,
            action: Action::Aggregate {
                index: "i".into(),
                query: Query::All,
                pipeline: Vec::new(),
                cursor: Some(CursorSlot { slot: 0, count: 1 }),
            },
        };
        assert!(matches!(
            aggregate.lower(&State::default()),
            Input::Send { wait: true, .. }
        ));
    }
}
