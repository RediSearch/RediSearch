/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The commands an op sends.

use rand::RngCore;

use crate::program::{
    Algorithm, Argv, Bound, Bytes, DataType, Field, FieldKind, Metric, OpKind, Query, argv,
};
use crate::vocabulary::Vocabulary;

pub fn lower(op: &OpKind) -> Vec<Argv> {
    match op {
        OpKind::Create {
            index,
            on,
            prefix,
            fields,
        } => {
            let data_type = match on {
                DataType::Hash => "HASH",
                DataType::Json => "JSON",
            };
            let mut command = vec!["FT.CREATE".into(), index.clone()];
            command.extend(argv(&["ON", data_type, "PREFIX", "1"]));
            command.extend([prefix.clone(), "SCHEMA".into()]);
            for field in fields {
                command.extend(schema_field(*on, field));
            }
            vec![command]
        }
        OpKind::Alter { index, on, field } => {
            let mut command = vec!["FT.ALTER".into(), index.clone()];
            command.extend(argv(&["SCHEMA", "ADD"]));
            command.extend(schema_field(*on, field));
            vec![command]
        }
        OpKind::Query {
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
        OpKind::Populate {
            on,
            prefix,
            fields,
            n,
            seed,
        } => {
            let vocabulary = Vocabulary::fixed();
            let mut rng = SplitMix(*seed);
            (0..*n)
                .map(|i| {
                    let values: Vec<(&Field, Bytes)> = fields
                        .iter()
                        .map(|field| (field, vocabulary.value(&field.kind, &mut rng)))
                        .collect();
                    write(*on, key(prefix, i), &values)
                })
                .collect()
        }
        OpKind::Barrier => Vec::new(),
        OpKind::Command { argv } => vec![argv.clone()],
    }
}

pub fn key(prefix: &Bytes, i: u32) -> Bytes {
    let mut key = prefix.clone();
    key.0.extend_from_slice(i.to_string().as_bytes());
    key
}

/// SplitMix64, so a `Populate` expansion never changes with a dependency.
struct SplitMix(u64);

impl RngCore for SplitMix {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn next_u32(&mut self) -> u32 {
        u32::try_from(self.next_u64() >> 32).expect("32 bits")
    }

    fn fill_bytes(&mut self, dst: &mut [u8]) {
        for chunk in dst.chunks_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes()[..chunk.len()]);
        }
    }
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
        let op = OpKind::Populate {
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
            lower(&op),
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
                OpKind::Query {
                    index: "i".into(),
                    query,
                    options: Default::default(),
                },
                search,
            ),
            (
                OpKind::Create {
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
            assert_eq!(lower(&op), [expected]);
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
}
