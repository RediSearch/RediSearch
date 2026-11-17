/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::program::{
    Algorithm, Argv, Bound, Bytes, DataType, Field, FieldKind, Metric, OpKind, Query, argv,
};

/// The values generated programs draw from. Changing them changes what every
/// `Populate` expands to.
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
const POPULATE_NUMBERS: u64 = 10_000;

pub fn lower(op: &OpKind) -> Vec<Argv> {
    match op {
        OpKind::Create {
            index,
            on,
            prefix,
            fields,
        } => {
            let mut command = argv(&["FT.CREATE"]);
            command.push(index.clone());
            command.push("ON".into());
            command.push(
                match on {
                    DataType::Hash => "HASH",
                    DataType::Json => "JSON",
                }
                .into(),
            );
            command.extend(argv(&["PREFIX", "1"]));
            command.push(prefix.clone());
            command.push("SCHEMA".into());
            fields
                .iter()
                .for_each(|field| schema_field(*on, field, &mut command));
            vec![command]
        }
        OpKind::Alter { index, on, field } => {
            let mut command = argv(&["FT.ALTER"]);
            command.push(index.clone());
            command.extend(argv(&["SCHEMA", "ADD"]));
            schema_field(*on, field, &mut command);
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
        OpKind::Barrier => Vec::new(),
        OpKind::Command { argv } => vec![argv.clone()],
        OpKind::Populate {
            on,
            prefix,
            fields,
            n,
            seed,
        } => {
            let mut rng = SplitMix(*seed);
            (0..*n)
                .map(|i| {
                    let mut key = prefix.clone();
                    key.0.extend_from_slice(i.to_string().as_bytes());
                    let values: Vec<(&Field, Bytes)> = fields
                        .iter()
                        .map(|field| (field, populated(field, &mut rng)))
                        .collect();
                    write(*on, key, &values)
                })
                .collect()
        }
    }
}

/// SplitMix64: a fixed algorithm, so an expansion never changes with a
/// dependency. Only raw integer draws are used.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        let n = u64::try_from(n).expect("fits");
        usize::try_from(self.next() % n).expect("below a usize")
    }

    fn word(&mut self) -> &'static str {
        WORDS[self.below(WORDS.len())]
    }

    fn words(&mut self, separator: &str, most: usize) -> Bytes {
        let count = 1 + self.below(most);
        (0..count)
            .map(|_| self.word())
            .collect::<Vec<_>>()
            .join(separator)
            .into()
    }
}

/// A FLOAT32 blob of `dim` components between -10 and 10, in tenths.
pub fn vector(dim: u16, mut component: impl FnMut() -> i16) -> Bytes {
    Bytes(
        (0..dim)
            .flat_map(|_| (f32::from(component()) / 10.0).to_le_bytes())
            .collect(),
    )
}

fn populated(field: &Field, rng: &mut SplitMix) -> Bytes {
    match field.kind {
        FieldKind::Vector { dim, .. } => vector(dim, || {
            i16::try_from(rng.next() % 201).expect("small") - 100
        }),
        FieldKind::Text => rng.words(" ", 3),
        FieldKind::Tag => rng.words(",", 2),
        FieldKind::Numeric => match rng.next() % 2 {
            0 => NUMBERS[rng.below(NUMBERS.len())].to_string().into(),
            _ => (rng.next() % POPULATE_NUMBERS).to_string().into(),
        },
        FieldKind::Geo => {
            let lon = i64::try_from(rng.next() % 359).expect("small") - 179;
            let lat = i64::try_from(rng.next() % 169).expect("small") - 84;
            format!(
                "{lon}.{:04},{lat}.{:04}",
                rng.next() % 10_000,
                rng.next() % 10_000
            )
            .into()
        }
    }
}

/// A write of `values` to `key`: `HSET`, or `JSON.SET` of an object whose
/// members are the values as JSON. Text and geo points are strings, tags an
/// array of strings, numbers stay numbers, and a vector blob becomes an array
/// of floats.
pub fn write(on: DataType, key: Bytes, values: &[(&Field, Bytes)]) -> Argv {
    match on {
        DataType::Hash => {
            let mut command = vec!["HSET".into(), key];
            for (field, value) in values {
                command.push(field.name.clone());
                command.push(value.clone());
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
            vec![
                "JSON.SET".into(),
                key,
                "$".into(),
                format!("{{{}}}", members.join(",")).into(),
            ]
        }
    }
}

fn json_value(kind: &FieldKind, value: &Bytes) -> String {
    match kind {
        FieldKind::Numeric => String::from_utf8_lossy(value.as_slice()).into_owned(),
        FieldKind::Vector { .. } => {
            let floats: Vec<String> = value
                .as_slice()
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]).to_string())
                .collect();
            format!("[{}]", floats.join(","))
        }
        FieldKind::Tag => {
            let tags: Vec<String> = String::from_utf8_lossy(value.as_slice())
                .split(',')
                .map(json_string)
                .collect();
            format!("[{}]", tags.join(","))
        }
        FieldKind::Text | FieldKind::Geo => json_string(&String::from_utf8_lossy(value.as_slice())),
    }
}

fn json_string(text: &str) -> String {
    serde_json::to_string(text).expect("strings serialize")
}

fn schema_field(on: DataType, field: &Field, command: &mut Argv) {
    if on == DataType::Json {
        let mut path = Bytes::from("$.");
        path.0.extend_from_slice(field.name.as_slice());
        command.push(path);
        command.push("AS".into());
    }
    command.push(field.name.clone());
    match &field.kind {
        FieldKind::Text => command.push("TEXT".into()),
        FieldKind::Numeric => command.push("NUMERIC".into()),
        FieldKind::Tag => command.push("TAG".into()),
        FieldKind::Geo => command.push("GEO".into()),
        FieldKind::Vector {
            algorithm,
            dim,
            metric,
        } => command.extend(argv(&[
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
        ])),
    }
    if field.sortable {
        command.push("SORTABLE".into());
    }
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
        params.push(format!("v{i}").into());
        params.push(vector);
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
        // Dialect 1 parses `-(a) b` as `-(a b)`; the outer parentheses
        // keep the negation to its own operand in every dialect.
        Query::Not(child) => {
            out.extend_from_slice(b"(-(");
            render(child, out, vectors);
            out.extend_from_slice(b"))");
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
        let words = |line: &str| -> Argv { line.split('|').map(Bytes::from).collect() };
        assert_eq!(
            lower(&op),
            [
                words("HSET|k:0|t|x-y|n|7|g|日本語|p|-84.7985,27.4425"),
                words("HSET|k:1|t|the green|n|0|g|red|p|9.1000,3.3743"),
                words("HSET|k:2|t|running running hello|n|906|g|red,日本語|p|68.5272,35.7618"),
            ]
        );
    }

    #[test]
    fn queries_schemas_and_writes_render_to_redisearch_syntax() {
        let text = |query: Query| {
            let (text, _) = rendered(&query);
            String::from_utf8(text.0).unwrap()
        };
        assert_eq!(text(Query::Term("a-b c".into())), r"a\-b\ c");
        assert_eq!(
            text(Query::And(vec![
                Query::Tag {
                    field: "g".into(),
                    values: vec!["red".into(), "a b".into()],
                },
                Query::Or(vec![Query::Term("b".into()), Query::Term("c".into())]),
                Query::Not(Box::new(Query::Range {
                    field: "n".into(),
                    min: Bound::Exclusive(-3),
                    max: Bound::Unbounded,
                })),
            ])),
            r"(@g:{red | a\ b} (b | c) (-(@n:[(-3 +inf])))"
        );
        let knn = OpKind::Query {
            index: "i".into(),
            query: Query::Knn {
                filter: Box::new(Query::All),
                field: "v".into(),
                k: 3,
                vector: vector(1, || 10),
            },
            options: Default::default(),
        };
        let mut expected = argv(&["FT.SEARCH", "i", "*=>[KNN 3 @v $v0]", "PARAMS", "2", "v0"]);
        expected.extend([vector(1, || 10), "DIALECT".into(), "2".into()]);
        assert_eq!(lower(&knn), [expected]);

        let n = field("n", FieldKind::Numeric);
        let create = OpKind::Create {
            index: "i".into(),
            on: DataType::Json,
            prefix: "j:".into(),
            fields: vec![n.clone()],
        };
        let schema = "FT.CREATE i ON JSON PREFIX 1 j: SCHEMA $.n AS n NUMERIC";
        assert_eq!(
            lower(&create),
            [argv(&schema.split(' ').collect::<Vec<_>>())]
        );
        let (text_field, tag) = (field("t", FieldKind::Text), field("g", FieldKind::Tag));
        let v = field(
            "v",
            FieldKind::Vector {
                algorithm: Algorithm::Flat,
                dim: 2,
                metric: Metric::L2,
            },
        );
        let values = [
            (&text_field, Bytes::from("say \"hi\"")),
            (&tag, Bytes::from("red,blue")),
            (&n, Bytes::from("-7")),
            (&v, vector(2, || 15)),
        ];
        assert_eq!(
            write(DataType::Json, "j:1".into(), &values),
            argv(&[
                "JSON.SET",
                "j:1",
                "$",
                r#"{"t":"say \"hi\"","g":["red","blue"],"n":-7,"v":[1.5,1.5]}"#
            ])
        );
    }
}
