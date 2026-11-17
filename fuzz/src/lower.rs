/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::program::{Argv, Bound, Bytes, Field, FieldKind, OpKind, Query, argv};

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
            prefix,
            fields,
        } => {
            let mut command = argv(&["FT.CREATE"]);
            command.push(index.clone());
            command.extend(argv(&["ON", "HASH", "PREFIX", "1"]));
            command.push(prefix.clone());
            command.push("SCHEMA".into());
            for field in fields {
                command.push(field.name.clone());
                command.push(
                    match field.kind {
                        FieldKind::Text => "TEXT",
                        FieldKind::Numeric => "NUMERIC",
                        FieldKind::Tag => "TAG",
                        FieldKind::Geo => "GEO",
                    }
                    .into(),
                );
                if field.sortable {
                    command.push("SORTABLE".into());
                }
            }
            vec![command]
        }
        OpKind::Query {
            index,
            query,
            options,
        } => {
            let mut text = Vec::new();
            render(query, &mut text);
            let mut command = vec!["FT.SEARCH".into(), index.clone(), Bytes(text)];
            if options.no_content {
                command.push("NOCONTENT".into());
            }
            if let Some((offset, count)) = options.limit {
                command.extend(argv(&["LIMIT", &offset.to_string(), &count.to_string()]));
            }
            if let Some(dialect) = options.dialect {
                command.extend(argv(&["DIALECT", &dialect.to_string()]));
            }
            vec![command]
        }
        OpKind::Barrier => Vec::new(),
        OpKind::Command { argv } => vec![argv.clone()],
        OpKind::Populate {
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
                    let mut command = vec!["HSET".into(), key];
                    for field in fields {
                        command.push(field.name.clone());
                        command.push(populated(field, &mut rng));
                    }
                    command
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

fn populated(field: &Field, rng: &mut SplitMix) -> Bytes {
    match field.kind {
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

fn render(query: &Query, out: &mut Vec<u8>) {
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
        Query::And(children) => group(children, b" ", out),
        Query::Or(children) => group(children, b" | ", out),
        Query::Not(child) => {
            out.extend_from_slice(b"-(");
            render(child, out);
            out.push(b')');
        }
    }
}

fn group(children: &[Query], separator: &[u8], out: &mut Vec<u8>) {
    if children.is_empty() {
        out.push(b'*');
        return;
    }
    out.push(b'(');
    for (i, child) in children.iter().enumerate() {
        if i > 0 {
            out.extend_from_slice(separator);
        }
        render(child, out);
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

    fn query(q: Query) -> String {
        let command = &lower(&OpKind::Query {
            index: "i".into(),
            query: q,
            options: Default::default(),
        })[0];
        String::from_utf8(command[2].0.clone()).unwrap()
    }

    #[test]
    fn a_populate_expansion_never_changes() {
        let field = |name: &str, kind| Field {
            name: name.into(),
            kind,
            sortable: false,
        };
        let op = OpKind::Populate {
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
    fn queries_render_to_escaped_query_syntax() {
        assert_eq!(query(Query::Term("a-b c".into())), r"a\-b\ c");
        assert_eq!(
            query(Query::Tag {
                field: "g".into(),
                values: vec!["red".into(), "a b".into()]
            }),
            r"@g:{red | a\ b}"
        );
        assert_eq!(
            query(Query::Range {
                field: "n".into(),
                min: Bound::Exclusive(-3),
                max: Bound::Unbounded
            }),
            "@n:[(-3 +inf]"
        );
        assert_eq!(
            query(Query::And(vec![
                Query::InField {
                    field: "t".into(),
                    term: "a".into()
                },
                Query::Or(vec![Query::Term("b".into()), Query::Term("c".into())]),
                Query::Not(Box::new(Query::Or(vec![]))),
            ])),
            "(@t:a (b | c) -(*))"
        );
    }
}
