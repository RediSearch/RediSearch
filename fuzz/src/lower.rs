/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::program::{Argv, Bound, Bytes, Field, FieldKind, OpKind, Query, QueryOptions};

pub fn lower(op: &OpKind) -> Vec<Argv> {
    match op {
        OpKind::Create {
            index,
            prefix,
            fields,
        } => vec![create(index, prefix, fields)],
        OpKind::Write { key, fields } => {
            let mut argv = vec!["HSET".into(), key.clone()];
            for (name, value) in fields {
                argv.push(name.clone());
                argv.push(value.clone());
            }
            vec![argv]
        }
        OpKind::Delete { key } => vec![vec!["DEL".into(), key.clone()]],
        OpKind::Query {
            index,
            query,
            options,
        } => vec![search(index, query, options)],
        OpKind::Raw { argv } => vec![argv.clone()],
    }
}

fn create(index: &Bytes, prefix: &Bytes, fields: &[Field]) -> Argv {
    let mut argv: Argv = vec![
        "FT.CREATE".into(),
        index.clone(),
        "ON".into(),
        "HASH".into(),
        "PREFIX".into(),
        "1".into(),
        prefix.clone(),
        "SCHEMA".into(),
    ];
    for field in fields {
        argv.push(field.name.clone());
        argv.push(
            match field.kind {
                FieldKind::Text => "TEXT",
                FieldKind::Numeric => "NUMERIC",
                FieldKind::Tag => "TAG",
                FieldKind::Geo => "GEO",
            }
            .into(),
        );
        if field.sortable {
            argv.push("SORTABLE".into());
        }
    }
    argv
}

fn search(index: &Bytes, query: &Query, options: &QueryOptions) -> Argv {
    let mut text = Vec::new();
    render(query, &mut text);
    let mut argv: Argv = vec!["FT.SEARCH".into(), index.clone(), Bytes(text)];
    if options.no_content {
        argv.push("NOCONTENT".into());
    }
    if let Some((offset, count)) = options.limit {
        argv.push("LIMIT".into());
        argv.push(offset.to_string().into());
        argv.push(count.to_string().into());
    }
    if let Some(dialect) = options.dialect {
        argv.push("DIALECT".into());
        argv.push(dialect.to_string().into());
    }
    argv
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
        Bound::Exclusive(n) => {
            out.push(b'(');
            out.extend_from_slice(n.to_string().as_bytes());
        }
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

    fn strings(argv: &Argv) -> Vec<String> {
        argv.iter()
            .map(|a| String::from_utf8(a.0.clone()).unwrap())
            .collect()
    }

    fn query(q: Query) -> String {
        let argv = &lower(&OpKind::Query {
            index: "i".into(),
            query: q,
            options: QueryOptions::default(),
        })[0];
        String::from_utf8(argv[2].0.clone()).unwrap()
    }

    #[test]
    fn create_lowers_schema() {
        let op = OpKind::Create {
            index: "i1".into(),
            prefix: "doc:".into(),
            fields: vec![
                Field {
                    name: "t".into(),
                    kind: FieldKind::Text,
                    sortable: false,
                },
                Field {
                    name: "n".into(),
                    kind: FieldKind::Numeric,
                    sortable: true,
                },
            ],
        };
        assert_eq!(
            strings(&lower(&op)[0]),
            [
                "FT.CREATE",
                "i1",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "t",
                "TEXT",
                "n",
                "NUMERIC",
                "SORTABLE"
            ]
        );
    }

    #[test]
    fn write_and_delete_lower_to_hash_commands() {
        let write = OpKind::Write {
            key: "doc:1".into(),
            fields: vec![("t".into(), "hello world".into())],
        };
        assert_eq!(
            strings(&lower(&write)[0]),
            ["HSET", "doc:1", "t", "hello world"]
        );
        let delete = OpKind::Delete {
            key: "doc:1".into(),
        };
        assert_eq!(strings(&lower(&delete)[0]), ["DEL", "doc:1"]);
    }

    #[test]
    fn query_options_follow_the_query() {
        let op = OpKind::Query {
            index: "i".into(),
            query: Query::All,
            options: QueryOptions {
                no_content: true,
                limit: Some((0, 0)),
                dialect: Some(2),
            },
        };
        assert_eq!(
            strings(&lower(&op)[0]),
            [
                "FT.SEARCH",
                "i",
                "*",
                "NOCONTENT",
                "LIMIT",
                "0",
                "0",
                "DIALECT",
                "2"
            ]
        );
    }

    #[test]
    fn query_rendering() {
        assert_eq!(query(Query::Term("hello".into())), "hello");
        assert_eq!(query(Query::Term("a-b c".into())), r"a\-b\ c");
        assert_eq!(
            query(Query::InField {
                field: "t".into(),
                term: "x".into()
            }),
            "@t:x"
        );
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
                Query::Term("a".into()),
                Query::Or(vec![Query::Term("b".into()), Query::Term("c".into())]),
                Query::Not(Box::new(Query::Term("d".into()))),
            ])),
            "(a (b | c) -(d))"
        );
        assert_eq!(query(Query::Or(vec![])), "*");
    }

    #[test]
    fn non_utf8_terms_pass_through_unescaped() {
        let argv = &lower(&OpKind::Query {
            index: "i".into(),
            query: Query::Term(Bytes(vec![0xe0, b'.'])),
            options: QueryOptions::default(),
        })[0];
        assert_eq!(argv[2].0, vec![0xe0, b'\\', b'.']);
    }
}
