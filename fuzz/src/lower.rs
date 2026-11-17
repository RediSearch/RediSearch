/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::program::{Argv, Bound, Bytes, FieldKind, OpKind, Query, argv};

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
