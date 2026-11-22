/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The tree-sitter syntax trees both languages are scanned from.

use anyhow::{Context, Result};
use std::collections::BTreeSet;
use tree_sitter::{Language, Node, Parser, Tree};

/// Node kinds holding comments, across the grammars.
const COMMENTS: &[&str] = &["comment", "line_comment", "block_comment"];

/// A file's syntax tree.
pub struct Syntax<'a> {
    source: &'a [u8],
    tree: Tree,
}

impl<'a> Syntax<'a> {
    pub fn parse(source: &'a [u8], language: &Language) -> Result<Self> {
        Self::parse_as(source, source, language)
    }

    /// Parses `parsed` in place of `source`, which it must match byte for
    /// byte wherever the tree is read.
    pub fn parse_as(source: &'a [u8], parsed: &[u8], language: &Language) -> Result<Self> {
        let mut parser = Parser::new();
        parser.set_language(language)?;
        let tree = parser
            .parse(parsed, None)
            .context("tree-sitter parse cancelled")?;
        Ok(Self { source, tree })
    }

    pub fn root(&self) -> Node<'_> {
        self.tree.root_node()
    }

    pub fn text(&self, node: Node) -> &'a str {
        std::str::from_utf8(&self.source[node.byte_range()]).unwrap_or("")
    }

    /// The number of syntax errors tree-sitter recovered from.
    pub fn error_count(&self) -> usize {
        let mut count = 0;
        self.walk(|node| {
            if node.is_error() || node.is_missing() {
                count += 1;
            }
            if node.has_error() {
                Walk::Into
            } else {
                Walk::Over
            }
        });
        count
    }

    /// Lines holding code: those with a token outside comments and outside the
    /// nodes `skip` selects.
    pub fn code_lines(&self, skip: impl Fn(Node) -> bool) -> usize {
        let mut lines = BTreeSet::new();
        self.walk(|node| {
            if COMMENTS.contains(&node.kind()) || skip(node) {
                return Walk::Over;
            }
            if node.child_count() > 0 {
                return Walk::Into;
            }
            let (start, end) = (node.start_position(), node.end_position());
            // A token ending with a newline, such as a C directive's, ends at
            // the start of the next line.
            let last = if end.column == 0 && end.row > start.row {
                end.row - 1
            } else {
                end.row
            };
            lines.extend(start.row..=last);
            Walk::Over
        });
        lines.len()
    }

    pub fn walk(&self, visit: impl FnMut(Node<'_>) -> Walk) {
        walk_from(self.root(), visit);
    }
}

/// Whether [`walk_from`] visits a node's children.
#[derive(PartialEq, Eq)]
pub enum Walk {
    Into,
    Over,
}

/// Visits `root` and its descendants depth first, without recursing so that
/// deeply nested expressions cannot overflow the stack.
pub fn walk_from(root: Node, mut visit: impl FnMut(Node<'_>) -> Walk) {
    let mut cursor = root.walk();
    loop {
        if visit(cursor.node()) == Walk::Into && cursor.goto_first_child() {
            continue;
        }
        loop {
            if cursor.node() == root {
                return;
            }
            if cursor.goto_next_sibling() {
                break;
            }
            if !cursor.goto_parent() {
                return;
            }
        }
    }
}
