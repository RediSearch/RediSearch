/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Line counting, test and benchmark detection for Rust sources.

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::Visit;

/// Attribute names that turn a function into a test.
const TEST_ATTRIBUTES: &[&str] = &["test", "rstest", "proptest", "apply"];

/// `rstest_reuse`'s attribute declaring a reusable set of cases. It sits on
/// an `#[rstest]` function that never runs on its own: each function applying
/// it with `#[apply(...)]` is a test instead.
const TEMPLATE_ATTRIBUTE: &str = "template";

/// Criterion methods registering a benchmark.
const BENCH_METHODS: &[&str] = &["bench_function", "bench_with_input"];

/// What a single Rust file contributes to the report.
#[derive(Default)]
pub struct FileStats {
    /// Lines of non-test code, ignoring blank lines, comments and `use`,
    /// `extern crate` and `mod foo;` declarations.
    pub code_lines: u64,
    /// Out-of-line modules declared test-only in this file, by name. Their
    /// files must not be counted as code.
    pub test_modules: Vec<String>,
    /// Functions carrying one of the [`TEST_ATTRIBUTES`], but not the
    /// [`TEMPLATE_ATTRIBUTE`].
    pub tests: u64,
    /// Calls to one of the [`BENCH_METHODS`].
    pub benchmarks: u64,
    /// Why some of the figures above may be inaccurate.
    pub warning: Option<String>,
}

/// Analyzes one Rust file, failing if it cannot be tokenized.
///
/// Some files use syntax [`syn`] rejects, such as the legacy GAT where-clause
/// placement required by `nougat`. Their test code then cannot be told apart
/// and is counted as code; [`FileStats::warning`] says so if they have any.
pub fn analyze(src: &str) -> Result<FileStats, proc_macro2::LexError> {
    // Span locations keep a copy of every file parsed on the thread, which
    // adds up to gigabytes over a history and eventually wraps their 32-bit
    // offsets. The spans of the previous file are no longer used once its
    // results are returned.
    proc_macro2::extra::invalidate_current_thread_spans();
    let tokens: TokenStream = src.parse()?;
    let mut code = BTreeSet::new();
    let mut stats = FileStats::default();
    scan_tokens(tokens.clone(), &mut code, &mut stats);

    let excluded = match syn::parse_file(src) {
        Ok(file) if is_test_only(&file.attrs) => {
            // `#![cfg(test)]`: the whole file is test code.
            return Ok(stats);
        }
        Ok(file) => {
            let mut excluded = Excluded::default();
            excluded.visit_file(&file);
            stats.test_modules = std::mem::take(&mut excluded.test_modules);
            excluded.ranges
        }
        Err(e) => {
            if stats.tests > 0 || src.contains("cfg(test") {
                let at = e.span().start();
                stats.warning = Some(format!(
                    "{}:{}: {e}; its test code is counted as code",
                    at.line, at.column
                ));
            }
            let mut ranges = Vec::new();
            declaration_ranges(tokens, &mut ranges);
            ranges
        }
    };
    stats.code_lines = code
        .iter()
        .filter(|line| !excluded.iter().any(|r| r.contains(line)))
        .count() as u64;
    Ok(stats)
}

/// Token-level fallback finding the `use`, `extern crate` and `mod foo;`
/// declarations that [`Excluded`] finds when the file can be parsed.
fn declaration_ranges(tokens: TokenStream, ranges: &mut Vec<RangeInclusive<usize>>) {
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    let is_punct =
        |t: Option<&TokenTree>, c: char| matches!(t, Some(TokenTree::Punct(p)) if p.as_char() == c);
    let is_ident =
        |t: Option<&TokenTree>, s: &str| matches!(t, Some(TokenTree::Ident(i)) if i == s);
    for (i, token) in tokens.iter().enumerate() {
        let starts_declaration = match token {
            // `impl Trait + use<'a>` is precise capturing, not an import.
            TokenTree::Ident(id) if id == "use" => {
                !is_punct(i.checked_sub(1).and_then(|p| tokens.get(p)), '+')
            }
            TokenTree::Ident(id) if id == "extern" => is_ident(tokens.get(i + 1), "crate"),
            TokenTree::Ident(id) if id == "mod" => is_punct(tokens.get(i + 2), ';'),
            TokenTree::Group(group) => {
                declaration_ranges(group.stream(), ranges);
                false
            }
            _ => false,
        };
        if starts_declaration && let Some(end) = tokens[i..].iter().find(|t| is_punct(Some(t), ';'))
        {
            ranges.push(token.span().start().line..=end.span().start().line);
        }
    }
}

/// Walks the token tree, recording which lines hold a token and counting
/// test attributes and benchmark calls.
///
/// Comments never become tokens, except doc comments which are desugared into
/// `#[doc = "..."]` attributes; those are skipped so they count as comments.
fn scan_tokens(tokens: TokenStream, code: &mut BTreeSet<usize>, stats: &mut FileStats) {
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    // End of the run of attributes already classified, so that a function
    // with several test attributes, such as `#[rstest] #[tokio::test]`,
    // counts once.
    let mut classified_until = 0;
    let mut i = 0;
    while i < tokens.len() {
        if let Some((attr, len)) = attribute_at(&tokens[i..]) {
            if is_doc_comment(&attr) {
                i += len;
                continue;
            }
            if i >= classified_until {
                let (names, end) = attribute_run(&tokens[i..]);
                classified_until = i + end;
                let is_test = names.iter().any(|n| TEST_ATTRIBUTES.contains(&n.as_str()));
                if is_test && !names.iter().any(|n| n == TEMPLATE_ATTRIBUTE) {
                    stats.tests += 1;
                }
            }
        }

        if let TokenTree::Ident(ident) = &tokens[i]
            && BENCH_METHODS.iter().any(|m| ident == m)
            && i > 0
            && matches!(&tokens[i - 1], TokenTree::Punct(p) if p.as_char() == '.')
            && matches!(tokens.get(i + 1), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis)
        {
            stats.benchmarks += 1;
        }

        match &tokens[i] {
            TokenTree::Group(group) => {
                code.insert(group.span_open().start().line);
                code.insert(group.span_close().start().line);
                scan_tokens(group.stream(), code, stats);
            }
            // Multi-line string literals cover every line they span.
            other => code.extend(lines(other.span())),
        }
        i += 1;
    }
}

/// The last path segment of each attribute of the run `tokens` starts with,
/// doc comments aside, and the number of tokens the run spans.
fn attribute_run(tokens: &[TokenTree]) -> (Vec<String>, usize) {
    let mut names = Vec::new();
    let mut end = 0;
    while let Some((attr, len)) = attribute_at(&tokens[end..]) {
        if !is_doc_comment(&attr)
            && let Some(name) = attribute_path(&attr).pop()
        {
            names.push(name);
        }
        end += len;
    }
    (names, end)
}

/// If `tokens` starts with an outer or inner attribute, returns its bracketed
/// contents and the number of tokens it spans.
fn attribute_at(tokens: &[TokenTree]) -> Option<(TokenStream, usize)> {
    let TokenTree::Punct(hash) = tokens.first()? else {
        return None;
    };
    if hash.as_char() != '#' {
        return None;
    }
    let mut next = 1;
    if let Some(TokenTree::Punct(bang)) = tokens.get(1)
        && bang.as_char() == '!'
    {
        next = 2;
    }
    match tokens.get(next)? {
        TokenTree::Group(g) if g.delimiter() == Delimiter::Bracket => Some((g.stream(), next + 1)),
        _ => None,
    }
}

/// Whether an attribute is a desugared doc comment, `#[doc = "..."]`, rather
/// than code such as `#[doc(hidden)]`.
fn is_doc_comment(attr: &TokenStream) -> bool {
    let mut tokens = attr.clone().into_iter();
    matches!(tokens.next(), Some(TokenTree::Ident(i)) if i == "doc")
        && matches!(tokens.next(), Some(TokenTree::Punct(p)) if p.as_char() == '=')
}

/// The `a::b::c` path an attribute starts with, as its segments.
fn attribute_path(attr: &TokenStream) -> Vec<String> {
    let mut path = Vec::new();
    for token in attr.clone() {
        match token {
            TokenTree::Ident(ident) => path.push(ident.to_string()),
            TokenTree::Punct(p) if p.as_char() == ':' => {}
            _ => break,
        }
    }
    path
}

fn lines(span: proc_macro2::Span) -> RangeInclusive<usize> {
    span.start().line..=span.end().line
}

/// Collects the line ranges of a file that are not counted as code.
#[derive(Default)]
struct Excluded {
    ranges: Vec<RangeInclusive<usize>>,
    test_modules: Vec<String>,
}

impl Excluded {
    fn exclude(&mut self, node: &impl Spanned) {
        self.ranges.push(lines(node.span()));
    }
}

impl<'ast> Visit<'ast> for Excluded {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        let attrs = item_attrs(item);
        if is_test_only(attrs) {
            if let syn::Item::Mod(m) = item
                && m.content.is_none()
            {
                self.test_modules.push(m.ident.to_string());
            }
            self.exclude(item);
            return;
        }
        match item {
            syn::Item::Use(_) | syn::Item::ExternCrate(_) => self.exclude(item),
            syn::Item::Mod(m) if m.content.is_none() => self.exclude(item),
            syn::Item::Fn(_) if is_test(attrs) => self.exclude(item),
            _ => syn::visit::visit_item(self, item),
        }
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        let attrs = match item {
            syn::ImplItem::Const(i) => &i.attrs,
            syn::ImplItem::Fn(i) => &i.attrs,
            syn::ImplItem::Type(i) => &i.attrs,
            syn::ImplItem::Macro(i) => &i.attrs,
            _ => return syn::visit::visit_impl_item(self, item),
        };
        if is_test_only(attrs) || is_test(attrs) {
            self.exclude(item);
        } else {
            syn::visit::visit_impl_item(self, item);
        }
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        let attrs = match item {
            syn::TraitItem::Const(i) => &i.attrs,
            syn::TraitItem::Fn(i) => &i.attrs,
            syn::TraitItem::Type(i) => &i.attrs,
            syn::TraitItem::Macro(i) => &i.attrs,
            _ => return syn::visit::visit_trait_item(self, item),
        };
        if is_test_only(attrs) {
            self.exclude(item);
        } else {
            syn::visit::visit_trait_item(self, item);
        }
    }
}

fn item_attrs(item: &syn::Item) -> &[syn::Attribute] {
    match item {
        syn::Item::Const(i) => &i.attrs,
        syn::Item::Enum(i) => &i.attrs,
        syn::Item::ExternCrate(i) => &i.attrs,
        syn::Item::Fn(i) => &i.attrs,
        syn::Item::ForeignMod(i) => &i.attrs,
        syn::Item::Impl(i) => &i.attrs,
        syn::Item::Macro(i) => &i.attrs,
        syn::Item::Mod(i) => &i.attrs,
        syn::Item::Static(i) => &i.attrs,
        syn::Item::Struct(i) => &i.attrs,
        syn::Item::Trait(i) => &i.attrs,
        syn::Item::TraitAlias(i) => &i.attrs,
        syn::Item::Type(i) => &i.attrs,
        syn::Item::Union(i) => &i.attrs,
        syn::Item::Use(i) => &i.attrs,
        _ => &[],
    }
}

fn is_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path()
            .segments
            .last()
            .is_some_and(|s| TEST_ATTRIBUTES.iter().any(|t| s.ident == t))
    })
}

/// Whether one of the `#[cfg(...)]` attributes can only hold under `cfg(test)`.
fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Meta>()
                .is_ok_and(|pred| eval_without_test(&pred) == Some(false))
    })
}

/// Evaluates a `cfg` predicate outside of test builds.
///
/// Returns `None` when the outcome depends on something else than `test`, such
/// as a feature or the target platform.
fn eval_without_test(pred: &syn::Meta) -> Option<bool> {
    match pred {
        syn::Meta::Path(path) if path.is_ident("test") => Some(false),
        syn::Meta::List(list) => {
            let args: Vec<syn::Meta> = list
                .parse_args_with(Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
                .ok()?
                .into_iter()
                .collect();
            let values: Vec<Option<bool>> = args.iter().map(eval_without_test).collect();
            if list.path.is_ident("not") {
                values.first().copied().flatten().map(|v| !v)
            } else if list.path.is_ident("all") {
                if values.contains(&Some(false)) {
                    Some(false)
                } else {
                    values.iter().all(|v| *v == Some(true)).then_some(true)
                }
            } else if list.path.is_ident("any") {
                if values.contains(&Some(true)) {
                    Some(true)
                } else {
                    values.iter().all(|v| *v == Some(false)).then_some(false)
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_code_but_not_comments_uses_or_tests() {
        let src = r#"
//! Crate docs.
use std::fmt;
use std::{
    io,
    path::Path,
};
mod inner;

/// A documented function.
pub fn add(a: u32, b: u32) -> u32 {
    // add them
    a + b
}

#[cfg(test)]
fn helper() {}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod inline_tests {
    #[test]
    fn it_works() {}
}
"#;
        let stats = analyze(src).unwrap();
        assert_eq!(stats.code_lines, 3);
        assert_eq!(stats.tests, 1);
        assert_eq!(stats.test_modules, ["tests"]);
    }

    #[test]
    fn doc_attributes_other_than_comments_are_code() {
        let src = "#![doc(test(attr(deny(warnings))))]\n#[doc(hidden)]\npub struct S;\n";
        assert_eq!(analyze(src).unwrap().code_lines, 3);
    }

    #[test]
    fn counts_multiline_strings_on_every_line() {
        let src = "const S: &str = \"a\nb\nc\";\n";
        assert_eq!(analyze(src).unwrap().code_lines, 3);
    }

    #[test]
    fn counts_tests_inside_macros_and_test_attribute_variants() {
        let src = r#"
proptest! {
    #[test]
    fn prop(x in 0..10u32) {}
}
#[rstest]
#[case(1)]
fn param(#[case] x: u32) {}
#[tokio::test]
async fn async_test() {}
"#;
        let stats = analyze(src).unwrap();
        assert_eq!(stats.tests, 3);
        // Only the macro invocation is left: syn cannot see the test inside it.
        assert_eq!(stats.code_lines, 4);
    }

    #[test]
    fn counts_rstest_reuse_applications_not_templates() {
        let src = r#"
#[template]
#[rstest]
#[case(1)]
fn cases(#[case] x: u32) {}

#[apply(cases)]
fn first(x: u32) {}

#[rstest_reuse::apply(cases)]
fn second(x: u32) {}

/// Doc comments do not split a run of attributes.
#[rstest]
/// Nor do they make a test count twice.
#[tokio::test]
async fn async_case() {}
"#;
        assert_eq!(analyze(src).unwrap().tests, 3);
    }

    #[test]
    fn counts_benchmark_registrations() {
        let src = r#"
fn benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("g");
    group.bench_function("a", |b| b.iter(|| 1));
    group.bench_with_input("b", &1, |b, i| b.iter(|| *i));
    bench_function();
}
"#;
        assert_eq!(analyze(src).unwrap().benchmarks, 2);
    }

    #[test]
    fn falls_back_to_tokens_on_unsupported_syntax() {
        let src = r#"
use std::fmt;
use std::{
    io,
};
mod inner;
impl<'a> LendingIterator for Iter<'a> {
    type Item<'next>
    where
        Self: 'next,
    = &'next u8;
}
fn f() -> impl Sized + use<> {}
"#;
        let stats = analyze(src).unwrap();
        assert_eq!(stats.code_lines, 7);
        assert!(stats.warning.is_none());
        let with_tests = format!("{src}#[cfg(test)]\nmod tests {{}}\n");
        assert!(analyze(&with_tests).unwrap().warning.is_some());
    }

    #[test]
    fn whole_file_cfg_test() {
        let src = "#![cfg(test)]\nfn helper() {}\n";
        assert_eq!(analyze(src).unwrap().code_lines, 0);
    }

    #[test]
    fn cfg_predicates() {
        let eval = |s: &str| eval_without_test(&syn::parse_str(s).unwrap());
        assert_eq!(eval("test"), Some(false));
        assert_eq!(eval("not(test)"), Some(true));
        assert_eq!(eval("all(test, unix)"), Some(false));
        assert_eq!(eval("any(test, feature = \"x\")"), None);
        assert_eq!(eval("any(test, not(test))"), Some(true));
        assert_eq!(eval("miri"), None);
    }
}
