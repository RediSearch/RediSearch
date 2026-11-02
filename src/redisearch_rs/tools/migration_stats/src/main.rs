/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tracks the C to Rust migration: measures production code size, tests and
//! benchmarks of both languages at a given commit.

mod c;
mod date;
mod git;
mod history;
mod rust;

use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const RUST_ROOT: &str = "src/redisearch_rs/";

/// Crates that only support testing, benchmarking or the build.
const NON_PRODUCTION_CRATES: &[&str] = &[
    "build_utils",
    "redis_mock",
    "tracing_assert",
    "workspace_hack",
];

/// Directories of a crate that hold no production code.
const NON_PRODUCTION_CRATE_DIRS: &[&str] = &["tests", "benches", "examples"];

/// Google Test macros defining a test.
const GTEST_MACROS: &[&str] = &["TEST", "TEST_F", "TEST_P", "TYPED_TEST", "TYPED_TEST_P"];

/// The `tests/ctests` harness macros running one test function: their own
/// `TESTFUNC`, and minunit's `MU_RUN_TEST` used by the coordinator tests.
const CTEST_MACROS: &[&str] = &["TESTFUNC", "MU_RUN_TEST"];

/// Google Benchmark macros defining a benchmark. `BENCHMARK_REGISTER_F` is
/// left out as it registers a fixture benchmark already counted by its
/// `BENCHMARK_DEFINE_F`.
const GBENCH_MACROS: &[&str] = &[
    "BENCHMARK",
    "BENCHMARK_F",
    "BENCHMARK_DEFINE_F",
    "BENCHMARK_CAPTURE",
    "BENCHMARK_TEMPLATE",
    "BENCHMARK_TEMPLATE1",
    "BENCHMARK_TEMPLATE2",
];

#[derive(Default)]
struct Loc {
    files: u64,
    lines: u64,
}

impl Loc {
    const fn add(&mut self, lines: u64) {
        self.files += 1;
        self.lines += lines;
    }
}

#[derive(Default)]
struct CodeStats {
    /// C and C++, the latter being a small part ported along with the C.
    c: Loc,
    /// Rust code that does not exist to bridge with C.
    rust_core: Loc,
    /// Rust crates exposing Rust to C, or wrapping C for Rust. They shrink
    /// back as the C side goes away.
    rust_ffi: Loc,
}

#[derive(Default)]
struct TestStats {
    /// Google Test tests and `tests/ctests` test functions.
    c: u64,
    /// Unit and integration tests.
    rust: u64,
}

#[derive(Default)]
struct BenchStats {
    google_benchmark: u64,
    criterion: u64,
}

/// The measurements of one commit.
struct Stats {
    commit: String,
    /// Committer date, as `YYYY-MM-DD`.
    date: String,
    subject: String,
    code: CodeStats,
    tests: TestStats,
    benchmarks: BenchStats,
}

#[derive(clap::Parser)]
#[command(about = "Tracks the C to Rust migration of RediSearch.")]
struct Cli {
    /// The repository to measure. Defaults to the one containing the current
    /// directory.
    #[arg(long, global = true)]
    repo: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Measure C and Rust code, tests and benchmarks at one revision.
    Measure {
        /// The git revision to measure.
        #[arg(default_value = "HEAD")]
        rev: String,
    },
    /// Measure the first-parent history weekly and report its evolution, with
    /// graphs.
    History(history::Options),
}

fn main() -> Result<()> {
    let cli = <Cli as clap::Parser>::parse();
    let repo = match cli.repo {
        Some(repo) => repo,
        None => git::toplevel()?.into(),
    };
    match cli.command {
        Command::Measure { rev } => print_markdown(&measure(&repo, &rev)?),
        Command::History(options) => history::run(&repo, &options)?,
    }
    Ok(())
}

/// Measures the commit `rev` of `repo`.
fn measure(repo: &Path, rev: &str) -> Result<Stats> {
    let commit = git::resolve(repo, rev)?;
    let files = git::load_files(repo, &commit.id, is_relevant)?;

    let mut stats = Stats {
        code: c_code_stats(&files),
        tests: TestStats {
            c: count_c_macros(&files, "tests/cpptests/", GTEST_MACROS)
                + count_c_macros(&files, "tests/ctests/", CTEST_MACROS),
            rust: 0,
        },
        benchmarks: BenchStats {
            google_benchmark: count_c_macros(&files, "tests/", GBENCH_MACROS),
            ..Default::default()
        },
        commit: commit.id,
        date: commit.date,
        subject: commit.subject,
    };
    add_rust_stats(&files, &mut stats);
    Ok(stats)
}

fn extension(path: &str) -> &str {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
}

fn is_c_or_cpp(path: &str) -> bool {
    matches!(extension(path), "c" | "cc" | "cpp" | "cxx")
}

/// Whether a file of the tree is needed for any of the metrics.
fn is_relevant(path: &str) -> bool {
    if let Some(rel) = path.strip_prefix(RUST_ROOT) {
        return extension(rel) == "rs" || rel.ends_with("Cargo.toml");
    }
    if path.starts_with("src/") && matches!(extension(path), "rl" | "y") {
        // Only their presence matters: they reveal the `.c` files generated
        // from them.
        return true;
    }
    (path.starts_with("src/") || path.starts_with("tests/")) && is_c_or_cpp(path)
}

/// Sizes the hand-written C and C++ production code: everything under `src/`
/// but the Rust tree, headers and generated parsers.
fn c_code_stats(files: &BTreeMap<String, String>) -> CodeStats {
    let mut stats = CodeStats::default();
    for (path, src) in files {
        if !path.starts_with("src/") || path.starts_with(RUST_ROOT) || !is_c_or_cpp(path) {
            continue;
        }
        if c::is_generated(path, src, |p| files.contains_key(p)) {
            continue;
        }
        stats.c.add(c::count_code_lines(src));
    }
    stats
}

fn count_c_macros(files: &BTreeMap<String, String>, dir: &str, macros: &[&str]) -> u64 {
    files
        .iter()
        .filter(|(path, _)| path.starts_with(dir) && is_c_or_cpp(path))
        .map(|(_, src)| c::count_macro_invocations(src, macros))
        .sum()
}

/// Where a Rust file sits in the workspace.
struct RustFile<'a> {
    /// The crate's directory, relative to [`RUST_ROOT`].
    crate_dir: &'a str,
    /// The file's path relative to the crate directory.
    in_crate: &'a str,
}

fn locate<'a>(path: &'a str, crate_dirs: &BTreeSet<&str>) -> Option<RustFile<'a>> {
    let rel = path.strip_prefix(RUST_ROOT)?;
    let mut dir = Path::new(rel).parent();
    while let Some(d) = dir.and_then(|d| d.to_str()) {
        if crate_dirs.contains(d) {
            let in_crate = if d.is_empty() {
                rel
            } else {
                &rel[d.len() + 1..]
            };
            return Some(RustFile {
                crate_dir: d,
                in_crate,
            });
        }
        dir = Path::new(d).parent();
    }
    None
}

impl RustFile<'_> {
    fn crate_name(&self) -> &str {
        self.crate_dir.rsplit('/').next().unwrap_or(self.crate_dir)
    }

    fn is_tool(&self) -> bool {
        self.crate_dir.starts_with("tools/")
    }

    fn is_production(&self) -> bool {
        let name = self.crate_name();
        let top_dir = self.in_crate.split('/').next().unwrap_or("");
        !self.is_tool()
            && !name.ends_with("_bencher")
            && !name.ends_with("_test_utils")
            && !NON_PRODUCTION_CRATES.contains(&name)
            && !NON_PRODUCTION_CRATE_DIRS.contains(&top_dir)
            && self.in_crate != "build.rs"
    }

    fn is_ffi(&self) -> bool {
        self.crate_dir == "ffi"
            || self.crate_dir.starts_with("c_entrypoint/")
            || self.crate_dir.starts_with("c_wrappers/")
    }
}

/// The directory holding the files of the modules declared in `path`.
fn module_dir(path: &str) -> String {
    let (parent, file) = path.rsplit_once('/').unwrap_or(("", path));
    match file {
        "lib.rs" | "main.rs" | "mod.rs" => parent.to_owned(),
        _ => path.trim_end_matches(".rs").to_owned(),
    }
}

fn add_rust_stats(files: &BTreeMap<String, String>, stats: &mut Stats) {
    let crate_dirs: BTreeSet<&str> = files
        .keys()
        .filter_map(|p| p.strip_prefix(RUST_ROOT)?.strip_suffix("Cargo.toml"))
        .map(|d| d.trim_end_matches('/'))
        .collect();

    // Prefixing warnings with the commit, as in `<rev>:<path>`, tells apart
    // those of the many commits `history` measures.
    let rev = stats.commit[..10].to_owned();
    let mut analyzed = Vec::new();
    for (path, src) in files {
        if extension(path) != "rs" {
            continue;
        }
        let Some(file) = locate(path, &crate_dirs) else {
            continue;
        };
        if file.is_tool() {
            continue;
        }
        match rust::analyze(src) {
            Ok(stats) => {
                if let Some(warning) = &stats.warning {
                    eprintln!("warning: {rev}:{path}:{warning}");
                }
                analyzed.push((path.as_str(), file, stats));
            }
            Err(e) => {
                let at = e.span().start();
                eprintln!(
                    "warning: skipping {rev}:{path}:{}:{}: {e}",
                    at.line, at.column
                );
            }
        }
    }

    // Out-of-line `#[cfg(test)] mod foo;` files carry no attribute of their
    // own, so they can only be recognized from the declaring file.
    let mut test_files = BTreeSet::new();
    let mut test_dirs = Vec::new();
    for (path, _, stats) in &analyzed {
        let dir = module_dir(path);
        for module in &stats.test_modules {
            test_files.insert(format!("{dir}/{module}.rs"));
            test_dirs.push(format!("{dir}/{module}/"));
        }
    }

    for (path, file, file_stats) in &analyzed {
        stats.tests.rust += file_stats.tests;
        stats.benchmarks.criterion += file_stats.benchmarks;

        let in_test_module =
            test_files.contains(*path) || test_dirs.iter().any(|d| path.starts_with(d.as_str()));
        if file.is_production() && !in_test_module {
            let loc = if file.is_ffi() {
                &mut stats.code.rust_ffi
            } else {
                &mut stats.code.rust_core
            };
            loc.add(file_stats.code_lines);
        }
    }
}

fn percent(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

fn print_markdown(stats: &Stats) {
    let code = &stats.code;
    let rust_total = code.rust_core.lines + code.rust_ffi.lines;
    let total = code.c.lines + rust_total;

    println!("# C to Rust migration stats");
    println!();
    println!(
        "Commit `{}` ({} {})",
        stats.commit, stats.date, stats.subject
    );
    println!();
    println!("## Production code");
    println!();
    println!("| Language                | Files |   Lines | Share |");
    println!("|-------------------------|------:|--------:|------:|");
    let row = |name: &str, loc: &Loc| {
        println!(
            "| {name:<23} | {:>5} | {:>7} | {:>4.1}% |",
            loc.files,
            loc.lines,
            percent(loc.lines, total)
        );
    };
    row("C and C++", &code.c);
    row("Rust (core)", &code.rust_core);
    row("Rust (FFI and wrappers)", &code.rust_ffi);
    println!(
        "| **Rust total**          |       | {rust_total:>7} | {:>4.1}% |",
        percent(rust_total, total)
    );

    let tests = &stats.tests;
    println!();
    println!("## Tests");
    println!();
    println!("| Suite                         | Tests |");
    println!("|-------------------------------|------:|");
    println!("| C and C++                     | {:>5} |", tests.c);
    println!("| Rust                          | {:>5} |", tests.rust);

    let benches = &stats.benchmarks;
    println!();
    println!("## Benchmarks");
    println!();
    println!("| Suite                         | Benchmarks |");
    println!("|-------------------------------|-----------:|");
    println!(
        "| C++ Google Benchmark          | {:>10} |",
        benches.google_benchmark
    );
    println!(
        "| Rust Criterion                | {:>10} |",
        benches.criterion
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locates_files_in_nested_crates() {
        let crates = BTreeSet::from(["trie_rs", "c_entrypoint/trie_ffi"]);
        let file = locate(
            "src/redisearch_rs/c_entrypoint/trie_ffi/src/lib.rs",
            &crates,
        )
        .unwrap();
        assert_eq!(file.crate_dir, "c_entrypoint/trie_ffi");
        assert_eq!(file.in_crate, "src/lib.rs");
        assert!(file.is_ffi() && file.is_production());

        let file = locate("src/redisearch_rs/trie_rs/tests/it.rs", &crates).unwrap();
        assert!(!file.is_production());
    }

    #[test]
    fn excludes_c_generated_from_a_grammar() {
        let tree = [
            ("src/parser/lexer.rl", ""),
            ("src/parser/lexer.c", "int lex(void) {\n  return 0;\n}\n"),
            ("src/parser/parser.y", ""),
            ("src/parser/parser.c", "int parse(void) {\n  return 0;\n}\n"),
            ("src/query.c", "int query(void) {\n  return 0;\n}\n"),
        ];
        // Selected the way `measure` loads a tree.
        let files: BTreeMap<String, String> = tree
            .iter()
            .filter(|(path, _)| is_relevant(path))
            .map(|(path, src)| (path.to_string(), src.to_string()))
            .collect();
        let stats = c_code_stats(&files);
        assert_eq!((stats.c.files, stats.c.lines), (1, 3));
    }

    #[test]
    fn module_dirs() {
        assert_eq!(module_dir("a/src/lib.rs"), "a/src");
        assert_eq!(module_dir("a/src/x/mod.rs"), "a/src/x");
        assert_eq!(module_dir("a/src/x.rs"), "a/src/x");
    }
}
