/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Draws the dependency graph between C modules and Rust crates, to find
//! which C module to port to Rust next.

mod c;
mod rust;
mod syntax;

use anyhow::{Context, Result, bail};
use cargo_metadata::MetadataCommand;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// Light blue.
const C_COLOR: &str = "#cfe3fa";
/// Light red, after Ferris.
const RUST_COLOR: &str = "#fbc8bd";
/// Light purple, between [`C_COLOR`] and [`RUST_COLOR`] as FFI crates are
/// between the two languages.
const FFI_COLOR: &str = "#e2d5f7";
/// A darker shade of [`C_COLOR`]: dependencies are C code too.
const DEP_COLOR: &str = "#9dbfe8";

/// Headers of `deps/` ignored unless `--no-default-excludes` is passed, as
/// nearly every module includes them for helpers Rust has its own way of
/// doing: the allocator wrappers and the assertion macros.
const IGNORED_DEP_HEADERS: &[&str] = &["deps/rmalloc/rmalloc.h", "deps/rmutil/rm_assert.h"];

/// Modules left out unless `--no-default-excludes` is passed, as they will
/// never be ported:
/// - `redismodule`: the Redis module API, used by nearly every C module.
/// - `query_error_compat`, `util/strconv` and `rlookup_load_document`: thin C
///   layers implementing what Rust code needs.
const DEFAULT_EXCLUDES: &[&str] = &[
    "c:redismodule",
    "c:query_error_compat",
    "c:util/strconv",
    "c:rlookup_load_document",
];

#[derive(clap::Parser)]
#[command(about)]
struct Cli {
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Html)]
    format: Format,
    /// Only show these modules and their neighbours. Repeatable, except for
    /// the web page, which opens on a single module.
    ///
    /// A module is named `c:<path under src/ without extension>`,
    /// `rs:<crate>` or `dep:<directory of deps/>`; the prefix may be dropped
    /// when the name is unambiguous.
    #[arg(long, value_name = "MODULE")]
    focus: Vec<String>,
    /// How many hops away from a focused module to show, following edges in
    /// both directions.
    #[arg(long, default_value_t = 1, requires = "focus")]
    depth: usize,
    /// With `--focus`, also draw the edges between modules at the same
    /// distance from the focused ones, left out by default as they bury the
    /// edges that matter.
    #[arg(long, requires = "focus")]
    all_edges: bool,
    /// Remove a module from the graph, along with its edges. Repeatable. An
    /// excluded module no longer counts as a dependency of the others.
    #[arg(long, value_name = "MODULE")]
    exclude: Vec<String>,
    /// Keep the modules excluded by default, see `DEFAULT_EXCLUDES`.
    #[arg(long)]
    no_default_excludes: bool,
    /// Keep the modules made of headers holding no function body. They are
    /// left out by default, as there is no code to port.
    #[arg(long)]
    keep_header_only: bool,
    /// Where to write the output, `-` for stdout. Defaults to a file under
    /// `port_graph/` in the Cargo target directory, which version control
    /// ignores.
    #[arg(long, short)]
    output: Option<PathBuf>,
    /// Path to the workspace `Cargo.toml`.
    #[arg(long, default_value = "Cargo.toml")]
    manifest_path: PathBuf,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Format {
    /// A Graphviz graph; each node points to the modules it uses.
    Dot,
    /// A markdown table of the C modules, the readiest to port first.
    Candidates,
    /// A self-contained web page to explore the graph. `--focus` and
    /// `--depth` set the view it opens on.
    Html,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Node {
    C(String),
    Rust(String),
    /// A directory of `deps/`.
    Dep(String),
}

impl Node {
    fn id(&self) -> String {
        match self {
            Self::C(name) => format!("c:{name}"),
            Self::Rust(name) => format!("rs:{name}"),
            Self::Dep(name) => format!("dep:{name}"),
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::C(name) | Self::Rust(name) | Self::Dep(name) => name,
        }
    }
}

/// What a node is, beyond its language.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    C,
    Rust(rust::CrateKind),
    Dep,
}

impl Kind {
    const fn color(self) -> &'static str {
        match self {
            Self::C => C_COLOR,
            Self::Rust(rust::CrateKind::Core) => RUST_COLOR,
            Self::Rust(rust::CrateKind::Ffi) => FFI_COLOR,
            Self::Dep => DEP_COLOR,
        }
    }

    /// The name the web page knows it by.
    const fn page_name(self) -> &'static str {
        match self {
            Self::C => "c",
            Self::Rust(rust::CrateKind::Core) => "rust",
            Self::Rust(rust::CrateKind::Ffi) => "ffi",
            Self::Dep => "dep",
        }
    }
}

#[derive(Default)]
struct Graph {
    nodes: BTreeSet<Node>,
    /// The kind of each crate; crates missing from it are [`rust::CrateKind::Core`].
    crate_kinds: BTreeMap<String, rust::CrateKind>,
    /// Lines of code of each node, see [`c::CFile::lines`].
    lines: BTreeMap<Node, usize>,
    /// `(user, used)` pairs, with the symbols the user uses from the used
    /// module; empty for a Cargo dependency.
    edges: BTreeMap<(Node, Node), BTreeSet<String>>,
}

impl Graph {
    fn kind(&self, node: &Node) -> Kind {
        match node {
            Node::C(_) => Kind::C,
            Node::Dep(_) => Kind::Dep,
            Node::Rust(name) => Kind::Rust(
                self.crate_kinds
                    .get(name)
                    .copied()
                    .unwrap_or(rust::CrateKind::Core),
            ),
        }
    }

    fn add_edge(&mut self, user: Node, used: Node) {
        if user != used {
            self.edges.entry((user, used)).or_default();
        }
    }

    /// Records that `user` uses `symbol`, defined in `used`.
    fn add_use(&mut self, user: Node, used: Node, symbol: &str) {
        if user != used {
            self.edges
                .entry((user, used))
                .or_default()
                .insert(symbol.to_owned());
        }
    }

    fn uses<'a>(&'a self, node: &'a Node) -> impl Iterator<Item = &'a Node> {
        self.edges
            .keys()
            .filter(move |(user, _)| user == node)
            .map(|(_, used)| used)
    }

    /// Parses a module name given on the command line, see [`Cli::focus`].
    fn find(&self, name: &str) -> Result<Node> {
        let candidates: Vec<&Node> = self
            .nodes
            .iter()
            .filter(|n| n.id() == name || n.name() == name)
            .collect();
        match candidates.as_slice() {
            [node] => Ok((*node).clone()),
            [] => bail!("no module named `{name}`"),
            _ => bail!("`{name}` is ambiguous, prefix it with `c:`, `rs:` or `dep:`"),
        }
    }

    fn remove(&mut self, node: &Node) {
        self.nodes.remove(node);
        self.edges
            .retain(|(user, used), _| user != node && used != node);
    }

    /// Removes the nodes without edges, which say nothing about porting order.
    /// FFI crates are kept, as one nobody uses is worth knowing about, see
    /// [`Graph::is_unused_ffi`].
    fn remove_isolated(&mut self) {
        let connected: BTreeSet<Node> = self
            .edges
            .keys()
            .flat_map(|(a, b)| [a.clone(), b.clone()])
            .collect();
        let ffi: BTreeSet<Node> = self
            .nodes
            .iter()
            .filter(|n| self.kind(n) == Kind::Rust(rust::CrateKind::Ffi))
            .cloned()
            .collect();
        self.nodes
            .retain(|n| connected.contains(n) || ffi.contains(n));
    }

    /// Whether `node` is an FFI crate no module uses: a bridge left over from
    /// a port, or one whose other side is not wired yet.
    fn is_unused_ffi(&self, node: &Node) -> bool {
        self.kind(node) == Kind::Rust(rust::CrateKind::Ffi) && self.users(node).next().is_none()
    }

    /// The nodes at most `depth` edges away from `from`, in either direction,
    /// with their distance.
    fn neighbourhood(&self, from: &[Node], depth: usize) -> BTreeMap<Node, usize> {
        let mut seen: BTreeMap<Node, usize> = from.iter().map(|n| (n.clone(), 0)).collect();
        let mut frontier: BTreeSet<Node> = from.iter().cloned().collect();
        for hops in 1..=depth {
            let mut next = BTreeSet::new();
            for (user, used) in self.edges.keys() {
                if frontier.contains(user) && !seen.contains_key(used) {
                    next.insert(used.clone());
                }
                if frontier.contains(used) && !seen.contains_key(user) {
                    next.insert(user.clone());
                }
            }
            seen.extend(next.iter().map(|n| (n.clone(), hops)));
            frontier = next;
        }
        seen
    }

    fn users<'a>(&'a self, node: &'a Node) -> impl Iterator<Item = &'a Node> {
        self.edges
            .keys()
            .filter(move |(_, used)| used == node)
            .map(|(user, _)| user)
    }
}

fn main() -> Result<()> {
    let cli = <Cli as clap::Parser>::parse();
    let metadata = MetadataCommand::new()
        .manifest_path(&cli.manifest_path)
        .no_deps()
        .exec()
        .context("running cargo metadata")?;
    let workspace_root = metadata.workspace_root.as_std_path();
    let Some(repo_root) = workspace_root.parent().and_then(|p| p.parent()) else {
        bail!("unexpected workspace root {}", workspace_root.display());
    };

    let c_files = c::scan(repo_root, "src", Some("src/redisearch_rs"))?;
    let crates = rust::scan(&metadata)?;
    let headers_dir = workspace_root.join("headers");
    let headers_dir = headers_dir.strip_prefix(repo_root)?;
    let headers = rust::generated_headers(workspace_root, headers_dir, &crates)?;
    let generated = c::scan(repo_root, &headers_dir.to_string_lossy(), None)?;

    // Where each shared name is defined: C modules, the crates exporting
    // functions, and those whose generated header defines types or constants.
    let mut definers: BTreeMap<&str, BTreeSet<Node>> = BTreeMap::new();
    for file in &c_files {
        for symbol in &file.symbols.defined {
            definers
                .entry(symbol)
                .or_default()
                .insert(Node::C(file.module.clone()));
        }
    }
    for file in &generated {
        let Some(krate) = headers.get(&file.path) else {
            continue;
        };
        for symbol in &file.symbols.defined {
            definers
                .entry(symbol)
                .or_default()
                .insert(Node::Rust(krate.clone()));
        }
    }
    for krate in &crates {
        for function in &krate.exported {
            definers
                .entry(function)
                .or_default()
                .insert(Node::Rust(krate.name.clone()));
        }
    }

    let mut graph = Graph::default();
    graph
        .nodes
        .extend(c_files.iter().map(|f| Node::C(f.module.clone())));
    graph
        .nodes
        .extend(crates.iter().map(|c| Node::Rust(c.name.clone())));
    graph
        .crate_kinds
        .extend(crates.iter().map(|c| (c.name.clone(), c.kind)));
    for file in &c_files {
        *graph.lines.entry(Node::C(file.module.clone())).or_default() += file.lines;
    }
    graph
        .lines
        .extend(crates.iter().map(|c| (Node::Rust(c.name.clone()), c.lines)));

    let module_of: BTreeMap<&Path, &str> = c_files
        .iter()
        .map(|f| (f.path.as_path(), f.module.as_str()))
        .collect();
    for file in &c_files {
        let user = Node::C(file.module.clone());
        // A name defined in several modules, such as a common macro, is
        // taken from those the file includes.
        let mut included = BTreeSet::new();
        for include in &file.includes {
            let exists = |p: &Path| {
                module_of.contains_key(p)
                    || headers.contains_key(p)
                    || (p.starts_with("deps") && repo_root.join(p).is_file())
            };
            let Some(path) = c::resolve(include, &file.path, exists) else {
                continue;
            };
            if let Some(module) = module_of.get(path.as_path()) {
                included.insert(Node::C((*module).to_owned()));
            } else if let Some(krate) = headers.get(&path) {
                included.insert(Node::Rust(krate.clone()));
            } else if let Ok(in_deps) = path.strip_prefix("deps") {
                let ignored = !cli.no_default_excludes
                    && IGNORED_DEP_HEADERS.iter().any(|h| path == Path::new(h));
                let Some(dir) = in_deps.components().next() else {
                    continue;
                };
                if !ignored {
                    // Dependencies are not scanned: the edge stands for the
                    // headers included.
                    let dep = Node::Dep(dir.as_os_str().to_string_lossy().into_owned());
                    if graph.nodes.insert(dep.clone()) {
                        graph.lines.insert(
                            dep.clone(),
                            c::dir_code_lines(&repo_root.join("deps").join(dep.name())),
                        );
                    }
                    graph.add_use(user.clone(), dep, &in_deps.to_string_lossy());
                }
            }
        }
        for symbol in &file.symbols.used {
            let Some(defining) = definers.get(symbol.as_str()) else {
                continue;
            };
            if defining.contains(&user) {
                continue;
            }
            let chosen: Vec<&Node> = if defining.len() == 1 {
                defining.iter().collect()
            } else {
                defining.intersection(&included).collect()
            };
            for used in chosen {
                graph.add_use(user.clone(), used.clone(), symbol);
            }
        }
    }

    for krate in &crates {
        let user = Node::Rust(krate.name.clone());
        for dependency in &krate.dependencies {
            graph.add_edge(user.clone(), Node::Rust(dependency.clone()));
        }
        // Crates reach each other through Cargo; through the bindings, only C
        // is of interest, and only when the name is defined once.
        for name in &krate.c_names {
            if let Some(defining) = definers.get(name.as_str())
                && let [used @ Node::C(_)] = Vec::from_iter(defining).as_slice()
            {
                graph.add_use(user.clone(), (*used).clone(), name);
            }
        }
    }

    // Headers holding no code and implemented nowhere: nothing to port.
    let with_source = c::modules_with_source(&c_files);
    let with_bodies: BTreeSet<&str> = c_files
        .iter()
        .filter(|f| f.symbols.has_function_bodies)
        .map(|f| f.module.as_str())
        .collect();
    let header_only: BTreeSet<Node> = c_files
        .iter()
        .map(|f| f.module.as_str())
        .filter(|m| !with_source.contains(m) && !with_bodies.contains(m))
        .map(|m| Node::C(m.to_owned()))
        .collect();

    let defaults = DEFAULT_EXCLUDES
        .iter()
        .filter(|_| !cli.no_default_excludes)
        .map(|s| s.to_string());
    for name in defaults.chain(cli.exclude.iter().cloned()) {
        let node = graph.find(&name)?;
        graph.remove(&node);
    }
    if !cli.keep_header_only {
        for node in &header_only {
            graph.remove(node);
        }
    }
    // After the exclusions, which can leave modules without edges.
    graph.remove_isolated();
    if matches!(cli.format, Format::Html) && cli.focus.len() > 1 {
        bail!("the page opens on a single module: pass one --focus with --format html");
    }
    let focus = cli
        .focus
        .iter()
        .map(|name| graph.find(name))
        .collect::<Result<Vec<_>>>()?;
    let view = View {
        hops: if focus.is_empty() {
            graph.nodes.iter().map(|n| (n.clone(), 0)).collect()
        } else {
            graph.neighbourhood(&focus, cli.depth)
        },
        edges: if focus.is_empty() || cli.all_edges {
            EdgeFilter::All
        } else {
            EdgeFilter::OneHopFurther
        },
        focus: focus.into_iter().collect(),
        depth: cli.depth,
        graph: &graph,
    };

    let output = match cli.format {
        Format::Dot => dot(&view),
        Format::Candidates => candidates(&view),
        Format::Html => html(&view),
    };
    let path = cli.output.unwrap_or_else(|| {
        metadata
            .target_directory
            .as_std_path()
            .join("port_graph")
            .join(cli.format.file_name())
    });
    if path == Path::new("-") {
        print!("{output}");
    } else {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        std::fs::write(&path, output).with_context(|| format!("writing {}", path.display()))?;
        // The target directory comes from `.cargo/config.toml` as a relative path.
        println!("{}", std::fs::canonicalize(&path).unwrap_or(path).display());
    }
    Ok(())
}

impl Format {
    const fn file_name(self) -> &'static str {
        match self {
            Self::Dot => "graph.dot",
            Self::Candidates => "candidates.md",
            Self::Html => "graph.html",
        }
    }
}

/// The part of the graph to output. Neighbour counts always come from the
/// whole graph, so that a focused view does not make modules look portable.
struct View<'a> {
    graph: &'a Graph,
    /// The visible nodes, with their distance to the nearest focused node.
    hops: BTreeMap<Node, usize>,
    focus: BTreeSet<Node>,
    /// The `--depth` the view was made with.
    depth: usize,
    edges: EdgeFilter,
}

/// Which edges between visible nodes to draw.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EdgeFilter {
    All,
    /// Only those leading one hop further away from the focused nodes.
    OneHopFurther,
}

impl View<'_> {
    fn shows_edge(&self, user: &Node, used: &Node) -> bool {
        match (self.hops.get(user), self.hops.get(used), self.edges) {
            (Some(_), Some(_), EdgeFilter::All) => true,
            (Some(a), Some(b), EdgeFilter::OneHopFurther) => a.abs_diff(*b) == 1,
            _ => false,
        }
    }
}

/// Counts of a C module's neighbours, by language.
struct Neighbours {
    uses_c: usize,
    uses_rust: usize,
    uses_deps: usize,
    used_by_c: usize,
    used_by_rust: usize,
}

impl Neighbours {
    fn of(graph: &Graph, node: &Node) -> Self {
        let count = |nodes: &mut dyn Iterator<Item = &Node>| {
            nodes.fold((0, 0, 0), |(c, rust, deps), n| match n {
                Node::C(_) => (c + 1, rust, deps),
                Node::Rust(_) => (c, rust + 1, deps),
                Node::Dep(_) => (c, rust, deps + 1),
            })
        };
        let (uses_c, uses_rust, uses_deps) = count(&mut graph.uses(node));
        let (used_by_c, used_by_rust, _) = count(&mut graph.users(node));
        Self {
            uses_c,
            uses_rust,
            uses_deps,
            used_by_c,
            used_by_rust,
        }
    }

    /// A C module using no other C module nor dependency can be ported on
    /// its own.
    const fn is_leaf(&self) -> bool {
        self.uses_c == 0 && self.uses_deps == 0
    }
}

fn dot(view: &View) -> String {
    let mut out = String::new();
    out.push_str("digraph port_graph {\n");
    if !view.focus.is_empty() {
        // Users on the left, dependencies on the right of the focused modules.
        out.push_str("  rankdir=LR;\n");
    }
    out.push_str("  node [shape=box, style=\"filled,rounded\", fontname=\"Helvetica\"];\n");
    for node in view.hops.keys() {
        let (color, mut extra) = match node {
            Node::C(_) => {
                let n = Neighbours::of(view.graph, node);
                let tooltip = format!(
                    "{} lines; uses {} C / {} Rust / {} deps; used by {} C / {} Rust",
                    view.graph.lines.get(node).copied().unwrap_or_default(),
                    n.uses_c,
                    n.uses_rust,
                    n.uses_deps,
                    n.used_by_c,
                    n.used_by_rust
                );
                // Leaves get a thick border so they stand out without colour.
                let border = if n.is_leaf() { ", penwidth=3" } else { "" };
                (C_COLOR, format!(", tooltip=\"{tooltip}\"{border}"))
            }
            Node::Rust(_) if view.graph.is_unused_ffi(node) => (
                view.graph.kind(node).color(),
                ", style=\"filled,rounded,dashed\", tooltip=\"FFI crate no module uses\""
                    .to_owned(),
            ),
            Node::Rust(_) | Node::Dep(_) => (
                view.graph.kind(node).color(),
                format!(
                    ", tooltip=\"{} lines\"",
                    view.graph.lines.get(node).copied().unwrap_or_default()
                ),
            ),
        };
        if view.focus.contains(node) {
            extra.push_str(", peripheries=2, fontsize=20");
        }
        let _ = writeln!(
            out,
            "  \"{}\" [label=\"{}\", fillcolor=\"{color}\"{extra}];",
            node.id(),
            node.name()
        );
    }
    for ((user, used), symbols) in &view.graph.edges {
        if !view.shows_edge(user, used) {
            continue;
        }
        let mut tooltip = symbols.iter().cloned().collect::<Vec<_>>().join(", ");
        // Modules using each other get one edge, which Graphviz would
        // otherwise draw as two overlapping ones.
        let style = match view.graph.edges.get(&(used.clone(), user.clone())) {
            Some(_) if used < user => continue,
            Some(back) => {
                let back = back.iter().cloned().collect::<Vec<_>>().join(", ");
                tooltip = format!(
                    "{} uses: {tooltip}\\n{} uses: {back}",
                    user.name(),
                    used.name()
                );
                ", dir=both, style=dashed, color=\"#d0453a\", penwidth=2"
            }
            None => "",
        };
        let _ = writeln!(
            out,
            "  \"{}\" -> \"{}\" [tooltip=\"{}\"{style}];",
            user.id(),
            used.id(),
            tooltip.replace('"', "\\\"")
        );
    }
    out.push_str("}\n");
    out
}

fn candidates(view: &View) -> String {
    let mut rows: Vec<(&str, Neighbours)> = view
        .hops
        .keys()
        .filter(|n| matches!(n, Node::C(_)))
        .map(|n| (n.name(), Neighbours::of(view.graph, n)))
        .collect();
    // Fewest C dependencies first, then the most Rust users: each port removes
    // FFI from the crates using the module.
    rows.sort_by_key(|(name, n)| {
        (
            n.uses_c,
            n.uses_deps,
            std::cmp::Reverse(n.used_by_rust),
            *name,
        )
    });

    let mut out = String::new();
    out.push_str(
        "| C module | Lines | Uses C | Uses deps | Uses Rust | Used by C | Used by Rust |\n",
    );
    out.push_str(
        "|----------|------:|-------:|----------:|----------:|----------:|-------------:|\n",
    );
    for (name, n) in rows {
        let lines = view
            .graph
            .lines
            .get(&Node::C(name.to_owned()))
            .copied()
            .unwrap_or_default();
        let _ = writeln!(
            out,
            "| {name} | {lines} | {} | {} | {} | {} | {} |",
            n.uses_c, n.uses_deps, n.uses_rust, n.used_by_c, n.used_by_rust
        );
    }
    let unused: Vec<&Node> = view
        .hops
        .keys()
        .filter(|n| view.graph.is_unused_ffi(n))
        .collect();
    if !unused.is_empty() {
        out.push_str("\n## Unused FFI crates\n\n");
        for node in unused {
            let _ = writeln!(out, "- {}", node.name());
        }
    }
    out
}

/// The page, loading its libraries from CDNs, with the graph spliced in.
const PAGE: &str = include_str!("page.html");

fn html(view: &View) -> String {
    let nodes: Vec<_> = view
        .graph
        .nodes
        .iter()
        .map(|node| {
            let n = Neighbours::of(view.graph, node);
            serde_json::json!({
                "id": node.id(),
                "name": node.name(),
                "lang": view.graph.kind(node).page_name(),
                "usesC": n.uses_c,
                "usesRust": n.uses_rust,
                "usesDeps": n.uses_deps,
                "lines": view.graph.lines.get(node).copied().unwrap_or_default(),
                "usedByC": n.used_by_c,
                "usedByRust": n.used_by_rust,
                "unused": view.graph.is_unused_ffi(node),
            })
        })
        .collect();
    let edges: Vec<_> = view
        .graph
        .edges
        .iter()
        .map(|((user, used), symbols)| serde_json::json!([user.id(), used.id(), symbols]))
        .collect();
    let data = serde_json::json!({
        "nodes": nodes,
        "edges": edges,
        "focus": view.focus.first().map(Node::id),
        "drawAllEdges": !view.focus.is_empty() && view.edges == EdgeFilter::All,
        "depth": view.depth,
    });
    // Module names cannot hold `</script>`, but the page must not depend on it.
    let data = data.to_string().replace("</", "<\\/");
    PAGE.replace("__PORT_GRAPH_DATA__", &data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `a -> b -> c -> d`, plus a C module and a crate both named `x`.
    fn chain() -> Graph {
        let mut graph = Graph::default();
        let [a, b, c, d] = ["a", "b", "c", "d"].map(|n| Node::C(n.to_owned()));
        graph
            .nodes
            .extend([a.clone(), b.clone(), c.clone(), d.clone()]);
        graph.nodes.insert(Node::C("x".to_owned()));
        graph.nodes.insert(Node::Rust("x".to_owned()));
        graph.add_edge(a, b.clone());
        graph.add_edge(b, c.clone());
        graph.add_edge(c, d);
        graph
    }

    #[test]
    fn neighbourhood_follows_edges_both_ways() {
        let graph = chain();
        let names = |depth| -> Vec<String> {
            graph
                .neighbourhood(&[Node::C("b".to_owned())], depth)
                .keys()
                .map(|n| n.name().to_owned())
                .collect()
        };
        assert_eq!(names(0), ["b"]);
        assert_eq!(names(1), ["a", "b", "c"]);
        assert_eq!(names(2), ["a", "b", "c", "d"]);
    }

    #[test]
    fn find_requires_a_prefix_when_ambiguous() {
        let graph = chain();
        assert!(graph.find("a").is_ok_and(|n| n == Node::C("a".to_owned())));
        assert!(
            graph
                .find("rs:x")
                .is_ok_and(|n| n == Node::Rust("x".to_owned()))
        );
        assert!(graph.find("x").is_err());
        assert!(graph.find("nope").is_err());
    }

    #[test]
    fn html_embeds_the_graph_and_initial_focus() {
        let graph = chain();
        let focus = Node::C("b".to_owned());
        let view = View {
            hops: graph.neighbourhood(std::slice::from_ref(&focus), 1),
            focus: [focus].into(),
            depth: 1,
            edges: EdgeFilter::OneHopFurther,
            graph: &graph,
        };
        let page = html(&view);
        let start = page.find("const DATA = ").unwrap() + "const DATA = ".len();
        let end = start + page[start..].find(";\n").unwrap();
        let data: serde_json::Value = serde_json::from_str(&page[start..end]).unwrap();
        // The page draws the whole graph, not just the focused view.
        assert_eq!(data["nodes"].as_array().unwrap().len(), graph.nodes.len());
        assert_eq!(data["edges"][0], serde_json::json!(["c:a", "c:b", []]));
        assert_eq!(data["focus"], "c:b");
        assert_eq!(data["drawAllEdges"], false);
        assert!(!page.contains("__PORT_GRAPH_DATA__"));
    }

    #[test]
    fn remove_isolated_keeps_connected_nodes() {
        let mut graph = chain();
        graph.remove_isolated();
        let names: Vec<_> = graph.nodes.iter().map(Node::id).collect();
        assert_eq!(names, ["c:a", "c:b", "c:c", "c:d"]);
    }

    #[test]
    fn dot_draws_mutual_edges_once() {
        let mut graph = chain();
        graph.add_edge(Node::C("b".to_owned()), Node::C("a".to_owned()));
        let view = View {
            hops: graph.nodes.iter().map(|n| (n.clone(), 0)).collect(),
            focus: BTreeSet::new(),
            depth: 1,
            edges: EdgeFilter::All,
            graph: &graph,
        };
        let out = dot(&view);
        let between_a_and_b: Vec<_> = out
            .lines()
            .filter(|l| l.contains("\"c:a\"") && l.contains("\"c:b\"") && l.contains("->"))
            .collect();
        assert_eq!(between_a_and_b.len(), 1);
        assert!(between_a_and_b[0].contains("dir=both"));
    }

    #[test]
    fn edges_collect_their_symbols() {
        let mut graph = Graph::default();
        let [a, b] = ["a", "b"].map(|n| Node::C(n.to_owned()));
        graph.add_use(a.clone(), b.clone(), "B_New");
        graph.add_use(a.clone(), b.clone(), "B_Free");
        graph.add_use(a.clone(), a.clone(), "A_Own");
        assert_eq!(graph.edges.len(), 1);
        let symbols: Vec<_> = graph.edges[&(a, b)].iter().cloned().collect();
        assert_eq!(symbols, ["B_Free", "B_New"]);
    }

    #[test]
    fn unused_ffi_crates_are_kept_and_reported() {
        let mut graph = chain();
        let [used, unused] = ["used_ffi", "unused_ffi"].map(|n| Node::Rust(n.to_owned()));
        for node in [&used, &unused] {
            graph.nodes.insert(node.clone());
            graph
                .crate_kinds
                .insert(node.name().to_owned(), rust::CrateKind::Ffi);
        }
        graph.add_use(Node::C("a".to_owned()), used.clone(), "Used_New");
        graph.remove_isolated();
        assert!(graph.nodes.contains(&unused));
        assert!(graph.is_unused_ffi(&unused));
        assert!(!graph.is_unused_ffi(&used));
        assert!(!graph.is_unused_ffi(&Node::C("a".to_owned())));
    }

    #[test]
    fn dependencies_keep_modules_from_being_leaves() {
        let mut graph = Graph::default();
        let [user, dep] = [
            Node::C("phonetic_manager".to_owned()),
            Node::Dep("phonetics".to_owned()),
        ];
        graph.add_use(
            user.clone(),
            dep.clone(),
            "phonetics/double_metaphone_capi.h",
        );
        let n = Neighbours::of(&graph, &user);
        assert_eq!((n.uses_c, n.uses_deps), (0, 1));
        assert!(!n.is_leaf());
        assert_eq!(dep.id(), "dep:phonetics");
        assert_eq!(graph.kind(&dep), Kind::Dep);
    }

    #[test]
    fn remove_drops_edges() {
        let mut graph = chain();
        graph.remove(&Node::C("b".to_owned()));
        assert_eq!(graph.edges.len(), 1);
        assert!(!graph.nodes.contains(&Node::C("b".to_owned())));
    }
}
