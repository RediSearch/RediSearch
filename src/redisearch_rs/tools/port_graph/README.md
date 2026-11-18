# Port Graph

Draws the dependency graph between C modules and Rust crates, to find which C
module to port to Rust next.

## Usage

```bash
cd src/redisearch_rs
cargo port-graph                              # interactive page, see below
cargo port-graph --format dot                 # Graphviz graph
cargo port-graph --format candidates          # markdown table, see below
cargo port-graph --format dot --focus c:rlookup -o - | dot -Tsvg -o ../../bin/redisearch_rs/port_graph/rlookup.svg
```

Output goes to `port_graph/` in the Cargo target directory
(`bin/redisearch_rs/port_graph/`), out of version control, and its path is
printed; `-o <path>` writes elsewhere, `-o -` to stdout. For the whole graph:

```bash
sfdp -Tsvg -Goverlap=prism -O ../../bin/redisearch_rs/port_graph/graph.dot  # writes graph.dot.svg next to it
```

The whole graph is too dense to read in full; narrow it down with:

- `--focus <module>` (repeatable): only the given modules and their
  neighbours up to `--depth` hops away (1 by default), in both directions.
  Only edges leading one hop further from the focused modules are drawn,
  unless `--all-edges` is passed. Neighbour counts, and so the thick borders,
  still come from the whole graph.
- `--exclude <module>` (repeatable): removes a module and its edges, so it no
  longer counts as anyone's dependency. Excluded by default, as they will
  never be ported: `redismodule`, the Redis module API, and
  `query_error_compat`, `util/strconv` and `rlookup_load_document`, thin C
  layers implementing what Rust code needs; `--no-default-excludes` keeps
  them.

Modules are named `c:<path under src/ without extension>`, `rs:<crate>` or
`dep:<directory of deps/>`; the prefix may be dropped when the name is
unambiguous.

Reads the working tree; nothing needs to be built.

## The graph

Each node is a module, and points to the modules it uses, an edge standing
for the symbols one module uses from the other. Modules using each other are
joined by a single dashed red edge with an arrow at each end. Modules without
any edge, once `--exclude` has applied, are left out, as are modules made of
headers holding no function body (e.g. `err.h`, `redisearch.h`), with no code
to port; `--keep-header-only` keeps the latter.

- **C modules** (light blue): the `.c`, `.cpp`, `.h` and `.hpp` files under
  `src/`, outside `src/redisearch_rs/`. A source file and its header with the
  same path and stem form one module, named after that path under `src/`
  (e.g. `util/arr/arr`). C modules using no other C module nor dependency
  have a thick border: they can be ported on their own.
- **Rust crates**: the production crates of the workspace, in light red, or
  light purple for the FFI crates (`c_entrypoint/*` and `c_wrappers/*`),
  which bridge with C and shrink back as it goes away. Left out are the
  crates supporting tests, benchmarks or the build, as well as `ffi` and
  `redisearch_rs`, which would connect to nearly everything.
- **Dependencies** (a darker blue than C modules): one per directory of `deps/`, linked from
  the C modules including its headers. Their code is not scanned, so an edge
  to one stands for the headers included rather than symbols. Nearly every
  module includes `rmalloc.h` (allocation wrappers) and `rmutil/rm_assert.h`
  (assertion macros), so these two are ignored unless `--no-default-excludes`
  is passed.

Edges link where a name is used to where it is defined:

- **Definitions**: a C source file defines its non-`static` functions and
  global variables; a C header its inline functions, macros, types and enum
  constants. Function prototypes and `extern` declarations define nothing:
  they announce code found elsewhere, so a function declared in one header
  and implemented in another file links its callers to that file (e.g. the
  callers of `RSIndexResult_IterateOffsets`, declared in `redisearch.h`, to
  `offset_vector`). A crate defines the functions it exports
  (`#[unsafe(no_mangle)]`) and the types and constants of the header
  cheadergen generates from it in `src/redisearch_rs/headers/`.
- **C uses**: every identifier in a C file's code and macro bodies, member
  names (after `.` or `->`) left out. A name defined in several modules, such
  as a common macro, is taken from those the file includes directly, resolved
  against the C build's include directories.
- **Rust uses**: the names a crate uses through the `ffi` bindings or declares
  in its own `extern` blocks, when a single C module defines them.
- **Rust → Rust**: normal Cargo dependencies, edges without symbols.
- **C → dependency**: includes resolving under `deps/`.

Test-only code is ignored on the Rust side: `tests/` directories,
`#[cfg(test)]` items and `#[test]` functions.

## Web page

The default format writes a single page to explore the graph in a browser,
without Graphviz. It loads cytoscape.js from a CDN, so it needs a network
connection. On it:

- search a module, or click one, to focus it: the page shows its neighbours up
  to the chosen depth, with the same edge filtering as `--focus`, and lists
  every module it uses and is used by, each clickable, with the number of
  symbols behind the link;
- hover an edge, or a module in those lists, to see the symbols it stands
  for: the API to bridge or port first when porting one side;
- hide modules, and toggle C modules and Rust crates;
- size each module by its lines of code, see *Sizes*;
- the port candidates table can be sorted, and focuses a module on click;
- the view is kept in the URL fragment, so a link reopens it.

`--exclude` removes modules from the page altogether; `--focus` (a single
one), `--depth` and `--all-edges` set the view it opens on, until a view is
saved in the URL fragment.

## Sizes

Each module carries its lines of code, shown in the page, the DOT tooltips
and the candidates table: blank lines and comments are left out, as are
`#include` directives for C and test-only items (`#[cfg(test)]` modules,
`#[test]` functions) for Rust. A C module counts its source and header files,
a crate its `src/`, a dependency its whole `deps/` directory.

## Candidates

`--format candidates` lists every C module with its number of C and Rust
neighbours, those using the fewest C modules first and, among them, those used
by the most Rust crates: porting one removes FFI from each of these crates.

## Unused FFI crates

An FFI crate no module uses is a bridge left over from a port, or one whose
other side is not wired yet. Such crates are kept in the graph even without
any edge, drawn with a dashed border, listed in the page's sidebar and at the
end of `--format candidates`. Only `src/` is scanned, so a crate used by tests
alone is listed too.

## Limitations

C files are scanned with a heuristic rather than a C parser, and some Rust
files cannot be parsed, so a few edges may be missing or spurious, in
particular:

- Names are matched across the whole codebase: a local variable named like a
  global symbol of another module links to it.
- Calls through function pointers are not seen.
- Conditional compilation is ignored: both branches of an `#if` are scanned.
- Rust files `syn` cannot parse, such as those written in the syntax the
  `#[gat]` attribute takes, fall back to a token scan: only paths through
  `ffi` are found there, test-only code included.
