/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Measures the history weekly and reports how the migration evolved.

use crate::date::Date;
use crate::{Stats, git, measure};
use anyhow::{Context, Result, bail};
use plotters::prelude::*;
use std::fmt::Write as _;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The commit integrating Rust into the build, i.e. the start of the port.
pub const DEFAULT_BASELINE: &str = "fe9ec7aef24fd441dae4ac800e3ec5a788539c59";

const WEEK: i64 = 7 * 24 * 60 * 60;

#[derive(clap::Args)]
pub struct Options {
    /// Directory receiving the report, its graphs and data. Defaults to
    /// `migration-report/` in the cargo target directory.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// First sample. Defaults to the start of the port.
    #[arg(long, default_value = DEFAULT_BASELINE)]
    pub baseline: String,
    /// Last sample.
    #[arg(long, default_value = "HEAD")]
    pub to: String,
}

/// One sample of the history, as written to `data.tsv`.
struct Row {
    date: Date,
    commit: String,
    c: u64,
    rust_core: u64,
    rust_ffi: u64,
    rust: u64,
    /// `rust / (rust + c)`, in percent.
    rust_share: f64,
    /// [`Row::rust_share`] with `c` frozen at its size at the baseline, so
    /// that C added since then does not hide the progress made.
    rust_share_vs_baseline: f64,
    c_tests: u64,
    rust_tests: u64,
    c_benchmarks: u64,
    rust_benchmarks: u64,
}

const COLUMNS: &[&str] = &[
    "date",
    "commit",
    "c",
    "rust_core",
    "rust_ffi",
    "rust",
    "rust_share",
    "rust_share_vs_baseline",
    "c_tests",
    "rust_tests",
    "c_benchmarks",
    "rust_benchmarks",
];

impl Row {
    fn new(stats: &Stats, baseline_c: u64) -> Result<Self> {
        let code = &stats.code;
        let rust = code.rust_core.lines + code.rust_ffi.lines;
        Ok(Self {
            date: stats.date.parse().map_err(anyhow::Error::msg)?,
            commit: stats.commit.clone(),
            c: code.c.lines,
            rust_core: code.rust_core.lines,
            rust_ffi: code.rust_ffi.lines,
            rust,
            rust_share: crate::percent(rust, rust + code.c.lines),
            rust_share_vs_baseline: crate::percent(rust, rust + baseline_c),
            c_tests: stats.tests.c,
            rust_tests: stats.tests.rust,
            c_benchmarks: stats.benchmarks.google_benchmark,
            rust_benchmarks: stats.benchmarks.criterion,
        })
    }

    /// The values, in [`COLUMNS`] order.
    fn values(&self) -> Vec<String> {
        vec![
            self.date.to_string(),
            self.commit[..10].to_owned(),
            self.c.to_string(),
            self.rust_core.to_string(),
            self.rust_ffi.to_string(),
            self.rust.to_string(),
            format!("{:.2}", self.rust_share),
            format!("{:.2}", self.rust_share_vs_baseline),
            self.c_tests.to_string(),
            self.rust_tests.to_string(),
            self.c_benchmarks.to_string(),
            self.rust_benchmarks.to_string(),
        ]
    }
}

/// A plotted line: its legend and the value it takes for each sample.
type Series = (&'static str, fn(&Row) -> f64);

struct Graph {
    title: &'static str,
    ylabel: &'static str,
    /// Plotted values, with their legend.
    series: &'static [Series],
}

const GRAPHS: &[Graph] = &[
    Graph {
        title: "Production code",
        ylabel: "Lines of code",
        series: &[
            ("C and C++", |r| r.c as f64),
            ("Rust (total)", |r| r.rust as f64),
            ("Rust (core)", |r| r.rust_core as f64),
            ("Rust (FFI and wrappers)", |r| r.rust_ffi as f64),
        ],
    },
    Graph {
        title: "Rust share of production code",
        ylabel: "Rust / (Rust + C and C++) (%)",
        series: &[
            ("Current C and C++", |r| r.rust_share),
            ("C and C++ frozen at baseline", |r| r.rust_share_vs_baseline),
        ],
    },
    Graph {
        title: "Tests",
        ylabel: "Test definitions",
        series: &[
            ("C and C++", |r| r.c_tests as f64),
            ("Rust", |r| r.rust_tests as f64),
        ],
    },
    Graph {
        title: "Benchmarks",
        ylabel: "Benchmark definitions",
        series: &[
            ("C and C++", |r| r.c_benchmarks as f64),
            ("Rust", |r| r.rust_benchmarks as f64),
        ],
    },
];

pub fn run(repo: &Path, options: &Options) -> Result<()> {
    let out = match &options.out {
        Some(out) => out.clone(),
        None => default_out_dir()?,
    };
    std::fs::create_dir_all(&out).with_context(|| format!("cannot create {}", out.display()))?;

    let samples = weekly_samples(repo, &options.baseline, &options.to)?;
    let stats = measure_all(repo, &samples)?;
    let baseline_c = stats[0].code.c.lines;
    let rows = stats
        .iter()
        .map(|s| Row::new(s, baseline_c))
        .collect::<Result<Vec<_>>>()?;

    write_tsv(&rows, &out.join("data.tsv"))?;
    let graphs = GRAPHS
        .iter()
        .map(|graph| plot(graph, &rows).with_context(|| format!("cannot plot {}", graph.title)))
        .collect::<Result<Vec<_>>>()?;
    let report = out.join("report.html");
    std::fs::write(&report, html(&rows, &graphs))?;
    println!("{}", report.display());
    Ok(())
}

/// `migration-report/` in the cargo target directory, found from this binary's
/// location: `<target>/<profile>/migration_stats`.
fn default_out_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("cannot locate the executable")?;
    let target = exe
        .parent()
        .and_then(Path::parent)
        .context("cannot locate the target directory; pass --out")?;
    Ok(target.join("migration-report"))
}

/// The baseline, the last first-parent commit before the end of each following
/// week, and the target.
fn weekly_samples(repo: &Path, baseline: &str, target: &str) -> Result<Vec<String>> {
    let history = git::first_parent_history(repo, baseline, target)?;
    let (Some((newest, end)), Some((oldest, start))) = (history.first(), history.last()) else {
        bail!("empty history");
    };

    let mut samples = vec![oldest.clone()];
    let mut cutoff = start + WEEK;
    while cutoff < *end {
        if let Some((id, _)) = history.iter().find(|(_, ts)| *ts < cutoff)
            && !samples.contains(id)
        {
            samples.push(id.clone());
        }
        cutoff += WEEK;
    }
    if !samples.contains(newest) {
        samples.push(newest.clone());
    }
    Ok(samples)
}

/// Measures every commit of `samples`, in parallel, keeping their order.
fn measure_all(repo: &Path, samples: &[String]) -> Result<Vec<Stats>> {
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<Result<Stats>>>> =
        Mutex::new(samples.iter().map(|_| None).collect());
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    let progress = std::io::stderr().is_terminal();

    std::thread::scope(|scope| {
        for _ in 0..workers.min(samples.len()) {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(rev) = samples.get(i) else { break };
                    let stats = measure(repo, rev);
                    results.lock().expect("poisoned")[i] = Some(stats);
                    let done = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if progress {
                        eprint!("\rmeasured {done}/{}", samples.len());
                    }
                }
            });
        }
    });
    if progress {
        eprintln!();
    }

    results
        .into_inner()
        .expect("poisoned")
        .into_iter()
        .zip(samples)
        .map(|(stats, rev)| {
            stats
                .expect("every sample is measured")
                .with_context(|| format!("failed to measure {rev}"))
        })
        .collect()
}

fn write_tsv(rows: &[Row], path: &Path) -> Result<()> {
    let mut tsv = COLUMNS.join("\t") + "\n";
    for row in rows {
        tsv += &(row.values().join("\t") + "\n");
    }
    std::fs::write(path, tsv).with_context(|| format!("cannot write {}", path.display()))
}

/// Series colors, distinguishable by colorblind readers (Okabe-Ito).
const COLORS: &[RGBColor] = &[
    RGBColor(213, 94, 0),
    RGBColor(0, 114, 178),
    RGBColor(0, 158, 115),
    RGBColor(230, 159, 0),
];

/// Draws `graph`, returning it as an SVG document.
fn plot(graph: &Graph, rows: &[Row]) -> Result<String> {
    let mut svg = String::new();
    draw(graph, rows, SVGBackend::with_string(&mut svg, (1200, 600)))?;
    Ok(svg)
}

fn draw(graph: &Graph, rows: &[Row], backend: SVGBackend) -> Result<()> {
    let root = backend.into_drawing_area();
    root.fill(&WHITE)?;

    let (first, last) = (rows[0].date, rows[rows.len() - 1].date);
    // Label month starts, at most about a dozen of them.
    let months = (last.0 - first.0) / 30;
    let step = u32::try_from(months / 12 + 1).unwrap_or(u32::MAX);
    let ticks = first.month_starts(last, step).iter().map(|d| d.0).collect();
    let y_max = rows
        .iter()
        .flat_map(|r| graph.series.iter().map(move |(_, value)| value(r)))
        .fold(0.0, f64::max);
    let mut chart = ChartBuilder::on(&root)
        .caption(graph.title, ("sans-serif", 24))
        .margin(20)
        .x_label_area_size(40)
        .y_label_area_size(70)
        .build_cartesian_2d(
            (first.0..last.0).with_key_points(ticks),
            0.0..(y_max * 1.05).max(1.0),
        )?;
    chart
        .configure_mesh()
        .y_desc(graph.ylabel)
        .x_label_formatter(&|day| {
            let (year, month, _) = Date(*day).ymd();
            format!("{year}-{month:02}")
        })
        .light_line_style(WHITE.mix(0.0))
        .draw()?;

    for ((legend, value), &color) in graph.series.iter().zip(COLORS.iter().cycle()) {
        let style = color.stroke_width(2);
        chart
            .draw_series(LineSeries::new(
                rows.iter().map(|r| (r.date.0, value(r))),
                style,
            ))?
            .label(*legend)
            .legend(move |(x, y)| PathElement::new([(x, y), (x + 20, y)], style));
    }
    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.9))
        .border_style(BLACK.mix(0.3))
        .draw()?;
    root.present()?;
    Ok(())
}

/// Where the commits of the repository can be browsed.
const COMMIT_URL: &str = "https://github.com/RediSearch/RediSearch/commit/";

/// `commit`, abbreviated, linking to its page on [`COMMIT_URL`].
fn commit_link(commit: &str) -> String {
    format!(
        r#"<a href="{COMMIT_URL}{commit}"><code>{}</code></a>"#,
        &commit[..10]
    )
}

/// A standalone HTML page, with the graphs, in [`GRAPHS`] order, inlined.
fn html(rows: &[Row], graphs: &[String]) -> String {
    let (first, last) = (&rows[0], &rows[rows.len() - 1]);
    let delta = |f: fn(&Row) -> u64| {
        let (a, b) = (f(first), f(last));
        format!("{a} → {b} ({:+})", b as i64 - a as i64)
    };
    let summary = [
        ("C and C++ lines".to_owned(), delta(|r| r.c)),
        ("Rust lines".to_owned(), delta(|r| r.rust)),
        (
            "Rust share".to_owned(),
            format!("{:.1}% → {:.1}%", first.rust_share, last.rust_share),
        ),
        (
            format!("Rust share vs baseline C and C++ ({} lines)", first.c),
            format!(
                "{:.1}% → {:.1}%",
                first.rust_share_vs_baseline, last.rust_share_vs_baseline
            ),
        ),
        ("C and C++ tests".to_owned(), delta(|r| r.c_tests)),
        ("Rust tests".to_owned(), delta(|r| r.rust_tests)),
        ("C and C++ benchmarks".to_owned(), delta(|r| r.c_benchmarks)),
        ("Rust benchmarks".to_owned(), delta(|r| r.rust_benchmarks)),
    ];

    let mut html = String::new();
    let _ = write!(
        html,
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>C to Rust migration report</title>
<style>
  body {{ font-family: sans-serif; max-width: 1240px; margin: 2em auto; padding: 0 1em; color: #222; }}
  table {{ border-collapse: collapse; }}
  th, td {{ border: 1px solid #ccc; padding: 0.3em 0.8em; text-align: left; }}
  td:last-child {{ font-variant-numeric: tabular-nums; }}
  svg {{ width: 100%; height: auto; }}
  code {{ background: #f3f3f3; padding: 0 0.2em; }}
</style>
</head>
<body>
<h1>C to Rust migration report</h1>
<p>From baseline {} ({}) to {} ({}), sampled weekly ({} samples).</p>
<h2>Summary</h2>
<table>
<tr><th>Metric</th><th>Evolution</th></tr>
"#,
        commit_link(&first.commit),
        first.date,
        commit_link(&last.commit),
        last.date,
        rows.len()
    );
    for (metric, evolution) in &summary {
        let _ = writeln!(html, "<tr><td>{metric}</td><td>{evolution}</td></tr>");
    }
    let _ = writeln!(
        html,
        "</table>\n<p>The Rust share is <code>Rust / (Rust + C and C++)</code>. Its \
         baseline variant keeps C and C++ at its size at the baseline, so that C added \
         since then does not hide the progress made.</p>"
    );
    for (graph, svg) in GRAPHS.iter().zip(graphs) {
        let _ = writeln!(html, "<h2>{}</h2>\n{svg}", graph.title);
    }
    let _ = writeln!(html, "</body>\n</html>");
    html
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_values_match_columns() {
        let row = Row {
            date: Date(0),
            commit: "0123456789abcdef".to_owned(),
            c: 0,
            rust_core: 0,
            rust_ffi: 0,
            rust: 0,
            rust_share: 0.0,
            rust_share_vs_baseline: 0.0,
            c_tests: 0,
            rust_tests: 0,
            c_benchmarks: 0,
            rust_benchmarks: 0,
        };
        assert_eq!(row.values().len(), COLUMNS.len());
    }
}
