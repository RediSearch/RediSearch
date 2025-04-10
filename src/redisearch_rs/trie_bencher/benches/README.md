# Rust Benchmarks in CI

The CI for benchmarks shall perform the following steps:

1. Get baseline from master branch
2. Run benchmarks on current branch
3. If there is a regression add an approval to the PR at github

check if the rust code have been touched.
Save as artifact that can be download.

## Makefile

Criterion can be invoked in the [Makefile](./../../../../Makefile) with target name `crit-bench`:


## Challenge Criterion integration with CI

There is no exit code of criterion that idenitfies a regression, see [Issue 824](https://github.com/bheisler/criterion.rs/issues/824)

Criterion.rs is the crate for defining benchmarks. It outputs all data in json or csv and generates HTML reports.

An extension to cargo that replaces the `cargo bench` command with `cargo criterion`.
The run looks cleaner and is faster than with plain criterion.rs. There is also no exit code used, in respect to main.rs
It has less changes than criterion.rs - last change was 9 month ago. But it builds on top of Criterion.rs.
Internally Cargo bench is called and communication happens via a network interface.


We have these possible actions:

1. Fork Criterion.rs or Cargo Criterion and adapt the Code to support an exit code.
2. Rely on the output of Criterion and calculate a regression by ourselves.


#### Advantages

- We could simply return a different exit code for 

#### Disadvantages

### 3. Cargo-Criterion



#### Advantages

- Better integration in compiler

#### Disadvantages


