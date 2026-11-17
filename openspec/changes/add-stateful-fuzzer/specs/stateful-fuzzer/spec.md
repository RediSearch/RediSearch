# stateful-fuzzer (delta)

On merge, the requirements below are folded into
`openspec/specs/stateful-fuzzer/spec.md`.

## ADDED Requirements

### Requirement: Hermetic execution
The fuzzer SHALL run every program against a newly started `redis-server` and
SHALL NOT reuse a server for a second program.

#### Scenario: Replay of a saved program
- **WHEN** a program file is passed to the `run` command
- **THEN** a new server SHALL be started for that run
- **AND** the server SHALL be stopped and its process group reaped before the command returns

### Requirement: One execution path
The `fuzz`, `run`, and `reduce` commands SHALL execute and judge programs with
the same code.

#### Scenario: A finding is replayed
- **WHEN** the campaign records a finding with signature `S` for a program
- **AND** that program is passed to the `run` command against the same builds
- **THEN** the reported finding SHALL carry signature `S`, subject to the reproduction rate recorded in the finding's manifest

### Requirement: Programs are stable under deletion
Deleting an operation from a program SHALL NOT change the commands sent by any
remaining operation.

#### Scenario: Deleting an index creation
- **WHEN** the operation that creates index `i1` is deleted from a program
- **THEN** each remaining operation SHALL send the same commands as before
- **AND** operations that name `i1` SHALL receive an ordinary server error

### Requirement: Byte-exact programs
The program format SHALL represent arbitrary byte strings and every `f32` and
`f64` bit pattern, and SHALL carry a format version.

#### Scenario: Invalid UTF-8 round-trips
- **WHEN** a program containing an argument that is not valid UTF-8 is written and read back
- **THEN** the argument SHALL be byte-for-byte identical

### Requirement: Harness-held controls are not findings
A program that leaves a scheduling control held SHALL NOT be reported as a hang
if the server responds after the harness releases its controls.

#### Scenario: Workers left paused
- **WHEN** a program pauses the worker pool, issues a query, and never resumes
- **AND** the query replies after the harness sends its release list
- **THEN** the run SHALL be recorded as stalled
- **AND** no finding SHALL be reported for it

### Requirement: Sanitizer reports from child processes are detected
A sanitizer report written by any process in the server's process group SHALL be
reported as a finding.

#### Scenario: Report in the fork-GC child
- **WHEN** the fork-GC child writes a sanitizer report and the parent keeps running
- **THEN** the run SHALL report a finding whose signature is derived from that report

### Requirement: Logic rules are conservative
A logic rule SHALL be skipped when its preconditions are not confirmed by the
server's replies.

#### Scenario: Query timed out
- **WHEN** one of the queries compared by a logic rule returned a timeout error
- **THEN** that rule instance SHALL be skipped and no finding reported

#### Scenario: Inconclusive barrier
- **WHEN** a barrier between two compared queries timed out
- **THEN** that rule instance SHALL be skipped and no finding reported

### Requirement: Reduction preserves the finding
The `reduce` command SHALL only accept a smaller program that still produces the
original signature, and SHALL record how often the result reproduces.

#### Scenario: A different bug appears during reduction
- **WHEN** a smaller candidate fails with a signature other than the original
- **THEN** the candidate SHALL be rejected

### Requirement: Coverage instrumentation is opt-in
Coverage instrumentation and its shim SHALL be present only in builds made with
`FUZZ_COV=1`.

#### Scenario: Default build
- **WHEN** the module is built without `FUZZ_COV`
- **THEN** the module SHALL contain no coverage shim and no coverage instrumentation
