# Rust Code Reviewer Bot

The Rust Code Reviewer Bot automatically reviews Rust code changes in pull requests using OpenAI's GPT-4. It can be triggered via comments and supports custom review topics.

## Usage

### Basic Review

To trigger a basic code review, comment on a pull request:

```
/rust_review
```

This will review all Rust files changed in the PR (and related C/C++ files for context) using default criteria:
- Code quality and best practices
- Potential bugs or edge cases
- Performance considerations
- Readability and maintainability
- Proper error handling
- Memory safety concerns
- FFI safety and correctness
- Unsafe code validation

### Custom Review Topics

You can specify additional topics for the reviewer to focus on:

```
/rust_review focus on: performance, memory safety
```

```
/rust_review consider: error handling, testing, documentation
```

```
/rust_review check: async, concurrency, lifetimes
```

### Supported Topics

The following topics are supported (case-insensitive):

**Core Rust Concepts:**
- `performance`, `memory`, `safety`
- `lifetimes`, `borrowing`, `ownership`
- `traits`, `generics`, `macros`
- `types`, `async`, `concurrency`, `threading`
- `unsafe`, `ffi`, `c integration`, `bindings`, `extern`, `interop`

**Development Practices:**
- `error handling`, `testing`, `documentation`
- `api design`, `architecture`, `patterns`
- `best practices`, `conventions`, `style`
- `readability`, `maintainability`, `refactoring`

**Tools & Ecosystem:**
- `cargo`, `clippy`, `rustfmt`
- `modules`, `crates`, `dependencies`

**General:**
- `algorithms`, `data structures`, `optimization`
- `security`, `validation`, `logging`, `debugging`
- `formatting`, `naming`

## Security Features

### Permission Validation
- **Write Access Required**: Only users with write access to the repository can trigger reviews
- **GitHub API Validation**: Uses GitHub's official API to verify user permissions
- **Graceful Denial**: Users without permissions receive clear feedback without exposing permission levels


## Examples

### Performance Review
```
/rust_review focus on: performance, optimization
```

### Security Review
```
/rust_review consider: security, validation, error handling
```

### Code Quality Review
```
/rust_review check: best practices, readability, maintainability
```

### Async Code Review
```
/rust_review focus on: async, concurrency, error handling
```

### FFI/C Integration Review
```
/rust_review consider: ffi, c integration, unsafe, memory safety
```

### Bindings Review
```
/rust_review check: bindings, extern, interop, safety
```

## Limitations

- Maximum 5 custom topics per review
- Topics must contain recognized keywords
- Only triggers on pull requests with Rust file changes (but includes C/C++ files for context)
- Requires OpenAI API access
- Only users with write access can trigger reviews

## Workflow Files

- `.github/workflows/event-comment-rust-review.yml` - Handles comment triggers and permission validation
- `.github/workflows/task-rust-code-review.yml` - Executes the actual code review
- `.github/scripts/rust_code_review.py` - Python script that generates reviews using OpenAI
