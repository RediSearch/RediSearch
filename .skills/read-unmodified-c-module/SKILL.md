---
name: read-unmodified-c-module
description: Read the source of the C module we are working on, before we made any changes.
---

# Read Unmodified C Module

Read the source of the C module(s) we are working on, as they were before we made any changes.

## Arguments
- `<module path>`: Module name to read (e.g., `src/numeric_range_tree` or `src/aggregate/aggregate_debug`), without extension
- `<module path 1> <module path 2>`: Multiple module names to read

If the path doesn't include `src/`, assume it to be in the `src` directory. E.g. `numeric_range_tree` becomes `src/numeric_range_tree`.

## Instructions

For each module path:

```bash
# Read header file
git show master:<module path>.h
# Read implementation file
git show master:<module path>.c
```
