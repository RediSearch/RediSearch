# Select FT.INFO rows

## Why
Callers checking a few index observations currently receive the entire schema and statistics reply. See https://github.com/RediSearch/RediSearch/issues/12337.

## What changes
Add mutually exclusive `PREFIX <prefix>` and `FIELDS <field> [field ...]` options to select complete top-level rows. Plain `FT.INFO <index>` keeps its current reply.
