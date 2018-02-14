## SQL Query Support

Search indexes can be queried via SQL using the `FT.SQL` command.

## Building

* If your system contains a new enough sqlite, then no further steps are needed.
* If you have a newer version of sqlite installed in a non-default location, use
the `SQLITE_ROOT` Makefile variable to point to it. The standard layout of `/lib`
and `/include` are expected.
* If you do not have a sufficiently new version of sqlite, you can use the embedded
    version that comes in this branch. It will however add a minute or two to the
    build time. To use it, supply it as the `SQLITE_AMALGAMATED_DIR` variable
    to `make`, e.g.

    ```
    $ make SQLITE_AMALGAMATED_DIR=$PWD/src/dep/sqlite -j20
    ```

    Note that as in the example, the value should be the absolute path

As this feature stabilizes, a better build experience will come along.

## Command

```
FT.SQL {STATEMENT} [PARAMS...]
```

Execute an SQL statement in RediSearch. The statement can have placeholders (i.e.
`?` or anything else supported by SQLite) in which case `PARAMS` are the arguments
to the parameters.

You may use FT indexes as table names and FT fields as columns. Note that only
`TEXT` and `NUMERIC` fields are supported. `GEO` and `TAG` fields are not supported.

The `__RSID__` special column refers to the document key.

## Output Format

If there are no results, the output is an empty array. Otherwise the output is
an array consisting of a *header row* and one or more *data rows*

The header row contains metadata about data in the result set:

```
127.0.0.1:7777> FT.SQL "select * from gh limit 10"
 1)  1) __RSID__
     2) $
     3) type
     4) $
     5) actor
     6) $
     7) repo
     8) $
     9) date
    10) f
    11) message
    12) $
```

It consists of two-element sequences, the first being the name of the row and the
second being the type code for the return type. The type code can be either:

* `$` for string
* `f` for float
* `i` for integer

Note that `i` can be returned for SQLite functions or conversions, though native
FT numeric fields are always `f`.

Each row in the result set is an array of `N/2` elements, where `N` is the length
of the header row.

```
 2) 1) "7044401087"
    2) "forkevent"
    3) "adieuadieu"
    4) "loysoft/node-oauth2-server"
    5) "1514782800"
    6) ""
```