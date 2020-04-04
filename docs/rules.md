## Automatic Indexing of Redis Hashes

Starting in version 2.0, RediSearch allows a follow-hash index mode,
in which simple Redis hashes which match a rule can be indexed
automatically, eliminating the need for an explicit `FT.ADD` command.

In order to enable this feature, an index needs to be declared using the
`WITHRULES` argument to `FT.CREATE`, e.g.

```
FT.CREATE idx ... WITHRULES ... SCHEMA ...
```

Once the index has been created, you must define criteria which a hash must
match in order to be indexed. This can be as simple as a key prefix, or a
complex expression. Whenever any hash is modified, RediSearch will check
whether or not the hash matches any of the specified rules. If the hash
matches a rule, it will be (re)indexed accordingly.

## FT.RULEADD

The `FT.RULEADD` command can be used to add a single rule. Each rule
consists of a name, index, match specifier, and action. The name of the
rule exists simply to provide a friendly name, in case you wish to
delete or modify it at a later point.

```
FT.RULEADD {INDEX} {NAME} PREFIX {prefix}
FT.RULEADD {INDEX} {NAME} HASFIELD {fieldname}
FT.RULEADD {INDEX} {NAME} EXPR {expression}
FT.RULEADD {INDEX} ... [ABORT|GOTO]
FT.RULEADD {INDEX} ... SETATTR [LANGUAGE {language}] [SCORE {score}]
FT.RULEADD {INDEX} ... LOADATTR [LANGUAGE {lang-field}] [SCORE {score-field}]
```

The above prototype shows the variations of the `FT.RULEADD` command.
The first example shows how to add a prefix rule using the `PREFIX`
match specifier. If a given hash matches the given (case-insensitive) prefix
then the hash is indexed. For example, to add all hashes with the prefix
`user:` to the index `users`:

```
FT.RULEADD users myRule PREFIX user:
```

Using prefix evaluation is the most efficient as only the key name is evaluated

The second form checks for the presence of a given field within the hash
(the contents are not inspected). For example, to index all documents with a
`user_id` field:

```
FT.RULEADD users withId HASFIELD user_id
```

The third form allows you to use a complex expression. This expression
can reference fields within the document using the `@field` notation. For
example, to index all people who were born after 1985:

```
FT.RULEADD people myRule EXPR "@birth_year>1985"
```

The `EXPR` match type also allows you to apply boolean logic to prefix and
field-presence expressions, using special built-in functions:

* `PREFIX foobar` can be expressed as `hasprefix("foobar")`
* `HASFIELD foobar` can be expressed as `hasfield("foobar")`

For example, to index hashes which have a prefix of `user:` *and*
have a `user_id` field, *and* have a `birth_year` greater than 1985:

```
FT.RULEADD users myRule EXPR 'hasfield("user_id") && hasprefix("user:") && @birth_year>1985'
```

### Actions

The default action of a rule is to declare that a matching hash should be added
to the index. It is possible to provide additional action specifiers which
can direct RediSearch to ignore the document, or to change the way the
hash is indexed by modifying its attributes. You can even jump between rules

#### ABORT

The `ABORT` action indicates that RediSearch should not continue processing
the given hash, regardless of whether it may or may not match additional
rules. By default, rules are evaluated in the order they are added via
`FT.RULEADD`. The rule processing does not stop when a match is found, as
it is possible to allow multiple indexes to contain a given hash. The
`ABORT` action can be used to either ignore a hash entirely, or prevent
subsequent rules from evaluating it.

#### GOTO

The `GOTO` action can skip processing to the given named rule. This can be
used to fast-track processing towards more specific rules.

#### SETATTR

The `SETATTR` action takes attribute-value pairs for its arguments; for example,
to set the language to french for all documents which have a prefix `user:fr:`,
one can do:

```
FT.RULEADD ... PREFIX 'user:fr:' SETATTR LANGUAGE french
```

Supported attributes are `LANGUAGE` and `SCORE`

#### LOADATTR

The `LOADATTR` action takes attribute-field pair. Unlike `SETATTR` which must
statically assign a language or score based on a rule, `LOADATTR` takes the
name of the hash field from which the language and score will be read.

```
FT.RULEADD ... LOADATTR LANGUAGE lang SCORE score
```

The above example will read the language from the `language` field and the
score from the `SCORE` field.

## Applying rules to existing hashes

Whenever a new rule is added via `RULEADD`, RediSearch will initiate a
background scan of all hashes to locate and index any hashes which match
the new rule. You may continue searching your index at this time, but be
aware that not all potential matches may be included until the scan is complete.

The `FT.INFO sync` subcommand will return an array of two items; the first is
a string status and the second is the number of items. If the status string
is not `SYNCED` then it means the background scan is still in effect.

You can poll `FT.INFO` until the status changes to `SYNCED`.

## Asynchronous Indexing

An index may be declared using the `ASYNC` keyword, e.g.

```
FT.CREATE idx ASYNC WITHRULES ....
```

In which case, indexing is performed asynchronously