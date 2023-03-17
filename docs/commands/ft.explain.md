---
syntax: |
  FT.EXPLAIN index query 
    [DIALECT dialect]
---

Return the execution plan for a complex query

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is index name. You must first create the index using `FT.CREATE`.
</details>

<details open>
<summary><code>query</code></summary>

is query string, as if sent to FT.SEARCH`.
</details>

## Optional arguments

<details open>
<summary><code>DIALECT {dialect_version}</code></summary>

is dialect version under which to execute the query. If not specified, the query executes under the default dialect version set during module initial loading or via `FT.CONFIG SET` command.
</details>

{{% alert title="Notes" color="warning" %}}
 
- In the returned response, a `+` on a term is an indication of stemming.
- Use `redis-cli --raw` to properly read line-breaks in the returned response.

{{% /alert %}}

## Return

FT.EXPLAIN returns a string representing the execution plan.

## Examples

<details open>
<summary><b>Return the execution plan for a complex query</b></summary>

{{< highlight bash >}}
$ redis-cli --raw

127.0.0.1:6379> FT.EXPLAIN rd "(foo bar)|(hello world) @date:[100 200]|@date:[500 +inf]"
INTERSECT {
  UNION {
    INTERSECT {
      foo
      bar
    }
    INTERSECT {
      hello
      world
    }
  }
  UNION {
    NUMERIC {100.000000 <= x <= 200.000000}
    NUMERIC {500.000000 <= x <= inf}
  }
}
{{< / highlight >}}
</details>

## See also

`FT.CREATE` | `FT.SEARCH` | `FT.CONFIG SET`

## Related topics

[RediSearch](/docs/stack/search)

