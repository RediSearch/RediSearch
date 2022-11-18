---
syntax: |
  FT.EXPLAINCLI index query 
    [DIALECT dialect]
---

Return the execution plan for a complex query but formatted for easier reading without using `redis-cli --raw`

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

{{% alert title="Note" color="warning" %}}
 
In the returned response, a `+` on a term is an indication of stemming.

{{% /alert %}}

</details>

## Return

FT.EXPLAINCLI returns an array reply with a string representing the execution plan.

## Examples

<details open>
<summary><b>Return the execution plan for a complex query</b></summary>

{{< highlight bash >}}
$ redis-cli

127.0.0.1:6379> FT.EXPLAINCLI rd "(foo bar)|(hello world) @date:[100 200]|@date:[500 +inf]"
 1) INTERSECT {
 2)   UNION {
 3)     INTERSECT {
 4)       UNION {
 5)         foo
 6)         +foo(expanded)
 7)       }
 8)       UNION {
 9)         bar
10)         +bar(expanded)
11)       }
12)     }
13)     INTERSECT {
14)       UNION {
15)         hello
16)         +hello(expanded)
17)       }
18)       UNION {
19)         world
20)         +world(expanded)
21)       }
22)     }
23)   }
24)   UNION {
25)     NUMERIC {100.000000 <= @date <= 200.000000}
26)     NUMERIC {500.000000 <= @date <= inf}
27)   }
28) }
29)
{{< / highlight >}}
</details>

## See also

`FT.CREATE` | `FT.SEARCH` | `FT.CONFIG SET`

## Related topics

[RediSearch](/docs/stack/search)

