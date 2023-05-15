---
syntax: |
  FT.SUGGET key prefix 
    [FUZZY] 
    [WITHSCORES] 
    [WITHPAYLOADS] 
    [MAX max]
---

Get completion suggestions for a prefix

## Syntax

{{< highlight bash >}}

{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

is suggestion dictionary key.
</details>

<details open>
<summary><code>prefix</code></summary>

is prefix to complete on.
</details>

## Optional arguments

<details open>
<summary><code>FUZZY</code></summary> 

performs a fuzzy prefix search, including prefixes at Levenshtein distance of 1 from the prefix sent.
</details>

<details open>
<summary><code>MAX num</code></summary> 

limits the results to a maximum of `num` (default: 5).
</details>

<details open>
<summary><code>WITHSCORES</code></summary> 

also returns the score of each suggestion. This can be used to merge results from multiple instances.
</details>

<details open>
<summary><code>WITHPAYLOADS</code></summary> 

returns optional payloads saved along with the suggestions. If no payload is present for an entry, it returns a null reply.
</details>

## Return

FT.SUGGET returns an array reply, which is a list of the top suggestions matching the prefix, optionally with score after each entry.

## Examples

<details open>
<summary><b>Get completion suggestions for a prefix</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.SUGGET sug hell FUZZY MAX 3 WITHSCORES
1) "hell"
2) "2147483648"
3) "hello"
4) "0.70710676908493042"
{{< / highlight >}}
</details>

## See also

`FT.SUGADD` | `FT.SUGDEL` | `FT.SUGLEN` 

## Related topics

[RediSearch](/docs/stack/search)
