---
syntax: |
  FT.SUGADD key string score 
    [INCR] 
    [PAYLOAD payload]
---

Add a suggestion string to an auto-complete suggestion dictionary

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

is suggestion dictionary key.
</details>

<details open>
<summary><code>string</code></summary> 

is suggestion string to index.
</details>

<details open>
<summary><code>score</code></summary> 

is floating point number of the suggestion string's weight.
</details>

The auto-complete suggestion dictionary is disconnected from the index definitions and leaves creating and updating suggestions dictionaries to the user.

## Optional arguments

<details open>
<summary><code>INCR</code></summary> 

increments the existing entry of the suggestion by the given score, instead of replacing the score. This is useful for updating the dictionary based on user queries in real time.
</details>

<details open>
<summary><code>PAYLOAD {payload}</code></summary> 

saves an extra payload with the suggestion, that can be fetched by adding the `WITHPAYLOADS` argument to `FT.SUGGET`.
</details>

## Return

FT.SUGADD returns an integer reply, which is the current size of the suggestion dictionary.

## Examples

<details open>
<summary><b>Add a suggestion string to an auto-complete suggestion dictionary</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.SUGADD sug "hello world" 1
(integer) 3
{{< / highlight >}}
</details>

## See also

`FT.SUGGET` | `FT.SUGDEL` | `FT.SUGLEN` 

## Related topics

[RediSearch](/docs/stack/search)