---
syntax: |
  FT.CONFIG HELP option
---

Describe configuration options

[Examples](#examples)

## Required arguments

<details open>
<summary><code>option</code></summary> 

is name of the configuration option, or '*' for all. 
</details>

## Return

FT.CONFIG HELP returns an array reply of the configuration name and value.

## Examples

<details open>
<summary><b>Get configuration details</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.CONFIG HELP TIMEOUT
1) 1) TIMEOUT
   2) Description
   3) Query (search) timeout
   4) Value
   5) "42"
{{< / highlight >}}
</details>

## See also

`FT.CONFIG SET` | `FT.CONFIG GET` 

## Related topics

[RediSearch](/docs/stack/search)