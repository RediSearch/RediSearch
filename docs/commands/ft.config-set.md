---
syntax: 
---

Describe configuration options

## Syntax

{{< highlight bash >}}
FT.CONFIG SET option value
{{< / highlight >}}

[Examples](#examples)

## Required parameters

<details open>
<summary><code>option</code></summary> 

is name of the configuration option, or '*' for all. 
</details>

<details open>
<summary><code>value</code></summary> 

is value of the configuration option. 
</details>

## Return

FT.CONFIG SET returns a simple string reply `OK` if executed correctly, or an error reply otherwise.

## Examples

<details open>
<summary><b>Set runtime configuration options</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.CONFIG SET TIMEOUT 42
OK
{{< / highlight >}}
</details>

## See also

`FT.CONFIG GET` | `FT.CONFIG HELP` 

## Related topics

[RediSearch](/docs/stack/search)