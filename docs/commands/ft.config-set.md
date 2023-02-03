---
syntax: |
  FT.CONFIG SET option value
---

Set the value of a RediSearch configuration parameter.

Values set using `FT.CONFIG SET` are not persisted after server restart.

RediSearch configuration parameters are detailed in [Configuration parameters](/docs/stack/search/configuring).

{{% alert title="Note" color="warning" %}}
As detailed in the link above, not all RediSearch configuration parameters can be set at runtime.
{{% /alert %}}

[Examples](#examples)

## Required arguments

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
