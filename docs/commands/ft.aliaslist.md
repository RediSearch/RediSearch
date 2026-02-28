---
acl_categories:
- '@search'
arguments:
- name: index
  type: string
categories:
- docs
- develop
- stack
- oss
- rs
- rc
- oss
- kubernetes
- clients
command_flags:
- readonly
complexity: O(1)
description: Returns all aliases associated with an index
group: search
hidden: false
linkTitle: FT.ALIASLIST
module: Search
since: 2.10.0
stack_path: docs/interact/search-and-query
summary: Returns all aliases associated with an index
syntax_fmt: FT.ALIASLIST index
title: FT.ALIASLIST
---

Returns all aliases associated with an index.

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is the name of the index whose aliases you want to retrieve.
</details>

## Examples

<details open>
<summary><b>List aliases for an index</b></summary>

Create an index and add some aliases.

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT
OK
127.0.0.1:6379> FT.ALIASADD alias1 idx
OK
127.0.0.1:6379> FT.ALIASADD alias2 idx
OK
{{< / highlight >}}

List all aliases for the index.

{{< highlight bash >}}
127.0.0.1:6379> FT.ALIASLIST idx
1) "alias1"
2) "alias2"
{{< / highlight >}}

If the index has no aliases, an empty array is returned.

{{< highlight bash >}}
127.0.0.1:6379> FT.CREATE newidx ON HASH PREFIX 1 new: SCHEMA title TEXT
OK
127.0.0.1:6379> FT.ALIASLIST newidx
(empty array)
{{< / highlight >}}
</details>

## Redis Software and Redis Cloud compatibility

| Redis<br />Software | Redis Cloud<br />Flexible & Annual | Redis Cloud<br />Free & Fixed | <span style="min-width: 9em; display: table-cell">Notes</span> |
|:----------------------|:-----------------|:-----------------|:------|
| <span title="Supported">&#x2705; Supported</span> | <span title="Supported">&#x2705; Supported</span> | <span title="Supported">&#x2705; Supported</nobr></span> |  |

## Return information

{{< multitabs id="ft-aliaslist-return-info" 
    tab1="RESP2" 
    tab2="RESP3" >}}

One of the following:
* [Array]({{< relref "/develop/reference/protocol-spec#arrays" >}}) of alias names as [bulk strings]({{< relref "/develop/reference/protocol-spec#bulk-strings" >}}).
* [Simple error reply]({{< relref "/develop/reference/protocol-spec#simple-errors" >}}) if the index does not exist.

-tab-sep-

One of the following:
* [Set]({{< relref "/develop/reference/protocol-spec#sets" >}}) of alias names as [bulk strings]({{< relref "/develop/reference/protocol-spec#bulk-strings" >}}).
* [Simple error reply]({{< relref "/develop/reference/protocol-spec#simple-errors" >}}) if the index does not exist.

{{< /multitabs >}}

## See also

[`FT.ALIASADD`]({{< relref "commands/ft.aliasadd/" >}}) | [`FT.ALIASDEL`]({{< relref "commands/ft.aliasdel/" >}}) | [`FT.ALIASUPDATE`]({{< relref "commands/ft.aliasupdate/" >}})

