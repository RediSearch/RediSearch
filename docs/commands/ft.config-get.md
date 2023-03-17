---
syntax: |
  FT.CONFIG GET option
---

Retrieve configuration options

[Examples](#examples)

## Required arguments

<details open>
<summary><code>option</code></summary> 

is name of the configuration option, or '*' for all. 
</details>

## Return

FT.CONFIG GET returns an array reply of the configuration name and value.

## Examples

<details open>
<summary><b>Retrieve configuration options</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.CONFIG GET TIMEOUT
1) 1) TIMEOUT
   2) 42
{{< / highlight >}}

{{< highlight bash >}}
127.0.0.1:6379> FT.CONFIG GET *
 1) 1) EXTLOAD
    2) (nil)
 2) 1) SAFEMODE
    2) true
 3) 1) CONCURRENT_WRITE_MODE
    2) false
 4) 1) NOGC
    2) false
 5) 1) MINPREFIX
    2) 2
 6) 1) FORKGC_SLEEP_BEFORE_EXIT
    2) 0
 7) 1) MAXDOCTABLESIZE
    2) 1000000
 8) 1) MAXSEARCHRESULTS
    2) 1000000
 9) 1) MAXAGGREGATERESULTS
    2) unlimited
10) 1) MAXEXPANSIONS
    2) 200
11) 1) MAXPREFIXEXPANSIONS
    2) 200
12) 1) TIMEOUT
    2) 42
13) 1) INDEX_THREADS
    2) 8
14) 1) SEARCH_THREADS
    2) 20
15) 1) FRISOINI
    2) (nil)
16) 1) ON_TIMEOUT
    2) return
17) 1) GCSCANSIZE
    2) 100
18) 1) MIN_PHONETIC_TERM_LEN
    2) 3
19) 1) GC_POLICY
    2) fork
20) 1) FORK_GC_RUN_INTERVAL
    2) 30
21) 1) FORK_GC_CLEAN_THRESHOLD
    2) 100
22) 1) FORK_GC_RETRY_INTERVAL
    2) 5
23) 1) FORK_GC_CLEAN_NUMERIC_EMPTY_NODES
    2) true
24) 1) _FORK_GC_CLEAN_NUMERIC_EMPTY_NODES
    2) true
25) 1) _MAX_RESULTS_TO_UNSORTED_MODE
    2) 1000
26) 1) UNION_ITERATOR_HEAP
    2) 20
27) 1) CURSOR_MAX_IDLE
    2) 300000
28) 1) NO_MEM_POOLS
    2) false
29) 1) PARTIAL_INDEXED_DOCS
    2) false
30) 1) UPGRADE_INDEX
    2) Upgrade config for upgrading
31) 1) _NUMERIC_COMPRESS
    2) false
32) 1) _FREE_RESOURCE_ON_THREAD
    2) true
33) 1) _PRINT_PROFILE_CLOCK
    2) true
34) 1) RAW_DOCID_ENCODING
    2) false
35) 1) _NUMERIC_RANGES_PARENTS
    2) 0
{{< / highlight >}}
</details>

## See also

`FT.CONFIG SET` | `FT.CONFIG HELP` 

## Related topics

[RediSearch](/docs/stack/search)