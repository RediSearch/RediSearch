---
syntax: |
  FT.INFO index
---

Return information and statistics on the index

[Examples](#examples)

## Required arguments

<details open>
<summary><code>index</code></summary>

is full-text index name. You must first create the index using `FT.CREATE`.
</details>

## Return

FT.INFO returns an array reply with pairs of keys and values.

Returned values include:

- `index_definition`: reflection of `FT.CREATE` command parameters.
- `fields`: index schema - field names, types, and attributes.
- Number of documents.
- Number of distinct terms.
- Average bytes per record.
- Size and capacity of the index buffers.
- Indexing state and percentage as well as failures:
  - `indexing`: whether of not the index is being scanned in the background.
  - `percent_indexed`: progress of background indexing (1 if complete).
  - `hash_indexing_failures`: number of failures due to operations not compatible with index schema.

Optional statistics include:

* `garbage collector` for all options other than NOGC.
* `cursors` if a cursor exists for the index.
* `stopword lists` if a custom stopword list is used.

## Examples

<details open>
<summary><b>Return statistics about an index</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> FT.INFO idx
1) index_name
 2) wikipedia
 3) index_options
 4) (empty array)
    11) score_field
    12) __score
    13) payload_field
    14) __payload
 7) fields
 8) 1) 1) title
       2) type
       3) TEXT
       4) WEIGHT
       5) "1"
       6) SORTABLE
    2) 1) body
       2) type
       3) TEXT
       4) WEIGHT
       5) "1"
    3) 1) id
       2) type
       3) NUMERIC
    4) 1) subject location
       2) type
       3) GEO
 9) num_docs
10) "0"
11) max_doc_id
12) "345678"
13) num_terms
14) "691356"
15) num_records
16) "0"
17) inverted_sz_mb
18) "0"
19) vector_index_sz_mb
20) "0"
21) total_inverted_index_blocks
22) "933290"
23) offset_vectors_sz_mb
24) "0.65932846069335938"
25) doc_table_size_mb
26) "29.893482208251953"
27) sortable_values_size_mb
28) "11.432285308837891"
29) key_table_size_mb
30) "1.239776611328125e-05"
31) records_per_doc_avg
32) "-nan"
33) bytes_per_record_avg
34) "-nan"
35) offsets_per_term_avg
36) "inf"
37) offset_bits_per_record_avg
38) "8"
39) hash_indexing_failures
40) "0"
41) indexing
42) "0"
43) percent_indexed
44) "1"
45) number_of_uses
46) 1
47) gc_stats
48)  1) bytes_collected
     2) "4148136"
     3) total_ms_run
     4) "14796"
     5) total_cycles
     6) "1"
     7) average_cycle_time_ms
     8) "14796"
     9) last_run_time_ms
    10) "14796"
    11) gc_numeric_trees_missed
    12) "0"
    13) gc_blocks_denied
    14) "0"
49) cursor_stats
50) 1) global_idle
    2) (integer) 0
    3) global_total
    4) (integer) 0
    5) index_capacity
    6) (integer) 128
    7) index_total
    8) (integer) 0
51) stopwords_list
52) 1) "tlv"
    2) "summer"
    3) "2020"
{{< / highlight >}}
</details>

## See also

`FT.CREATE` | `FT.SEARCH`

## Related topics

[RediSearch](/docs/stack/search)

