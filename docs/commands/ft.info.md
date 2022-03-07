Returns information and statistics on the index. Returned values include:

* `index_definition`: reflection of `FT.CREATE` command parameters.
* `fields`: index schema - field names, types, and attributes.
* Number of documents.
* Number of distinct terms.
* Average bytes per record.
* Size and capacity of the index buffers.
* Indexing state and percentage as well as failures:
  * `indexing`: whether of not the index is being scanned in the background,
  * `percent_indexed`: progress of background indexing (1 if complete),
  * `hash_indexing_failures`: number of failures due to operations not compatible with index schema.

Optional

* Statistics about the `garbage collector` for all options other than NOGC.
* Statistics about `cursors` if a cursor exists for the index.
* Statistics about `stopword lists` if a custom stopword list is used.

#### Parameters

- **index**: The Fulltext index name. The index must be first created with FT.CREATE

@return

@array-reply - pairs of keys and values.

@examples

```
127.0.0.1:6379> ft.info idx
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
19) total_inverted_index_blocks
20) "933290"
21) offset_vectors_sz_mb
22) "0.65932846069335938"
23) doc_table_size_mb
24) "29.893482208251953"
25) sortable_values_size_mb
26) "11.432285308837891"
27) key_table_size_mb
28) "1.239776611328125e-05"
29) records_per_doc_avg
30) "-nan"
31) bytes_per_record_avg
32) "-nan"
33) offsets_per_term_avg
34) "inf"
35) offset_bits_per_record_avg
36) "8"
37) hash_indexing_failures
38) "0"
39) indexing
40) "0"
41) percent_indexed
42) "1"
43) gc_stats
44)  1) bytes_collected
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
45) cursor_stats
46) 1) global_idle
    2) (integer) 0
    3) global_total
    4) (integer) 0
    5) index_capacity
    6) (integer) 128
    7) index_total
    8) (integer) 0
47) stopwords_list
48) 1) "tlv"
    2) "summer"
    3) "2020"
```