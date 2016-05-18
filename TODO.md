## Upcoming features:

MVP:
* Drop index support
* Optimize buffer sizes (truncate to actual size at end of indexing)
* Numeric Fields
* Auto-suggest / speller
* Index HASH values

* Deletion support
* Custom user flags filtering
* Boolean expression parsing
* Geo Fields
* Proper unicode support (currently we assume input is utf-8)
* Query expansion
* Fuzzy matching
* Index json values
* Doc Snippets retrieval
* Query Explain
* Update support
## Known bugs:

* intersect doesn't return last result if it's the end of the index
