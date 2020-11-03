
#pragma once

#include "redismodule.h"
#include "doc_table.h"
#include "document.h"
#include "value.h"
#include "geo_index.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct InvertedIndex;

/**
 * A Tag Index is an index that indexes textual tags for documents, in a simple manner than a full
 * text index, although
 * it uses the same internal mechanism as a full-text index.
 *
 * The main differences are:
 *
 * 1. An entire tag index resides in a single redis key, and doesn't have a key per term
 *
 * 2. We do not perform stemming on tag indexes.
 *
 * 3. The tokenization is simpler: The user can determine a separator (defaults to a comma),
 *    and we do whitespace trimming at the end of tags.
 *    Thus, tags can contain spaces, punctuation marks, accents, etc. The only two transformations
 *    we perform are lower-casing (not unicode sensitive as of now), and whitespace trimming.
 *
 * 4. Tags cannot be found from a general full-text search. i.e. if a document has a field called
 *    "tags" with the values "foo" and "bar", searching for foo or bar without a special tag
 * modifier
 *    (see below) will not return this document.
 *
 * 4. The index is much simpler and more compressed: We do not store frequencies, offset vectors of
 * field flags.
 *    The index contains only docuent ids encoded as deltas. This means that an entry in a tag index
 * is usually
 *    one or two bytes long. This makes them very memory efficient and fast.
 *
 * ## Creating a tag field
 *
 * Tag fields can be added to the schema in FT.ADD with the following syntax:
 *
 *    FT.CREATE ... SCHEMA ... {field_name} TAG [SEPARATOR {sep}]
 *
 * SEPARATOR defaults to a comma (`,`), and can be any printable ascii character.  For example:
 *
 *    FT.CREATE idx SCHEMA tags TAG SEPARATOR ";"
 *
 * An unlimited number of tag fields can be created per document, as long as the overall number of
 * fields is under 1024.
 *
 * ## Querying Tag Fields
 *
 * As mentioned above, just searching for a tag without any modifiers will not retrieve documents
 * containing it.
 *
 * The syntax for matching tags in a query is as follows (the curly braces are part of the syntax in
 * this case):
 *
 *    @<field_name>:{ <tag> | <tag> | ...}
 *
 * e.g.
 *
 *    @tags:{hello world | foo bar}
 *
 * **IMPORTANT**: When specifying multiple tags in the same tag clause, the semantic meaning is a
 *    **UNION** of the documents containing any of the tags (as in a SQL WHERE IN clause).
 *    If you need to intersect tags, you should repeat several tag clauses.
 *    For example:
 *
 *        FT.SEARCH idx "@tags:{hello | world}"
 *
 *    Will return documents containing either hello or world (or both). But:
 *
 *        FT.SEARCH idx "@tags:{hello} @tags:{world}"
 *
 *    Will return documents containing **both tags**.
 *
 * Notice that since tags can contain spaces (the separator by default is a comma), so can tags in
 * the query.
 *
 * However, if a tag contains stopwords (for example, the tag `to be or not to be` will cause a
 * syntax error),
 * you can alternatively escape the spaces inside the tags to avoid syntax errors. In redis-cli and
 * some clients, a second escaping is needed:
 *
 *    127.0.0.7:6379> FT.SEARCH idx "@tags:{to\\ be\\ or\\ not\\ to\\ be}"
 *
 *
 */

struct TagIndex : public Object {
  static uint32_t niqueId;

  uint32_t uniqueId;
  TrieMap *values;

  // Create a new tag index
  TagIndex();

  // Open the tag index key in redis
  static TagIndex *Open(RedisSearchCtx *sctx, RedisModuleString *formattedKey, int openWrite, RedisModuleKey **keyp);

  ~TagIndex();

  // Format the key name for a tag index
  static RedisModuleString *FormatName(RedisSearchCtx *sctx, const char *field);

  static char *SepString(char sep, char **s, size_t *toklen);

  struct Tags {
    arrayof(char*) tags;

    // Preprocess a document tag field, returning a vector of all tags split from the content
    Tags() : tags(NULL) {}
    Tags(Tags &&t) : tags(std::move(t.tags)) {}
    Tags(char sep, TagFieldFlags flags, const DocumentField *data);
    ~Tags() { Clear(); }

    Tags &operator=(Tags &&t) { tags = std::move(t.tags); return *this; }

    void Clear() {
      array_foreach(tags, tag, { rm_free(tag); });
      array_free(tags);
      tags = NULL;
    }

    size_t size() const { return array_len(tags); }
    bool operator!() const { return !tags; }
    const char *operator[](int i) const { return tags[i]; }
  };

  // Index a vector of pre-processed tags for a docId
  size_t Index(const Tags &tags, t_docId docId);

  // Open an index reader to iterate a tag index for a specific tag. Used at query evaluation time.
  // Returns NULL if there is no such tag in the index.
  IndexIterator *OpenReader(IndexSpec *sp, const char *value, size_t len, double weight);

  void RegisterConcurrentIterators(ConcurrentSearchCtx *conc, RedisModuleKey *key,
                                   RedisModuleString *keyname, array_t *iters);

  struct InvertedIndex *OpenIndex(const char *value, size_t len, int create);

  // Serialize all the tags in the index to the redis client
  void SerializeValues(RedisModuleCtx *ctx);

  // Ecode a single docId into a specific tag value
  size_t Put(const char *value, size_t len, t_docId docId);
};

//---------------------------------------------------------------------------------------------

extern RedisModuleType *TagIndexType;

// Register the tag index type in redis
int TagIndex_RegisterType(RedisModuleCtx *ctx);

///////////////////////////////////////////////////////////////////////////////////////////////
