#include "numeric_index.h"
#include "redis_index.h"
#include "sys/param.h"
#include "index.h"
#include "util/arr.h"
#include "redismodule.h"
#include "util/misc.h"

#include "rmutil/vector.h"
#include "rmutil/util.h"

#include <math.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Returns true if the entire numeric range is contained between min and max
bool NumericRange::Contained(double min, double max) const {
  bool rc = minVal >= min && maxVal <= max;

  // printf("range %f..%f, min %f max %f, WITHIN? %d\n", minVal, maxVal, min, max, rc);
  return rc;
}

//---------------------------------------------------------------------------------------------

// Returns true if min and max are both inside the range. this is the opposite of _Within.
bool NumericRange::Contains(double min, double max) const {
  bool rc = minVal <= min && maxVal > max;
  // printf("range %f..%f, min %f max %f, contains? %d\n", minVal, maxVal, min, max, rc);
  return rc;
}

//---------------------------------------------------------------------------------------------

// Returns true if there is any overlap between the range and min/max
bool NumericRange::Overlaps(double min, double max) const {
  bool rc = (min >= minVal && min <= maxVal) || (max >= minVal && max <= maxVal);
  // printf("range %f..%f, min %f max %f, overlaps? %d\n", minVal, maxVal, min, max, rc);
  return rc;
}

//---------------------------------------------------------------------------------------------

size_t NumericRange::Add(t_docId docId, double value, int checkCard) {
  bool add = false;
  if (checkCard) {
    add = true;
    auto n = array_len(values);
    for (int i = 0; i < n; i++) {
      if (values[i].value == value) {
        add = false;
        values[i].appearances++;
        break;
      }
    }
  }
  if (minVal == NF_NEGATIVE_INFINITY || value < minVal) minVal = value;
  if (maxVal == NF_INFINITY || value > maxVal) maxVal = value;
  if (add) {
    if (card < splitCard) {
      CardinalityValue val(value, 1);
      values = array_append(values, val);
      unique_sum += value;
    }
    ++card;
  }

  return entries.WriteNumericEntry(docId, value);
}

//---------------------------------------------------------------------------------------------

double NumericRange::Split(NumericRangeNode **lp, NumericRangeNode **rp) {
  double split = unique_sum / (double)card;

  // printf("split point :%f\n", split);
  *lp = new NumericRangeNode(entries.numDocs / 2 + 1, minVal, split,
                            MIN(NR_MAXRANGE_CARD, 1 + splitCard * NR_EXPONENT));
  *rp = new NumericRangeNode(entries.numDocs / 2 + 1, split, maxVal,
                             MIN(NR_MAXRANGE_CARD, 1 + splitCard * NR_EXPONENT));

  NumericResult *res;
  NumericIndexReader ir(&entries);
  while (INDEXREAD_OK == ir.Read(&res)) {
    auto range = res->value < split ? (*lp)->range : (*rp)->range;
    if (range) {
      range->Add(res->docId, res->value, 1);
    }
  }

  // printf("Splitting node %p %f..%f, card %d size %d\n", this, minVal, maxVal, card,
  //        entries.numDocs);
  // printf("left node: %d, right: %d\n", (*lp)->range->entries->numDocs,
  //        (*rp)->range->entries.numDocs);
  return split;
}

//---------------------------------------------------------------------------------------------

NumericRange::NumericRange(double min, double max, size_t splitCard) :
  minVal(min), maxVal(max), unique_sum(0), card(0), splitCard(splitCard),
  values(array_new(CardinalityValue, 1)),
  entries(Index_StoreNumeric, 1) {
}

//---------------------------------------------------------------------------------------------

NumericRange::~NumericRange() {
  array_free(values);
}

///////////////////////////////////////////////////////////////////////////////////////////////

NumericRangeNode::NumericRangeNode(size_t cap, double min, double max, size_t splitCard) :
  left(NULL), right(NULL), value(0), maxDepth(0), range(new NumericRange(min, max, splitCard)) {
}

//---------------------------------------------------------------------------------------------

NRN_AddRv NumericRangeNode::Add(t_docId docId, double newval) {
  NRN_AddRv rv;
  if (!IsLeaf()) {
    // if this node has already split but retains a range, just add to the range without checking
    // anything
    if (range) {
      // Not leaf so should not add??
      /* sz += */ range->Add(docId, newval, 0);
      // should continue after??
    }

    // recursively add to its left or right child.
    NumericRangeNode **childP = newval < value ? &left : &right;
    NumericRangeNode *child = *childP;
    // if the child has split we get 1 in return
    rv = child->Add(docId, newval);

    if (rv.changed) {
      // if there was a split it means our max depth has increased.
      // we are too deep - we don't retain this node's range anymore.
      // this keeps memory footprint in check
      if (++maxDepth > NR_MAX_DEPTH && range) {
        delete range;
        range = NULL;
      }

      // check if we need to rebalance the child.
      // To ease the rebalance we don't rebalance the root
      // nor do we rebalance nodes that are with ranges (maxDepth > NR_MAX_DEPTH)
      if ((child->right->maxDepth - child->left->maxDepth) > NR_MAX_DEPTH) {  // role to the left
        NumericRangeNode *right = child->right;
        child->right = right->left;
        right->left = child;
        --child->maxDepth;
        *childP = right;  // replace the child with the new child
      } else if ((child->left->maxDepth - child->right->maxDepth) > NR_MAX_DEPTH) {  // role to the right
        NumericRangeNode *left = child->left;
        child->left = left->right;
        left->right = child;
        --child->maxDepth;
        *childP = left;  // replace the child with the new child
      }
    }
    // return 1 or 0 to our called, so this is done recursively
    return rv;
  }

  // if this node is a leaf - we add AND check the cardinality. We only split leaf nodes
  rv.sz = (uint32_t) range->Add(docId, newval, 1);
  int card = range->card;
  // printf("Added %d %f to node %f..%f, card now %zd, size now %zd\n", docId, newval,
  //        range->minVal, range->maxVal, card, range->entries.numDocs);
  if (card >= range->splitCard || (range->entries.numDocs > NR_MAXRANGE_SIZE && card > 1)) {

    // split this node but don't delete its range
    double split = range->Split(&left, &right);

    value = split;

    maxDepth = 1;
    rv.changed = 1;
  }

  return rv;
}

//---------------------------------------------------------------------------------------------

// Recursively add a node's children to the range
void NumericRangeNode::AddChildren(Vector<NumericRange> &v, double min, double max) {
  if (range) {
    // printf("min %f, max %f, range %f..%f, contained? %d, overlaps? %d, leaf? %d\n", min, max,
    //        range->minVal, range->maxVal, range->Contained(min, max),
    //        n->range->Overlaps(min, max), IsLeaf());

    // if range is completely contained in the search, add it and not inspect any downwards
    if (range->Contained(min, max)) {
      v.push_back(*range);
      return;
    }
    // No overlap at all - no need to do anything
    if (!range->Overlaps(min, max)) {
      return;
    }
  }

  // for non leaf nodes - we try to descend into their children
  if (!IsLeaf()) {
    if (left) left->AddChildren(v, min, max);
    if (right) right->AddChildren(v, min, max);
  } else if (range && range->Overlaps(min, max)) {
    v.push_back(*range);
  }
}

//---------------------------------------------------------------------------------------------

// Find the numeric ranges that fit the range we are looking for.
// We try to minimize the number of nodes we'll later need to union.
Vector<NumericRange> NumericRangeNode::FindRange(double min, double max) {
  Vector<NumericRange> leaves;
  leaves.reserve(8);
  AddChildren(leaves, min, max);
  // printf("Found %zd ranges for %f...%f\n", leaves->top, min, max);
  // for (int i = 0; i < leaves->top; i++) {
  //   NumericRange *rng;
  //   leaves->Get(i, &rng);
  //   printf("%f...%f (%f). %d card, %d splitCard\n", rng->minVal, rng->maxVal,
  //          rng->maxVal - rng->minVal, rng->entries.numDocs, rng->splitCard);
  // }

  return leaves;
}

//---------------------------------------------------------------------------------------------

NumericRangeNode::~NumericRangeNode() {
  if (range) {
    delete range;
  }

  delete left;
  delete right;
}

///////////////////////////////////////////////////////////////////////////////////////////////

uint16_t NumericRangeTree::UniqueId = 0;

//---------------------------------------------------------------------------------------------

// Create a new numeric range tree
NumericRangeTree::NumericRangeTree() {
  root = new NumericRangeNode(2, NF_NEGATIVE_INFINITY, NF_INFINITY, 2);
  numEntries = 0;
  numRanges = 1;
  revisionId = 0;
  lastDocId = 0;
  uniqueId = UniqueId++;
}

//---------------------------------------------------------------------------------------------

size_t NumericRangeTree::Add(t_docId docId, double value) {

  // Do not allow duplicate entries. This might happen due to indexer bugs and we need to protect
  // from it
  if (docId <= lastDocId) {
    return 0;
  }
  lastDocId = docId;

  NRN_AddRv rv = root->Add(docId, value);
  // rc != 0 means the tree nodes have changed, and concurrent iteration is not allowed now
  // we increment the revision id of the tree, so currently running query iterators on it
  // will abort the next time they get execution context
  if (rv.changed) {
    revisionId++;
  }
  numRanges += rv.changed;
  numEntries++;

  return rv.sz;
}

//---------------------------------------------------------------------------------------------

Vector<NumericRange> NumericRangeTree::Find(double min, double max) {
  return root->FindRange(min, max);
}

//---------------------------------------------------------------------------------------------

NumericRangeTree::~NumericRangeTree() {
  delete root;
}

void NumericRangeTree::Free(NumericRangeTree *p) {
  delete p;
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *NewNumericRangeIterator(const IndexSpec *sp, NumericRange *nr, const NumericFilter *f) {
  // if this range is at either end of the filter, we need to check each record
  if (f->Match(nr->minVal) && f->Match(nr->maxVal)) {
    // make the filter NULL so the reader will ignore it
    f = NULL;
  }
  IndexReader *ir = new NumericIndexReader(&nr->entries, sp, f);
  IndexIterator *it(ir);
  ir->isValidP = it->isValid;
  return it;
}

//---------------------------------------------------------------------------------------------

// Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
// the filter
static IndexIterator *createNumericIterator(const IndexSpec *sp, NumericRangeTree *t,
                                     const NumericFilter *f) {
  Vector<NumericRange> v = t->Find(f->min, f->max);
  if (v.empty()) {
    return NULL;
  }

  int n = v.size();
  // if we only selected one range - we can just iterate it without union or anything
  if (n == 1) {
    NumericRange rng = v[0];
    return NewNumericRangeIterator(sp, &rng, f);
  }

  // We create a  union iterator, advancing a union on all the selected range,
  // treating them as one consecutive range
  IndexIterators its;

  for (size_t i = 0; i < n; i++) {
    its.push_back(NewNumericRangeIterator(sp, &v[i], f));
  }

  return new UnionIterator(its, NULL, 1, 1);
}

///////////////////////////////////////////////////////////////////////////////////////////////

RedisModuleType *NumericIndexType = NULL;

//---------------------------------------------------------------------------------------------

RedisModuleString *RedisSearchCtx::NumericIndexKey(const char *field) {
  return RedisModule_CreateStringPrintf(redisCtx, NUMERICINDEX_KEY_FMT, spec->name, field);
}

//---------------------------------------------------------------------------------------------

static NumericRangeTree *openNumericKeysDict(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                             int write) {
  NumericRangeTree *val = NULL;
  if (ctx->spec->keysDict.contains(keyName)) {
    BaseIndex *index = ctx->spec->keysDict[keyName];
    try {
      val = dynamic_cast<NumericRangeTree*>(index);
    } catch (std::bad_cast) {
      throw Error("error: invalid index type...");
    }
    return val;
  }

  if (!write) {
    return NULL;
  }

  val = new NumericRangeTree();
  ctx->spec->keysDict.insert({keyName, val});
  return val;
}

//---------------------------------------------------------------------------------------------

IndexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, const NumericFilter *flt, ConcurrentSearch *csx) {
  RedisModuleString *s = ctx->spec->GetFormattedKeyByName(flt->fieldName, INDEXFLD_T_NUMERIC);
  if (!s) {
    return NULL;
  }
  RedisModuleKey *key = NULL;
  NumericRangeTree *t = NULL;
  if (ctx->spec->keysDict.empty()) {
    key = RedisModule_OpenKey(ctx->redisCtx, s, REDISMODULE_READ);
    if (!key || RedisModule_ModuleTypeGetType(key) != NumericIndexType) {
      return NULL;
    }

    t = RedisModule_ModuleTypeGetValue(key);
  } else {
    t = openNumericKeysDict(ctx, s, 0);
  }

  if (!t) {
    return NULL;
  }

  IndexIterator *it = createNumericIterator(ctx->spec, t, flt);
  if (!it) {
    return NULL;
  }

  if (csx) {
    csx->AddKey(NumericUnionConcKey(key, s, *t, it));
  }

  return it;
}

///////////////////////////////////////////////////////////////////////////////////////////////

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey) {
  if (!ctx->spec->keysDict.empty()) {
    return openNumericKeysDict(ctx, keyName, 1);
  }

  RedisModuleKey *key_s = NULL;
  if (!idxKey) {
    idxKey = &key_s;
  }

  *idxKey = RedisModule_OpenKey(ctx->redisCtx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

  int type = RedisModule_KeyType(*idxKey);
  if (type != REDISMODULE_KEYTYPE_EMPTY &&
      RedisModule_ModuleTypeGetType(*idxKey) != NumericIndexType) {
    return NULL;
  }

  // Create an empty value object if the key is currently empty
  NumericRangeTree *t;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    t = new NumericRangeTree();
    RedisModule_ModuleTypeSetValue((*idxKey), NumericIndexType, t);
  } else {
    t = RedisModule_ModuleTypeGetValue(*idxKey);
  }
  return t;
}

//---------------------------------------------------------------------------------------------

unsigned long NumericIndexType_MemUsage(const void *value) {
  const NumericRangeTree *t = value;
  unsigned long size = sizeof(NumericRangeTree);
  t->root->Traverse([&](NumericRangeNode *n) {
      size += sizeof(NumericRangeNode);
      if (n->range) {
        size += sizeof(NumericRange);
        size += n->range->card * sizeof(double);
        size += InvertedIndex_MemUsage(&n->range->entries);
      }
    });
  return size;
}

//---------------------------------------------------------------------------------------------

#define NUMERIC_INDEX_ENCVER 1

int NumericIndexType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {version: REDISMODULE_TYPE_METHOD_VERSION,
                               rdb_load: NumericIndexType_RdbLoad,
                               rdb_save: NumericIndexType_RdbSave,
                               aof_rewrite: GenericAofRewrite_DisabledHandler,
                               mem_usage: NumericIndexType_MemUsage,
                               digest: NumericIndexType_Digest,
                               free: NumericIndexType_Free
                               };

  NumericIndexType = RedisModule_CreateDataType(ctx, "numericdx", NUMERIC_INDEX_ENCVER, &tm);
  if (NumericIndexType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

// A single entry in a numeric index's single range. Since entries are binned together, each needs
// to have the exact value.
struct NumericRangeEntry {
  t_docId docId;
  double value;
};

//---------------------------------------------------------------------------------------------

static int cmpdocId(const void *p1, const void *p2) {
  NumericRangeEntry *e1 = (NumericRangeEntry *)p1;
  NumericRangeEntry *e2 = (NumericRangeEntry *)p2;

  return (int)e1->docId - (int)e2->docId;
}

//---------------------------------------------------------------------------------------------

/** Version 0 stores the number of entries beforehand, and then loads them */
static size_t loadV0(RedisModuleIO *rdb, NumericRangeEntry **entriespp) {
  uint64_t num = RedisModule_LoadUnsigned(rdb);
  if (!num) {
    return 0;
  }

  *entriespp = array_newlen(NumericRangeEntry, num);
  NumericRangeEntry *entries = *entriespp;
  for (size_t ii = 0; ii < num; ++ii) {
    entries[ii].docId = RedisModule_LoadUnsigned(rdb);
    entries[ii].value = RedisModule_LoadDouble(rdb);
  }
  return num;
}

//---------------------------------------------------------------------------------------------

#define NUMERIC_IDX_INITIAL_LOAD_SIZE 1 << 16

/** Version 0 stores (id,value) pairs, with a final 0 as a terminator */
static size_t loadV1(RedisModuleIO *rdb, NumericRangeEntry **entriespp) {
  NumericRangeEntry *entries = array_new(NumericRangeEntry, NUMERIC_IDX_INITIAL_LOAD_SIZE);
  while (1) {
    NumericRangeEntry cur;
    cur.docId = RedisModule_LoadUnsigned(rdb);
    if (!cur.docId) {
      break;
    }
    cur.value = RedisModule_LoadDouble(rdb);
    entries = array_append(entries, cur);
  }
  *entriespp = entries;
  return array_len(entries);
}

//---------------------------------------------------------------------------------------------

void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver > NUMERIC_INDEX_ENCVER) {
    return NULL;
  }

  NumericRangeEntry *entries = NULL;
  size_t numEntries = 0;
  if (encver == 0) {
    numEntries = loadV0(rdb, &entries);
  } else if (encver == 1) {
    numEntries = loadV1(rdb, &entries);
  } else {
    return NULL;  // Unknown version
  }

  // sort the entries by doc id, as they were not saved in this order
  qsort(entries, numEntries, sizeof(NumericRangeEntry), cmpdocId);
  NumericRangeTree *t;

  // now push them in order into the tree
  for (size_t i = 0; i < numEntries; i++) {
    t->Add(entries[i].docId, entries[i].value);
  }
  array_free(entries);
  return t;
}

//---------------------------------------------------------------------------------------------

void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value) {
  NumericRangeTree *t = value;

  t->root->Traverse([&](NumericRangeNode *n) {
    if (n->IsLeaf() && n->range) {
      NumericRange *rng = n->range;
      NumericResult *res = NULL;
      NumericIndexReader ir(&rng->entries);

      while (INDEXREAD_OK == ir.Read(&res)) {
        RedisModule_SaveUnsigned(rdb, res->docId);
        RedisModule_SaveDouble(rdb, res->value);
      }
    }
  });
  // Save the final record
  RedisModule_SaveUnsigned(rdb, 0);
}

//---------------------------------------------------------------------------------------------

void NumericIndexType_Digest(RedisModuleDigest *digest, void *value) {
}

//---------------------------------------------------------------------------------------------

void NumericIndexType_Free(void *value) {
  NumericRangeTree *t = value;
  delete t;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#define NODE_STACK_INITIAL_SIZE 4

NumericRangeTreeIterator::NumericRangeTreeIterator(NumericRangeTree *t) {
  nodesStack = array_new(NumericRangeNode *, NODE_STACK_INITIAL_SIZE);
  array_append(nodesStack, t->root);
}

//---------------------------------------------------------------------------------------------

NumericRangeNode *NumericRangeTreeIterator::Next() {
  if (array_len(nodesStack) == 0) {
    return NULL;
  }
  NumericRangeNode *node = array_pop(nodesStack);
  if (!node->IsLeaf()) {
    nodesStack = array_append(nodesStack, node->left);
    nodesStack = array_append(nodesStack, node->right);
  }

  return node;
}

//---------------------------------------------------------------------------------------------

NumericRangeTreeIterator::~NumericRangeTreeIterator() {
  array_free(nodesStack);
}

///////////////////////////////////////////////////////////////////////////////////////////////
