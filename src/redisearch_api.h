/*
 * redisearch_api.h
 *
 *  Created on: 24 Jan 2019
 *      Author: root
 */

#ifndef SRC_REDISEARCH_API_H_
#define SRC_REDISEARCH_API_H_

#include "redismodule.h"

#define REDISEARCH_LOW_LEVEL_API_VERSION 1

#define MODULE_API_FUNC(x) (*x)

typedef struct IndexSpec Index;
typedef struct FieldSpec Field;
typedef struct Document Doc;
typedef struct RSQueryNode QN;
typedef struct indexIterator ResultsIterator;

int MODULE_API_FUNC(RediSearch_GetLowLevelApiVersion)();

Index* MODULE_API_FUNC(RediSearch_CreateSpec)(const char* name);

Field* MODULE_API_FUNC(RediSearch_CreateTextField)(Index* sp, const char* name);

void MODULE_API_FUNC(RediSearch_TextFieldSetWeight)(Field* fs, double w);

void MODULE_API_FUNC(RediSearch_TextFieldNoStemming)(Field* fs);

void MODULE_API_FUNC(RediSearch_TextFieldPhonetic)(Field* fs, Index* sp);

Field* MODULE_API_FUNC(RediSearch_CreateGeoField)(Index* sp, const char* name);

Field* MODULE_API_FUNC(RediSearch_CreateNumericField)(Index* sp, const char* name);

Field* MODULE_API_FUNC(RediSearch_CreateTagField)(Index* sp, const char* name);

void MODULE_API_FUNC(RediSearch_TagSetSeparator)(Field* fs, char sep);

void MODULE_API_FUNC(RediSearch_FieldSetSortable)(Field* fs, Index* sp);

void MODULE_API_FUNC(RediSearch_FieldSetNoIndex)(Field* fs);

Doc* MODULE_API_FUNC(RediSearch_CreateDocument)(const void *docKey, size_t len, double score, const char *lang);

void MODULE_API_FUNC(RediSearch_DocumentAddTextField)(Doc* d, const char* fieldName, const char* val);

void MODULE_API_FUNC(RediSearch_DocumentAddNumericField)(Doc* d, const char* fieldName, double num);

void MODULE_API_FUNC(RediSearch_SpecAddDocument)(Index* sp, Doc* d);

QN* MODULE_API_FUNC(RediSearch_CreateTokenNode)(const char* token);

QN* MODULE_API_FUNC(RediSearch_CreateNumericNode)(const char* field, double max, double min, int includeMax, int includeMin);

QN *MODULE_API_FUNC(RediSearch_CreatePrefixNode)(const char *s);

QN *MODULE_API_FUNC(RediSearch_CreateTagNode)(const char *field);

void MODULE_API_FUNC(RediSearch_TagNodeAddChild)(QN* qn, QN* child);

QN* MODULE_API_FUNC(RediSearch_CreateIntersectNode)(int exact);

void MODULE_API_FUNC(RediSearch_IntersectNodeAddChild)(QN* qn, QN* child);

QN* MODULE_API_FUNC(RediSearch_CreateUnionNode)();

void MODULE_API_FUNC(RediSearch_UnionNodeAddChild)(QN* qn, QN* child);

ResultsIterator* MODULE_API_FUNC(RediSearch_GetResutlsIterator)(QN* qn, Index* sp);

const void* MODULE_API_FUNC(RediSearch_ResutlsIteratorNext)(ResultsIterator* iter, Index* sp, size_t* len);

void MODULE_API_FUNC(RediSearch_ResutlsIteratorFree)(ResultsIterator* iter);

#define REDISEARCH_MODULE_INIT_FUNCTION(name) \
  if (RedisModule_GetApi("RediSearch_" #name, ((void **)&RediSearch_ ## name))) { \
    printf("could not initialize RediSearch_" #name "\r\n");\
    return REDISMODULE_ERR; \
  }

static bool RediSearch_Initialize(){
  REDISEARCH_MODULE_INIT_FUNCTION(GetLowLevelApiVersion);

  REDISEARCH_MODULE_INIT_FUNCTION(CreateSpec);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateTextField);
  REDISEARCH_MODULE_INIT_FUNCTION(TextFieldSetWeight);
  REDISEARCH_MODULE_INIT_FUNCTION(TextFieldNoStemming);
  REDISEARCH_MODULE_INIT_FUNCTION(TextFieldPhonetic);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateGeoField);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateNumericField);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateTagField);
  REDISEARCH_MODULE_INIT_FUNCTION(TagSetSeparator);
  REDISEARCH_MODULE_INIT_FUNCTION(FieldSetSortable);
  REDISEARCH_MODULE_INIT_FUNCTION(FieldSetNoIndex);

  REDISEARCH_MODULE_INIT_FUNCTION(CreateDocument);
  REDISEARCH_MODULE_INIT_FUNCTION(DocumentAddTextField);
  REDISEARCH_MODULE_INIT_FUNCTION(DocumentAddNumericField);

  REDISEARCH_MODULE_INIT_FUNCTION(SpecAddDocument);

  REDISEARCH_MODULE_INIT_FUNCTION(CreateTokenNode);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateNumericNode);
  REDISEARCH_MODULE_INIT_FUNCTION(CreatePrefixNode);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateTagNode);
  REDISEARCH_MODULE_INIT_FUNCTION(TagNodeAddChild);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateIntersectNode);
  REDISEARCH_MODULE_INIT_FUNCTION(IntersectNodeAddChild);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateUnionNode);
  REDISEARCH_MODULE_INIT_FUNCTION(UnionNodeAddChild);

  REDISEARCH_MODULE_INIT_FUNCTION(GetResutlsIterator);
  REDISEARCH_MODULE_INIT_FUNCTION(ResutlsIteratorNext);
  REDISEARCH_MODULE_INIT_FUNCTION(ResutlsIteratorFree);

  if(RediSearch_GetLowLevelApiVersion() > REDISEARCH_LOW_LEVEL_API_VERSION){
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}


#endif /* SRC_REDISEARCH_API_H_ */
