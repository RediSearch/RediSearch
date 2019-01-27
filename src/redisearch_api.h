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

  if(RediSearch_GetLowLevelApiVersion() > REDISEARCH_LOW_LEVEL_API_VERSION){
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}


#endif /* SRC_REDISEARCH_API_H_ */
