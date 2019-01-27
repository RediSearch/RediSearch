/*
 * redisearch_api.c
 *
 *  Created on: 24 Jan 2019
 *      Author: root
 */
#include "spec.h"
#include "field_spec.h"
#include "redisearch_api.h"
#include <assert.h>

int RS_GetLowLevelApiVersion(){
  return REDISEARCH_LOW_LEVEL_API_VERSION;
}

IndexSpec* RS_CreateSpec(const char* name){
  IndexSpec *spec = NewIndexSpec(name);
  spec->flags |= Index_Temporary; // temporary is so that we will not use threads!!
  return spec;
}

static inline FieldSpec* RS_CreateField(IndexSpec* sp, const char* name){
  FieldSpec *fs = IndexSpec_CreateField(sp);
  FieldSpec_SetName(fs, name);
  return fs;
}

FieldSpec* RS_CreateTextField(IndexSpec* sp, const char* name){
  FieldSpec *fs = RS_CreateField(sp, name);
  FieldSpec_InitializeText(fs);
  return fs;
}

void RS_TextFieldSetWeight(FieldSpec* fs, double w){
  assert(fs->type == FIELD_FULLTEXT);
  FieldSpec_TextSetWeight(fs, w);
}

void RS_TextFieldNoStemming(FieldSpec* fs){
  assert(fs->type == FIELD_FULLTEXT);
  FieldSpec_TextNoStem(fs);
}

void RS_TextFieldPhonetic(FieldSpec* fs, IndexSpec* sp){
  assert(fs->type == FIELD_FULLTEXT);
  FieldSpec_TextPhonetic(fs);
  sp->flags |= Index_HasPhonetic;
}

FieldSpec* RS_CreateGeoField(IndexSpec* sp, const char* name){
  FieldSpec *fs = RS_CreateField(sp, name);
  FieldSpec_InitializeGeo(fs);
  return fs;
}

FieldSpec* RS_CreateNumericField(IndexSpec* sp, const char* name){
  FieldSpec *fs = RS_CreateField(sp, name);
  FieldSpec_InitializeNumeric(fs);
  return fs;
}

FieldSpec* RS_CreateTagField(IndexSpec* sp, const char* name){
  FieldSpec *fs = RS_CreateField(sp, name);
  FieldSpec_InitializeTag(fs);
  return fs;
}

void RS_TagSetSeparator(FieldSpec* fs, char sep){
  assert(fs->type == FIELD_TAG);
  FieldSpec_TagSetSeparator(fs, sep);
}

void RS_FieldSetSortable(FieldSpec* fs, IndexSpec* sp){
  FieldSpec_SetSortable(fs);
  fs->sortIdx = RSSortingTable_Add(sp->sortables, fs->name, fieldTypeToValueType(fs->type));
}

void RS_FieldSetNoIndex(FieldSpec* fs){
  FieldSpec_SetNoIndex(fs);
}

#define REGISTER_API(name, registerApiCallback) \
  if(registerApiCallback("RediSearch_" #name, RS_ ## name) != REDISMODULE_OK){\
    printf("could not register RediSearch_" #name "\r\n");\
    return REDISMODULE_ERR;\
  }

int moduleRegisterApi(const char *funcname, void *funcptr);

int RS_InitializeLowLevelApi(RedisModuleCtx* ctx){
  REGISTER_API(GetLowLevelApiVersion, moduleRegisterApi);

  REGISTER_API(CreateSpec, moduleRegisterApi);
  REGISTER_API(CreateTextField, moduleRegisterApi);
  REGISTER_API(TextFieldSetWeight, moduleRegisterApi);
  REGISTER_API(TextFieldNoStemming, moduleRegisterApi);
  REGISTER_API(TextFieldPhonetic, moduleRegisterApi);
  REGISTER_API(CreateGeoField, moduleRegisterApi);
  REGISTER_API(CreateNumericField, moduleRegisterApi);
  REGISTER_API(CreateTagField, moduleRegisterApi);
  REGISTER_API(TagSetSeparator, moduleRegisterApi);
  REGISTER_API(FieldSetSortable, moduleRegisterApi);
  REGISTER_API(FieldSetNoIndex, moduleRegisterApi);

  return REDISMODULE_OK;
}


