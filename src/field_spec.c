#include "field_spec.h"
#include "rmalloc.h"

RSValueType fieldTypeToValueType(FieldType ft) {
  switch (ft) {
    case FIELD_NUMERIC:
      return RSValue_Number;
    case FIELD_FULLTEXT:
    case FIELD_TAG:
      return RSValue_String;
    case FIELD_GEO:
    default:
      // geo is not sortable so we don't care as of now...
      return RSValue_Null;
  }
}

void FieldSpec_SetName(FieldSpec* fs, const char* name){
  fs->name = rm_strdup(name);
}

void FieldSpec_SetIndex(FieldSpec* fs, uint16_t index){
  fs->index = index;
}

static inline void FieldSpec_Init(FieldSpec* fs){
  fs->sortIdx = -1;
  fs->options = 0;
}

static inline FieldSpec* FieldSpec_Create(){
  FieldSpec* ret = rm_malloc(sizeof(FieldSpec));
  return ret;
}

void FieldSpec_InitializeText(FieldSpec* fs){
  FieldSpec_Init(fs);
  fs->type = FIELD_FULLTEXT;
  fs->textOpts.weight = 1.0;
}

void FieldSpec_InitializeNumeric(FieldSpec* fs){
  FieldSpec_Init(fs);
  fs->type = FIELD_NUMERIC;
}

void FieldSpec_InitializeGeo(FieldSpec* fs){
  FieldSpec_Init(fs);
  fs->type = FIELD_GEO;
}

void FieldSpec_InitializeTag(FieldSpec* fs){
  FieldSpec_Init(fs);
  fs->type = FIELD_TAG;
  fs->tagOpts.separator = ',';
  fs->tagOpts.flags = TAG_FIELD_DEFAULT_FLAGS;
}

FieldSpec* FieldSpec_CreateNumeric(){
  FieldSpec* ret = FieldSpec_Create();
  FieldSpec_InitializeNumeric(ret);
  return ret;
}

FieldSpec* FieldSpec_CreateGeo(){
  FieldSpec* ret = FieldSpec_Create();
  FieldSpec_InitializeGeo(ret);
  return ret;
}

FieldSpec* FieldSpec_CreateTag(){
  FieldSpec* ret = FieldSpec_Create();
  FieldSpec_InitializeTag(ret);
  return ret;
}

FieldSpec* FieldSpec_CreateText(){
  FieldSpec* ret = FieldSpec_Create();
  FieldSpec_InitializeText(ret);
  return ret;
}

void FieldSpec_TextNoStem(FieldSpec* fs){
  fs->options |= FieldSpec_NoStemming;
}

void FieldSpec_TextSetWeight(FieldSpec* fs, double w){
  fs->textOpts.weight = w;
}

void FieldSpec_TextPhonetic(FieldSpec* fs){
  fs->options |= FieldSpec_Phonetics;
}

void FieldSpec_TagSetSeparator(FieldSpec* fs, char sep){
  fs->tagOpts.separator = sep;
}

void FieldSpec_SetSortable(FieldSpec* fs){
  fs->options |= FieldSpec_Sortable;
}

void FieldSpec_SetNoIndex(FieldSpec* fs){
  fs->options |= FieldSpec_NotIndexable;
}

void FieldSpec_Dispose(FieldSpec* fs){
  if (fs->name) {
    rm_free(fs->name);
    fs->name = NULL;
  }
}

void FieldSpec_Free(FieldSpec* fs){
  FieldSpec_Dispose(fs);
  rm_free(fs);
}



