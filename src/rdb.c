#include "spec.h"
#include "alias.h"
#include "rdb.h"

dict *specDict_g_bkup;
TrieMap *SchemaPrefixes_g_bkup;
AliasTable *AliasTable_g_bkup;

void Backup_Globals() {
  specDict_g_bkup = specDict_g;
  specDict_g = dictCreate(&dictTypeHeapStrings, NULL);

  SchemaPrefixes_g_bkup = SchemaPrefixes_g;
  SchemaPrefixes_Create();

  AliasTable_g_bkup = AliasTable_g;
  IndexAlias_InitGlobal();
}

void Restore_Globals() {
  Indexes_Free(specDict_g, SchemaPrefixes_g, AliasTable_g, NULL, 1);

  dictRelease(specDict_g);
  specDict_g = specDict_g_bkup;
  specDict_g_bkup = NULL;

  SchemaPrefixes_Free(SchemaPrefixes_g);
  SchemaPrefixes_g = SchemaPrefixes_g_bkup;
  SchemaPrefixes_g_bkup = NULL;

  IndexAlias_DestroyGlobal(&AliasTable_g);
  AliasTable_g = AliasTable_g_bkup;
  AliasTable_g_bkup = NULL;
}

void Discard_Globals_Backup() {
  Indexes_Free(specDict_g_bkup, SchemaPrefixes_g_bkup, AliasTable_g_bkup, NULL, 1);

  dictRelease(specDict_g_bkup);
  specDict_g_bkup = NULL;

  SchemaPrefixes_Free(SchemaPrefixes_g_bkup);
  SchemaPrefixes_g_bkup = NULL;

  IndexAlias_DestroyGlobal(&AliasTable_g_bkup);
}
