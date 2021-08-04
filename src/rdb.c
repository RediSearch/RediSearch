#include "spec.h"
#include "alias.h"
#include "rdb.h"

dict *specDict_g_bkup;
TrieMap *ScemaPrefixes_g_bkup;
AliasTable *AliasTable_g_bkup;

void Backup_Globals() {
  specDict_g_bkup = specDict_g;
  specDict_g = dictCreate(&dictTypeHeapStrings, NULL);

  ScemaPrefixes_g_bkup = ScemaPrefixes_g;
  SchemaPrefixes_Create();

  AliasTable_g_bkup = AliasTable_g;
  IndexAlias_InitGlobal();
}

void Restore_Globals() {
  Indexes_Free(specDict_g);
  specDict_g = specDict_g_bkup;
  specDict_g_bkup = NULL;

  SchemaPrefixes_Free(ScemaPrefixes_g);
  ScemaPrefixes_g = ScemaPrefixes_g_bkup;
  ScemaPrefixes_g_bkup = NULL;

  IndexAlias_DestroyGlobal(&AliasTable_g);
  AliasTable_g = AliasTable_g_bkup;
  AliasTable_g_bkup = NULL;
}

void Discard_Globals_Backup() {
  Indexes_Free(specDict_g_bkup);
  specDict_g_bkup = NULL;

  SchemaPrefixes_Free(ScemaPrefixes_g_bkup);
  ScemaPrefixes_g_bkup = NULL;

  IndexAlias_DestroyGlobal(&AliasTable_g_bkup);
}
