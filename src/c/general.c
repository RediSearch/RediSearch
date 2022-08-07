
//#include "spec.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// clang-format off

#define NUMERIC_STR "NUMERIC"
#define GEO_STR "GEO"

#define SPEC_NOOFFSETS_STR "NOOFFSETS"
#define SPEC_NOFIELDS_STR "NOFIELDS"
#define SPEC_NOFREQS_STR "NOFREQS"
#define SPEC_NOHL_STR "NOHL"
#define SPEC_SCHEMA_STR "SCHEMA"
#define SPEC_SCHEMA_EXPANDABLE_STR "MAXTEXTFIELDS"
#define SPEC_TEMPORARY_STR "TEMPORARY"
#define SPEC_TEXT_STR "TEXT"
#define SPEC_WEIGHT_STR "WEIGHT"
#define SPEC_NOSTEM_STR "NOSTEM"
#define SPEC_PHONETIC_STR "PHONETIC"
#define SPEC_TAG_STR "TAG"
#define SPEC_SORTABLE_STR "SORTABLE"
#define SPEC_STOPWORDS_STR "STOPWORDS"
#define SPEC_NOINDEX_STR "NOINDEX"
#define SPEC_SEPARATOR_STR "SEPARATOR"
#define SPEC_MULTITYPE_STR "MULTITYPE"

#define IDXFLD_LEGACY_FULLTEXT 0
#define IDXFLD_LEGACY_NUMERIC 1
#define IDXFLD_LEGACY_GEO 2
#define IDXFLD_LEGACY_TAG 3
#define IDXFLD_LEGACY_MAX 3

enum FieldType {
  // Newline
  INDEXFLD_T_FULLTEXT = 0x01,
  INDEXFLD_T_NUMERIC = 0x02,
  INDEXFLD_T_GEO = 0x04,
  INDEXFLD_T_TAG = 0x08
};

#define INDEXTYPE_TO_POS(T)           \
  (T == INDEXFLD_T_FULLTEXT   ? 0 : \
  (T == INDEXFLD_T_NUMERIC    ? 1 : \
  (T == INDEXFLD_T_GEO        ? 2 : \
  (T == INDEXFLD_T_TAG        ? 3 : -1))))

#define INDEXTYPE_FROM_POS(P) (1<<(P))

#define IXFLDPOS_FULLTEXT INDEXTYPE_TO_POS(INDEXFLD_T_FULLTEXT)
#define IXFLDPOS_NUMERIC INDEXTYPE_TO_POS(INDEXFLD_T_NUMERIC)
#define IXFLDPOS_GEO INDEXTYPE_TO_POS(INDEXFLD_T_GEO)
#define IXFLDPOS_TAG INDEXTYPE_TO_POS(INDEXFLD_T_TAG)

enum RSCondition {
  RSCondition_Eq,  // Equality, ==
  RSCondition_Lt,  // Less than, <
  RSCondition_Le,  // Less than or equal, <=
  RSCondition_Gt,  // Greater than, >
  RSCondition_Ge,  // Greater than or equal, >=
  RSCondition_Ne,  // Not equal, !=
  RSCondition_And, // Logical AND of 2 expressions, &&
  RSCondition_Or   // Logical OR of 2 expressions, ||
};

// clang-format on

///////////////////////////////////////////////////////////////////////////////////////////////

static const char *SpecTypeNames[] = {[IXFLDPOS_FULLTEXT] = SPEC_TEXT_STR,
                                      [IXFLDPOS_NUMERIC] = NUMERIC_STR,
                                      [IXFLDPOS_GEO] = GEO_STR,
                                      [IXFLDPOS_TAG] = SPEC_TAG_STR};

//---------------------------------------------------------------------------------------------

const char ToksepMap_g[256] = {
    [' '] = 1, ['\t'] = 1, [','] = 1,  ['.'] = 1, ['/'] = 1, ['('] = 1, [')'] = 1, ['{'] = 1,
    ['}'] = 1, ['['] = 1,  [']'] = 1,  [':'] = 1, [';'] = 1, ['~'] = 1, ['!'] = 1, ['@'] = 1,
    ['#'] = 1, ['$'] = 1,  ['%'] = 1,  ['^'] = 1, ['&'] = 1, ['*'] = 1, ['-'] = 1, ['='] = 1,
    ['+'] = 1, ['|'] = 1,  ['\''] = 1, ['`'] = 1, ['"'] = 1, ['<'] = 1, ['>'] = 1, ['?'] = 1,
};

//---------------------------------------------------------------------------------------------

const enum FieldType fieldTypeMap[] = {[IDXFLD_LEGACY_FULLTEXT] = INDEXFLD_T_FULLTEXT,
                                  [IDXFLD_LEGACY_NUMERIC] = INDEXFLD_T_NUMERIC,
                                  [IDXFLD_LEGACY_GEO] = INDEXFLD_T_GEO,
                                  [IDXFLD_LEGACY_TAG] = INDEXFLD_T_TAG};

//---------------------------------------------------------------------------------------------

static const char *RSConditionStrings[] = {
    [RSCondition_Eq] = "==",  [RSCondition_Lt] = "<",  [RSCondition_Le] = "<=",
    [RSCondition_Gt] = ">",   [RSCondition_Ge] = ">=", [RSCondition_Ne] = "!=",
    [RSCondition_And] = "&&", [RSCondition_Or] = "||",
};

///////////////////////////////////////////////////////////////////////////////////////////////
