
#include "spec.h"

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

const FieldType fieldTypeMap[] = {[IDXFLD_LEGACY_FULLTEXT] = INDEXFLD_T_FULLTEXT,
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
