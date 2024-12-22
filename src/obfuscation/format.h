/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef FORMAT_H
#define FORMAT_H
#include <stdint.h>
#include "obfuscation/hidden.h"
#include "obfuscation/obfuscation_api.h"

#ifdef __cplusplus
extern "C" {
#endif

const char *FormatHiddenText(const HiddenString *name, bool obfuscate);
// returned char* must be freed by the caller
const char *FormatHiddenField(const HiddenString *name, t_uniqueId fieldId, char buffer[MAX_OBFUSCATED_FIELD_NAME], bool obfuscate);

#ifdef __cplusplus
}
#endif

#endif //FORMAT_H