#include "obfuscation/format.h"

const char* FormatHiddenText(HiddenName *name, bool obfuscate) {
  return obfuscate ? Obfuscate_Text() : HiddenName_GetUnsafe(name, NULL);
}

const char* FormatHiddenField(HiddenName *name, t_uniqueId fieldId, char buffer[MAX_OBFUSCATED_FIELD_NAME], bool obfuscate) {
  return obfuscate ? Obfuscate_Field(fieldId, buffer) : HiddenName_GetUnsafe(name, NULL);
}