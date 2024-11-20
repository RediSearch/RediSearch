#include "obfuscation/format.h"

const char* FormatHiddenText(HiddenString *name, bool obfuscate) {
  return obfuscate ? Obfuscate_Text() : HiddenString_GetUnsafe(name, NULL);
}

const char* FormatHiddenField(HiddenString *name, t_uniqueId fieldId, char buffer[MAX_OBFUSCATED_FIELD_NAME], bool obfuscate) {
  return obfuscate ? Obfuscate_Field(fieldId, buffer) : HiddenString_GetUnsafe(name, NULL);
}