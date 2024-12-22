#include "obfuscation/format.h"

const char *FormatHiddenText(const HiddenString *name, bool obfuscate) {
  const char *value = HiddenString_GetUnsafe(name, NULL);
  return obfuscate ? Obfuscate_Text(value) : value;
}

const char *FormatHiddenField(const HiddenString *name, t_uniqueId fieldId, char buffer[MAX_OBFUSCATED_FIELD_NAME], bool obfuscate) {
  return obfuscate ? Obfuscate_Field(fieldId, buffer) : HiddenString_GetUnsafe(name, NULL);
}