#ifndef CNDICT_LOADER_H
#define CNDICT_LOADER_H

#include "dep/friso/friso.h"

// Defined in cndict_loader.c
// Loads the built-in dictionary into the provided dictionary object
int ChineseDictLoad(friso_dic_t);

// Defined in generated/cndict_data.c
// Configures the friso config object based on built-in settings.
void ChineseDictConfigure(friso_t, friso_config_t);
#endif