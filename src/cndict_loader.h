/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef CNDICT_LOADER_H
#define CNDICT_LOADER_H

#include "friso/friso.h"

// Defined in cndict_loader.c
// Loads the built-in dictionary into the provided dictionary object
int ChineseDictLoad(alloc_context *actx, friso_dic_t);

// Defined in generated/cndict_data.c
// Configures the friso config object based on built-in settings.
void ChineseDictConfigure(friso_t, friso_config_t);
#endif