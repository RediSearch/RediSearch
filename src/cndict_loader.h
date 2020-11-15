
#pragma once

#include "dep/friso/friso.h"

// Loads the built-in dictionary into the provided dictionary object
int ChineseDictLoad(friso_dic_t);

// Configures the friso config object based on built-in settings
void ChineseDictConfigure(friso_t, friso_config_t);
