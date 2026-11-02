/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <aggregate/reducer.h>
#include <stdbool.h>
#include <stddef.h>

#include "reducers_ffi.h"
#include "rmutil/args.h"

Reducer *RDCRFirstValue_New(const ReducerOptions *options) {
  const RLookupKey *retkey;
  const RLookupKey *sortkey = NULL;
  bool ascending = true;

  if (!ReducerOpts_GetKey(options, &retkey)) {
    return NULL;
  }

  if (AC_AdvanceIfMatch(options->args, "BY")) {
    // Get the next field...
    if (!ReducerOpts_GetKey(options, &sortkey)) {
      return NULL;
    }
    if (AC_AdvanceIfMatch(options->args, "ASC")) {
      ascending = true;
    } else if (AC_AdvanceIfMatch(options->args, "DESC")) {
      ascending = false;
    }
  }

  if (!ReducerOpts_EnsureArgsConsumed(options)) {
    return NULL;
  }

  return FirstValueReducer_Create(retkey, sortkey, ascending);
}
