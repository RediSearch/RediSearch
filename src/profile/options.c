/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "profile/options.h"

bool ApplyProfileFlags(QEFlags *flags, ProfileOptions profileOptions) {
  if (profileOptions & EXEC_WITH_PROFILE) {
    *flags |= QEXEC_F_PROFILE;
    if (profileOptions & EXEC_WITH_PROFILE_LIMITED) {
      *flags |= QEXEC_F_PROFILE_LIMITED;
    }
    return true;
  }
  return false;
}


void ApplyProfileOptions(QueryProcessingCtx* qctx, QEFlags *flags, ProfileOptions profileOptions) {
  qctx->isProfile = ApplyProfileFlags(flags, profileOptions);
}
