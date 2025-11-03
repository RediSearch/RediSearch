#include "profile/options.h"
#include "aggregate/aggregate.h"

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