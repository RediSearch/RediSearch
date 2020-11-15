#pragma once

#include "decNumber.h"

extern decContext decCtx_g;
extern decNumber decZero_g;

void initDecCtx();
void decSetInfinity(decNumber *dn, int negative);

static inline int decimalCmp(decNumber *dec1, decNumber *dec2) {
  decNumber total;
  decNumberCompare(&total, dec1, dec2, &decCtx_g);
  //decNumberSubtract(&total, dec1, dec2, &decCtx_g);
  if (decNumberIsZero(&total)) return 0;
  if (!decNumberIsNegative(&total)) return 1;
  return -1; 
}