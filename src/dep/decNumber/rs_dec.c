#include "rs_dec.h"

decContext decCtx_g;
decNumber decZero_g;

void initDecCtx() {
  decContextDefault(&decCtx_g, DEC_INIT_DECIMAL128);
  decNumberZero(&decZero_g);
}

void decSetInfinity(decNumber *dn, int negative) {
  decNumberZero(dn);          // be optimistic
  dn->bits|=DECINF;           // set as infinity
  if (negative) {
    dn->bits|=DECNEG;         // set as negative
  }
}