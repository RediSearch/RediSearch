
typedef unsigned char symbol;

/* Or replace 'char' above with 'short' for 16 bit characters.

   More precisely, replace 'char' with whatever type guarantees the
   character width you need. Note however that sizeof(symbol) should divide
   HEAD, defined in header.h as 2*sizeof(int), without remainder, otherwise
   there is an alignment problem. In the unlikely event of a problem here,
   consult Martin Porter.

*/

#include "rmalloc.h"

struct SN_env {
    symbol * p;
    int c; int l; int lb; int bra; int ket;
    symbol * * S;
    int * I;
};

#ifdef __cplusplus
extern "C" {
#endif

extern struct SN_env * SN_create_env(int S_size, int I_size, alloc_context *actx);
extern void SN_close_env(struct SN_env * z, int S_size, alloc_context *actx);

extern int SN_set_current(struct SN_env * z, int size, const symbol * s);

#ifdef __cplusplus
}
#endif
