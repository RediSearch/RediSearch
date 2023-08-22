
#include "header.h"
#include "rmalloc.h"

extern struct SN_env * SN_create_env(int S_size, int I_size, alloc_context *actx)
{
    struct SN_env * z = (struct SN_env *) rm_calloc(actx, 1, sizeof(struct SN_env));
    if (z == NULL) return NULL;
    z->p = create_s(actx);
    if (z->p == NULL) goto error;
    if (S_size)
    {
        int i;
        z->S = (symbol * *) rm_calloc(actx, S_size, sizeof(symbol *));
        if (z->S == NULL) goto error;

        for (i = 0; i < S_size; i++)
        {
            z->S[i] = create_s(actx);
            if (z->S[i] == NULL) goto error;
        }
    }

    if (I_size)
    {
        z->I = (int *) rm_calloc(actx, I_size, sizeof(int));
        if (z->I == NULL) goto error;
    }

    return z;
error:
    SN_close_env(z, S_size, actx);
    return NULL;
}

extern void SN_close_env(struct SN_env * z, int S_size, alloc_context *actx)
{
    if (z == NULL) return;
    if (S_size)
    {
        int i;
        for (i = 0; i < S_size; i++)
        {
            lose_s(z->S[i], actx);
        }
        rm_free(actx, z->S);
    }
    rm_free(actx, z->I);
    if (z->p) lose_s(z->p, actx);
    rm_free(actx, z);
}

extern int SN_set_current(struct SN_env * z, int size, const symbol * s, alloc_context *actx)
{
    int err = replace_s(z, 0, z->l, size, s, NULL, actx);
    z->c = 0;
    return err;
}
