/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

%token_type {Token}
%name MRTopologyRequest_Parse

%include {
#include "rmr/common.h"
#include "token.h"
#include "grammar.h"
#include "parser_ctx.h"
#include "../cluster.h"
#include "../node.h"
#include "../endpoint.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "lexer.h"

static void syntax_error(parseCtx *ctx, const char *fmt, ...);

} // END %include

%syntax_error {
    syntax_error(ctx, "Syntax error at offset %d near '%.*s'", TOKEN.pos, TOKEN.len, TOKEN.s);
}

%default_type { char * }
%default_destructor { rm_free($$); }
%extra_argument { parseCtx *ctx }
%type shard { RLShard }
%destructor shard { MRClusterNode_Free(&$$.node); }
%type endpoint { MREndpoint }
%destructor endpoint { MREndpoint_Free(&$$); }
%type topology { MRClusterTopology * }
%destructor topology { MRClusterTopology_Free($$); }

%type master {int}
%destructor master {}
%destructor cluster {}
%type tcp_addr {Token}
%destructor tcp_addr {}

root ::= cluster topology(D). {
    if (ctx->numSlots) {
        if (ctx->numSlots > 0 && ctx->numSlots <= 16384) {
            D->numSlots = ctx->numSlots;
        } else {
            // ERROR!
            syntax_error(ctx, "Invalid number of slots %d", ctx->numSlots);
            goto err;
        }
    }
    // translate optional shard func from arguments to proper enum.
    // We will only get it from newer versions of the cluster, so if we don't get it we assume
    // CRC12 / 4096
    D->hashFunc = MRHashFunc_None;
    if (ctx->shardFunc) {
        if (!strncasecmp(ctx->shardFunc, MRHASHFUNC_CRC12_STR, strlen(MRHASHFUNC_CRC12_STR))) {
            D->hashFunc = MRHashFunc_CRC12;
        } else if (!strncasecmp(ctx->shardFunc, MRHASHFUNC_CRC16_STR, strlen(MRHASHFUNC_CRC16_STR))) {
            D->hashFunc = MRHashFunc_CRC16;
        } else {
            // ERROR!
            syntax_error(ctx, "Invalid hash func %s", ctx->shardFunc);
            goto err;
        }
    }
    ctx->topology = D;

    // detect my id and mark the flag here
    for (size_t s = 0; s < ctx->topology->numShards; s++) {
        for (size_t n = 0; n < ctx->topology->shards[s].numNodes; n++) {
            if (ctx->my_id && !strcmp(ctx->topology->shards[s].nodes[n].id, ctx->my_id)) {
                ctx->topology->shards[s].nodes[n].flags |= MRNode_Self;
            }
        }
    }

err:
   if (ctx->ok == 0) {
        MRClusterTopology_Free(D);
   }
}

cluster ::= cluster MYID shardid(B) . {
    ctx->my_id = B;
}

cluster ::= cluster HASHFUNC STRING(A) NUMSLOTS INTEGER(B) . {
    ctx->shardFunc = A.strval;
    ctx->numSlots = B.intval;
}


cluster ::= cluster HASREPLICATION . {
    ctx->replication = 1;
}

cluster ::= .


topology(A) ::= RANGES INTEGER(B) . {
    A = MR_NewTopology(B.intval, 4096);
    // this is the default hash func
    A->hashFunc = MRHashFunc_CRC12;
}
//topology -> shardlist -> shard -> endpoint

topology(A) ::= topology(B) shard(C). {
    MRTopology_AddRLShard(B, &C);
    A = B;
}

shard(A) ::= SHARD shardid(B) SLOTRANGE INTEGER(C) INTEGER(D) endpoint(E) master(F). {
    A = (RLShard){
            .node = (MRClusterNode) {
            .id = B,
            .flags = MRNode_Coordinator | (F ? MRNode_Master : 0),
            .endpoint = E,
        },
        .startSlot = C.intval,
        .endSlot = D.intval,
    };
}


shardid(A) ::= STRING(B). {
    A = rm_strdup(B.strval);
}

shardid(A) ::= INTEGER(B). {
	__ignore__(rm_asprintf(&A, "%lld", B.intval));
}

endpoint(A) ::= tcp_addr(B). {
    A.unixSock = NULL;
    if (MREndpoint_Parse(B.strval, &A) != REDIS_OK) {
        syntax_error(ctx, "Invalid tcp address at offset %d: %s", B.pos, B.strval);
    }
}

endpoint(A) ::= tcp_addr(B) unix_addr(C) . {
    A.unixSock = C;
    if (MREndpoint_Parse(B.strval, &A) != REDIS_OK) {
        syntax_error(ctx, "Invalid tcp address at offset %d: %s", B.pos, B.strval);
    }
}


tcp_addr(A) ::= ADDR STRING(B) . {
    A = B;
}

unix_addr(A) ::= UNIXADDR STRING(B). {
    A = rm_strdup(B.strval);
}

master(A) ::= MASTER . {
    A = 1;
}

master(A) ::= . {
    A = 0;
}

%code {

static void syntax_error(parseCtx *ctx, const char *fmt, ...) {
    if (!ctx->errorMsg) {
        va_list ap;
        va_start(ap, fmt);
        __ignore__(rm_vasprintf(&ctx->errorMsg, fmt, ap));
        va_end(ap);
    }
    ctx->ok = 0;
}

static void parseCtx_Free(parseCtx *ctx) {
    if (ctx->my_id) {
        rm_free(ctx->my_id);
    }
}

MRClusterTopology *MR_ParseTopologyRequest(const char *c, size_t len, char **err)  {

    YY_BUFFER_STATE buf = yy_scan_bytes(c, len);

    void* pParser = MRTopologyRequest_ParseAlloc (rm_malloc);
    int t = 0;

    parseCtx ctx = {.topology = NULL, .ok = 1, .replication = 0, .my_id = NULL,
                    .errorMsg = NULL, .numSlots = 0, .shardFunc = MRHashFunc_None };

    while (ctx.ok && 0 != (t = yylex())) {
        MRTopologyRequest_Parse(pParser, t, tok, &ctx);
    }
    //if (ctx.ok) {
        MRTopologyRequest_Parse(pParser, 0, tok, &ctx);
    //}

    MRTopologyRequest_ParseFree(pParser, rm_free);

    if (err) {
        *err = ctx.errorMsg;
    }
    parseCtx_Free(&ctx);
    yy_delete_buffer(buf);

    return ctx.topology;
  }

}
