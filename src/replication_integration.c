/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * Integration example showing how to use the replication strategy functions
 * with the existing fork GC system and RDB operations.
 */

#include "spec.h"  // Contains replication functions
#include "fork_gc.h"
#include "module.h"
#include <unistd.h>
#include <sys/wait.h>

//---------------------------------------------------------------------------------------------
// Integration with Fork GC System
//---------------------------------------------------------------------------------------------

/**
 * Modified version of FGC_periodicCallback that integrates replication events
 * This shows how the replication functions would be called in the existing fork GC flow
 */
int FGC_periodicCallback_WithReplication(RedisModuleCtx *ctx, void *privdata) {
    ForkGC *gc = privdata;

    RedisModule_Log(ctx, "debug", "ForkGC: Starting periodic callback with replication support");

    // STEP 1: PRE-FORK PREPARATION
    // Call replication preparation before any fork operations
    int ret = RediSearch_Freeze();
    if (ret != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "ForkGC: Failed to prepare for fork, aborting GC cycle");
        return ret;
    }

    // STEP 2: EXISTING FORK GC LOGIC (simplified)
    // This would be the existing fork logic from fork_gc.c
    pid_t cpid;

    // Create pipe for communication
    int pipefd[2];
    if (pipe(pipefd) == -1) {
        RedisModule_Log(ctx, "warning", "ForkGC: Failed to create pipe");
        RediSearch_RollbackForkPreparation();
        return REDISMODULE_ERR;
    }

    // Acquire GIL and fork
    RedisModule_ThreadSafeContextLock(ctx);
    cpid = RedisModule_Fork(NULL, NULL);

    if (cpid == -1) {
        // Fork failed
        RedisModule_Log(ctx, "warning", "ForkGC: Fork failed");
        RedisModule_ThreadSafeContextUnlock(ctx);
        close(pipefd[0]);
        close(pipefd[1]);
        RediSearch_RollbackForkPreparation();
        return REDISMODULE_ERR;
    }

    if (cpid == 0) {
        // CHILD PROCESS
        RedisModule_ThreadSafeContextUnlock(ctx);
        close(pipefd[0]); // Close read end

        // STEP 3: POST-FORK NOTIFICATION (CHILD)
        ret = RediSearch_Unfreeze();
        if (ret != REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "ForkGC Child: Failed to handle fork creation");
            close(pipefd[1]);
            RedisModule_ExitFromChild(EXIT_FAILURE);
        }

        // Child GC work would happen here...
        RedisModule_Log(ctx, "debug", "ForkGC Child: Performing GC work");

        // Simulate some GC work
        usleep(1000); // 1ms of work

        close(pipefd[1]);
        RedisModule_ExitFromChild(EXIT_SUCCESS);

    } else {
        // PARENT PROCESS
        RedisModule_ThreadSafeContextUnlock(ctx);
        close(pipefd[1]); // Close write end

        // STEP 4: POST-FORK NOTIFICATION (PARENT)
        ret = RediSearch_Unfreeze();
        if (ret != REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "ForkGC Parent: Failed to handle fork creation");
            close(pipefd[0]);
            return ret;
        }

        // Wait for child and read results
        RedisModule_Log(ctx, "debug", "ForkGC Parent: Waiting for child to complete");

        // Simulate waiting for child
        int status;
        waitpid(cpid, &status, 0);

        close(pipefd[0]);

        // STEP 5: FORK COMPLETION
        ret = RediSearch_Unfreeze_Expensive_Writes();
        if (ret != REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "ForkGC Parent: Failed to complete fork");
            return ret;
        }

        RedisModule_Log(ctx, "debug", "ForkGC: Completed periodic callback with replication support");
    }

    return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------
// Integration with RDB Operations
//---------------------------------------------------------------------------------------------

/**
 * Modified RDB save function that integrates replication events
 * This shows how replication functions would be called during RDB save operations
 */
void Indexes_RdbSave_WithReplication(RedisModuleIO *rdb, int when) {
    RedisModule_Log(RSDummyContext, "debug", "RDB Save: Starting with replication support");

    // STEP 1: Prepare for RDB save (similar to fork preparation)
    int ret = RediSearch_Freeze();
    if (ret != REDISMODULE_OK) {
        RedisModule_Log(RSDummyContext, "warning", "RDB Save: Failed to prepare for save");
        return;
    }

    // STEP 2: Perform the actual RDB save
    // This would call the existing Indexes_RdbSave function
    RedisModule_Log(RSDummyContext, "debug", "RDB Save: Performing actual save");

    if (!specDict_g || dictSize(specDict_g) == 0) {
        RedisModule_Log(RSDummyContext, "debug", "RDB Save: No specs to save");
        RediSearch_Unfreeze_Expensive_Writes();
        return;
    }

    // Save number of indexes
    RedisModule_SaveUnsigned(rdb, dictSize(specDict_g));

    // Save each index
    dictIterator *iter = dictGetIterator(specDict_g);
    dictEntry *entry = NULL;

    while ((entry = dictNext(iter))) {
        StrongRef spec_ref = dictGetRef(entry);
        IndexSpec *sp = StrongRef_Get(spec_ref);

        if (!sp) {
            continue;
        }

        RedisModule_Log(RSDummyContext, "debug",
                      "RDB Save: Saving spec '%s'",
                      HiddenString_GetUnsafe(sp->specName, NULL));

        // Save spec data (simplified)
        HiddenString_SaveToRdb(sp->specName, rdb);
        RedisModule_SaveUnsigned(rdb, (uint64_t)sp->flags);
        RedisModule_SaveUnsigned(rdb, sp->numFields);

        // Save fields would happen here...
    }

    dictReleaseIterator(iter);

    // STEP 3: Complete the save operation
    ret = RediSearch_Unfreeze_Expensive_Writes();
    if (ret != REDISMODULE_OK) {
        RedisModule_Log(RSDummyContext, "warning", "RDB Save: Failed to complete save");
    }

    RedisModule_Log(RSDummyContext, "debug", "RDB Save: Completed with replication support");
}

//---------------------------------------------------------------------------------------------
// Usage Examples and Test Functions
//---------------------------------------------------------------------------------------------

/**
 * Example function showing how to manually trigger replication events
 * This could be used for testing or manual replication scenarios
 */
int RediSearch_ManualReplicationTest(void) {
    RedisModule_Log(RSDummyContext, "notice", "Manual Replication Test: Starting");

    // Test the full replication sequence
    int ret;

    // Step 1: Prepare for fork
    ret = RediSearch_Freeze();
    if (ret != REDISMODULE_OK) {
        RedisModule_Log(RSDummyContext, "warning", "Manual Test: Prepare failed");
        return ret;
    }

    // Step 2: Simulate fork creation
    ret = RediSearch_Unfreeze();
    if (ret != REDISMODULE_OK) {
        RedisModule_Log(RSDummyContext, "warning", "Manual Test: Fork creation failed");
        return ret;
    }

    // Step 3: Simulate some work
    usleep(1000); // 1ms

    // Step 4: Complete fork
    ret = RediSearch_Unfreeze_Expensive_Writes();
    if (ret != REDISMODULE_OK) {
        RedisModule_Log(RSDummyContext, "warning", "Manual Test: Fork completion failed");
        return ret;
    }

    RedisModule_Log(RSDummyContext, "notice", "Manual Replication Test: Completed successfully");
    return REDISMODULE_OK;
}

/**
 * Function to register replication integration with existing systems
 * This would be called during module initialization
 */
int RediSearch_InitializeReplicationIntegration(RedisModuleCtx *ctx) {
    RedisModule_Log(ctx, "notice", "Initializing RediSearch replication integration");

    // Here you would register the replication-aware callbacks
    // with the existing fork GC and RDB systems

    // For example:
    // - Replace the existing FGC periodic callback with FGC_periodicCallback_WithReplication
    // - Replace RDB save functions with replication-aware versions
    // - Set up any additional replication-specific timers or callbacks

    RedisModule_Log(ctx, "notice", "RediSearch replication integration initialized");
    return REDISMODULE_OK;
}
