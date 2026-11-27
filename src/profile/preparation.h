/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef RS_PROFILE_PREPARATION_H__
#define RS_PROFILE_PREPARATION_H__

#include <stdbool.h>

typedef struct ProfilePreparation ProfilePreparation;

ProfilePreparation* ProfilePreparation_New(void);
void ProfilePreparation_Free(ProfilePreparation* preparation);

bool ProfilePreparation_TryStart(ProfilePreparation* preparation);
bool ProfilePreparation_TryExecute(ProfilePreparation* preparation);

// Publish only after the worker's last access to the request's profiling state.
void ProfilePreparation_Publish(ProfilePreparation* preparation, bool pipelineReady);

// If execution has not started, claim the reply and wait for any active preparation.
// The caller must retain the request until it finishes printing the profile.
bool ProfilePreparation_ClaimTimeout(ProfilePreparation* preparation, bool* pipelineReady);

#endif
