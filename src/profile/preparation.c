/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "preparation.h"
#include "rmalloc.h"
#include <pthread.h>

typedef enum {
  PROFILE_QUEUED,
  PROFILE_PREPARING,
  PROFILE_EXECUTING,
  PROFILE_PREPARATION_TIMED_OUT,
} ProfilePreparationState;

struct ProfilePreparation {
  pthread_mutex_t lock;
  pthread_cond_t ready;
  ProfilePreparationState state;
  bool published;
  bool pipelineReady;
};

ProfilePreparation* ProfilePreparation_New(void) {
  ProfilePreparation* preparation = rm_calloc(1, sizeof(*preparation));
  pthread_mutex_init(&preparation->lock, NULL);
  pthread_cond_init(&preparation->ready, NULL);
  preparation->state = PROFILE_QUEUED;
  return preparation;
}

void ProfilePreparation_Free(ProfilePreparation* preparation) {
  if (!preparation) return;
  pthread_mutex_destroy(&preparation->lock);
  pthread_cond_destroy(&preparation->ready);
  rm_free(preparation);
}

static bool transition(ProfilePreparation* preparation, ProfilePreparationState from,
                       ProfilePreparationState to) {
  pthread_mutex_lock(&preparation->lock);
  bool acquired = preparation->state == from;
  if (acquired) preparation->state = to;
  pthread_mutex_unlock(&preparation->lock);
  return acquired;
}

bool ProfilePreparation_TryStart(ProfilePreparation* preparation) {
  return transition(preparation, PROFILE_QUEUED, PROFILE_PREPARING);
}

bool ProfilePreparation_TryExecute(ProfilePreparation* preparation) {
  return transition(preparation, PROFILE_PREPARING, PROFILE_EXECUTING);
}

void ProfilePreparation_Publish(ProfilePreparation* preparation, bool pipelineReady) {
  pthread_mutex_lock(&preparation->lock);
  preparation->pipelineReady = pipelineReady;
  preparation->published = true;
  pthread_cond_signal(&preparation->ready);
  pthread_mutex_unlock(&preparation->lock);
}

bool ProfilePreparation_ClaimTimeout(ProfilePreparation* preparation, bool* pipelineReady) {
  pthread_mutex_lock(&preparation->lock);
  if (preparation->state == PROFILE_EXECUTING) {
    pthread_mutex_unlock(&preparation->lock);
    return false;
  }
  // A queued worker must win TryStart before touching any profile fields.
  bool queued = preparation->state == PROFILE_QUEUED;
  preparation->state = PROFILE_PREPARATION_TIMED_OUT;
  while (!queued && !preparation->published) {
    pthread_cond_wait(&preparation->ready, &preparation->lock);
  }
  *pipelineReady = preparation->pipelineReady;
  pthread_mutex_unlock(&preparation->lock);
  return true;
}
