/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/utils/alignment.h"

#include "cpu_features_macros.h"
#ifdef CPU_FEATURES_ARCH_X86_64
#include "cpuinfo_x86.h"
#endif // CPU_FEATURES_ARCH_X86_64
#ifdef CPU_FEATURES_ARCH_AARCH64
#include "cpuinfo_aarch64.h"
#endif // CPU_FEATURES_ARCH_AARCH64

#if defined(__AVX512F__) || defined(__AVX__) || defined(__SSE__)
#if defined(__GNUC__)
#include <x86intrin.h>
// Override missing implementations in GCC < 11
// Full list and suggested alternatives for each missing function can be found here:
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95483
#if (__GNUC__ < 11)
#define _mm256_loadu_epi8(ptr) _mm256_maskz_loadu_epi8(~0, ptr)
#define _mm512_loadu_epi8(ptr) _mm512_maskz_loadu_epi8(~0, ptr)
#endif
#elif defined(__clang__)
#include <xmmintrin.h>
#elif defined(_MSC_VER)
#include <intrin.h>
#include <stdexcept>
#endif

#endif // __AVX512F__ || __AVX__ || __SSE__
