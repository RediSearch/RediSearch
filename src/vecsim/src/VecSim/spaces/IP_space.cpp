/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"
#include "VecSim/spaces/IP_space.h"
#include "VecSim/spaces/IP/IP.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include "VecSim/spaces/functions/AVX512F.h"
#include "VecSim/spaces/functions/F16C.h"
#include "VecSim/spaces/functions/AVX.h"
#include "VecSim/spaces/functions/SSE.h"
#include "VecSim/spaces/functions/AVX512BW_VBMI2.h"
#include "VecSim/spaces/functions/AVX512FP16_VL.h"
#include "VecSim/spaces/functions/AVX512BF16_VL.h"
#include "VecSim/spaces/functions/AVX512F_BW_VL_VNNI.h"
#include "VecSim/spaces/functions/AVX2.h"
#include "VecSim/spaces/functions/AVX2_F16C.h"
#include "VecSim/spaces/functions/AVX2_FMA.h"
#include "VecSim/spaces/functions/AVX2_FMA_F16C.h"
#include "VecSim/spaces/functions/SSE3.h"
#include "VecSim/spaces/functions/SSE4.h"
#include "VecSim/spaces/functions/SSE4_F16C.h"
#include "VecSim/spaces/functions/NEON.h"
#include "VecSim/spaces/functions/NEON_DOTPROD.h"
#include "VecSim/spaces/functions/NEON_HP.h"
#include "VecSim/spaces/functions/NEON_FHM.h"
#include "VecSim/spaces/functions/NEON_BF16.h"
#include "VecSim/spaces/functions/SVE.h"
#include "VecSim/spaces/functions/SVE_BF16.h"
#include "VecSim/spaces/functions/SVE2.h"

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;

namespace spaces {
// SQ8-FP32: asymmetric distance between SQ8 storage and FP32 query
dist_func_t<float> IP_SQ8_FP32_GetDistFunc(size_t dim, unsigned char *alignment,
                                           const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = SQ8_FP32_InnerProduct;
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);
#ifdef CPU_FEATURES_ARCH_AARCH64

#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_SQ8_FP32_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_SQ8_FP32_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd) {
        return Choose_SQ8_FP32_IP_implementation_NEON(dim);
    }
#endif

#endif

#ifdef CPU_FEATURES_ARCH_X86_64
    // Optimizations assume at least 8 elements (see the residual handling in the kernels).
    // Below that, the scalar implementation is at least as fast anyway.
    if (dim < 8) {
        return ret_dist_func;
    }
    // Alignment hints below refer to the SQ8 (first) operand per the GetDistFunc contract.
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        if (dim % 16 == 0) // SQ8 chunk = 16 bytes
            *alignment = 16 * sizeof(uint8_t);
        return Choose_SQ8_FP32_IP_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif
#ifdef OPT_AVX2_FMA
    if (features.avx2 && features.fma3) {
        if (dim % 8 == 0) // SQ8 chunk = 8 bytes
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP32_IP_implementation_AVX2_FMA(dim);
    }
#endif
#ifdef OPT_AVX2
    if (features.avx2) {
        if (dim % 8 == 0) // SQ8 chunk = 8 bytes
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP32_IP_implementation_AVX2(dim);
    }
#endif
#ifdef OPT_SSE4
    if (features.sse4_1) {
        if (dim % 4 == 0) // SQ8 chunk = 4 bytes
            *alignment = 4 * sizeof(uint8_t);
        return Choose_SQ8_FP32_IP_implementation_SSE4(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

// SQ8-FP32: asymmetric cosine distance between SQ8 storage and FP32 query
dist_func_t<float> Cosine_SQ8_FP32_GetDistFunc(size_t dim, unsigned char *alignment,
                                               const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = SQ8_FP32_Cosine;
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);
#ifdef CPU_FEATURES_ARCH_AARCH64

#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_SQ8_FP32_Cosine_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_SQ8_FP32_Cosine_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd) {
        return Choose_SQ8_FP32_Cosine_implementation_NEON(dim);
    }
#endif

#endif

#ifdef CPU_FEATURES_ARCH_X86_64
    // Optimizations assume at least 8 elements (see the residual handling in the kernels).
    // Below that, the scalar implementation is at least as fast anyway.
    if (dim < 8) {
        return ret_dist_func;
    }
    // Alignment hints below refer to the SQ8 (first) operand per the GetDistFunc contract.
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        if (dim % 16 == 0) // SQ8 chunk = 16 bytes
            *alignment = 16 * sizeof(uint8_t);
        return Choose_SQ8_FP32_Cosine_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif
#ifdef OPT_AVX2_FMA
    if (features.avx2 && features.fma3) {
        if (dim % 8 == 0) // SQ8 chunk = 8 bytes
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP32_Cosine_implementation_AVX2_FMA(dim);
    }
#endif
#ifdef OPT_AVX2
    if (features.avx2) {
        if (dim % 8 == 0) // SQ8 chunk = 8 bytes
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP32_Cosine_implementation_AVX2(dim);
    }
#endif
#ifdef OPT_SSE4
    if (features.sse4_1) {
        if (dim % 4 == 0) // SQ8 chunk = 4 bytes
            *alignment = 4 * sizeof(uint8_t);
        return Choose_SQ8_FP32_Cosine_implementation_SSE4(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

// SQ8-FP16: asymmetric inner product distance between SQ8 storage and FP16 query.
dist_func_t<float> IP_SQ8_FP16_GetDistFunc(size_t dim, unsigned char *alignment,
                                           const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = SQ8_FP16_InnerProduct;
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_X86_64
    if (dim < 16) {
        return ret_dist_func;
    }
    // Alignment hints below refer to the SQ8 (first) operand per the GetDistFunc contract.
    // AVX-512 tier only needs AVX-512F (cvtph_ps is part of AVX-512F, no VNNI/BW/VL required).
#ifdef OPT_AVX512F
    if (features.avx512f) {
        if (dim % 16 == 0) // SQ8 chunk = 16 bytes
            *alignment = 16 * sizeof(uint8_t);
        return Choose_SQ8_FP16_IP_implementation_AVX512F(dim);
    }
#endif
    // F16C is required by every non-AVX-512 SQ8↔FP16 tier (vcvtph2ps), so the guard is hoisted
    // around all three.
#ifdef OPT_F16C
#ifdef OPT_AVX2_FMA
    if (features.avx2 && features.fma3 && features.f16c) {
        if (dim % 8 == 0) // SQ8 chunk = 8 bytes
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP16_IP_implementation_AVX2_FMA(dim);
    }
#endif
#ifdef OPT_AVX2
    if (features.avx2 && features.f16c) {
        if (dim % 8 == 0)
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP16_IP_implementation_AVX2(dim);
    }
#endif
#ifdef OPT_SSE4
    // F16C is VEX-encoded — require AVX as well, matching the existing F16C/FP16 dispatcher.
    if (features.sse4_1 && features.f16c && features.avx) {
        if (dim % 4 == 0)
            *alignment = 4 * sizeof(uint8_t);
        return Choose_SQ8_FP16_IP_implementation_SSE4(dim);
    }
#endif
#endif // OPT_F16C
#endif // x86_64
#ifdef CPU_FEATURES_ARCH_AARCH64
    if (dim < 16) {
        return ret_dist_func;
    }
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_SQ8_FP16_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_SQ8_FP16_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_FHM
    if (features.asimdhp && features.asimdfhm) {
        return Choose_SQ8_FP16_IP_implementation_NEON_FHM(dim);
    }
#endif
#ifdef OPT_NEON_HP
    if (features.asimdhp) {
        return Choose_SQ8_FP16_IP_implementation_NEON_HP(dim);
    }
#endif
#endif // CPU_FEATURES_ARCH_AARCH64
    return ret_dist_func;
}

// SQ8-FP16: asymmetric cosine distance between SQ8 storage and FP16 query.
dist_func_t<float> Cosine_SQ8_FP16_GetDistFunc(size_t dim, unsigned char *alignment,
                                               const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = SQ8_FP16_Cosine;
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_X86_64
    if (dim < 16) {
        return ret_dist_func;
    }
#ifdef OPT_AVX512F
    if (features.avx512f) {
        if (dim % 16 == 0)
            *alignment = 16 * sizeof(uint8_t);
        return Choose_SQ8_FP16_Cosine_implementation_AVX512F(dim);
    }
#endif
#ifdef OPT_F16C
#ifdef OPT_AVX2_FMA
    if (features.avx2 && features.fma3 && features.f16c) {
        if (dim % 8 == 0)
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP16_Cosine_implementation_AVX2_FMA(dim);
    }
#endif
#ifdef OPT_AVX2
    if (features.avx2 && features.f16c) {
        if (dim % 8 == 0)
            *alignment = 8 * sizeof(uint8_t);
        return Choose_SQ8_FP16_Cosine_implementation_AVX2(dim);
    }
#endif
#ifdef OPT_SSE4
    if (features.sse4_1 && features.f16c && features.avx) {
        if (dim % 4 == 0)
            *alignment = 4 * sizeof(uint8_t);
        return Choose_SQ8_FP16_Cosine_implementation_SSE4(dim);
    }
#endif
#endif // OPT_F16C
#endif // x86_64
#ifdef CPU_FEATURES_ARCH_AARCH64
    if (dim < 16) {
        return ret_dist_func;
    }
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_SQ8_FP16_Cosine_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_SQ8_FP16_Cosine_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_FHM
    if (features.asimdhp && features.asimdfhm) {
        return Choose_SQ8_FP16_Cosine_implementation_NEON_FHM(dim);
    }
#endif
#ifdef OPT_NEON_HP
    if (features.asimdhp) {
        return Choose_SQ8_FP16_Cosine_implementation_NEON_HP(dim);
    }
#endif
#endif // CPU_FEATURES_ARCH_AARCH64
    return ret_dist_func;
}

// SQ8-to-SQ8 Inner Product distance function (both vectors are uint8 quantized with precomputed
// sum)
dist_func_t<float> IP_SQ8_SQ8_GetDistFunc(size_t dim, unsigned char *alignment,
                                          const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = SQ8_SQ8_InnerProduct;

    // The SQ8_SQ8 kernels call the shared UINT8_InnerProductImp directly, so they inherit its
    // 32-bit total and the same bound, and no uint8 chooser sees these calls. The scalar fallback
    // accumulates in float: imprecise past 2^24, but well defined, unlike a wrapped signed reduce.
    if (dim > spaces::UINT8_MAX_EXACT_SIMD_DIM) {
        return ret_dist_func;
    }
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_SQ8_SQ8_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_SQ8_SQ8_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (features.asimddp && dim >= 16) {
        return Choose_SQ8_SQ8_IP_implementation_NEON_DOTPROD(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd && dim >= 16) {
        return Choose_SQ8_SQ8_IP_implementation_NEON(dim);
    }
#endif
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
#ifdef OPT_AVX512_F_BW_VL_VNNI
    // AVX512 VNNI SQ8_SQ8 uses 64-element chunks; residual handling is in 32-byte sub-chunks.
    if (dim >= 64 && features.avx512f && features.avx512bw && features.avx512vl &&
        features.avx512vnni) {
        if (dim % 32 == 0) // align to 256 bits when there is no offsetting residual
            *alignment = 32 * sizeof(uint8_t);
        return Choose_SQ8_SQ8_IP_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

// SQ8-to-SQ8 Cosine distance function (both vectors are uint8 quantized with precomputed sum)
dist_func_t<float> Cosine_SQ8_SQ8_GetDistFunc(size_t dim, unsigned char *alignment,
                                              const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = SQ8_SQ8_Cosine;

    // Same bound as IP_SQ8_SQ8: these kernels reuse the shared UINT8_InnerProductImp.
    if (dim > spaces::UINT8_MAX_EXACT_SIMD_DIM) {
        return ret_dist_func;
    }
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_SQ8_SQ8_Cosine_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_SQ8_SQ8_Cosine_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (features.asimddp && dim >= 16) {
        return Choose_SQ8_SQ8_Cosine_implementation_NEON_DOTPROD(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd && dim >= 16) {
        return Choose_SQ8_SQ8_Cosine_implementation_NEON(dim);
    }
#endif
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
#ifdef OPT_AVX512_F_BW_VL_VNNI
    // AVX512 VNNI SQ8_SQ8 uses 64-element chunks; residual handling is in 32-byte sub-chunks.
    if (dim >= 64 && features.avx512f && features.avx512bw && features.avx512vl &&
        features.avx512vnni) {
        if (dim % 32 == 0) // align to 256 bits when there is no offsetting residual
            *alignment = 32 * sizeof(uint8_t);
        return Choose_SQ8_SQ8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

dist_func_t<float> IP_FP32_GetDistFunc(size_t dim, unsigned char *alignment, const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = FP32_InnerProduct;
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);
#ifdef CPU_FEATURES_ARCH_AARCH64

#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_FP32_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_FP32_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd) {
        return Choose_FP32_IP_implementation_NEON(dim);
    }
#endif

#endif

#ifdef CPU_FEATURES_ARCH_X86_64
    // Optimizations assume at least 8 floats (see the residual handling in the kernels).
    // Below that, the scalar implementation is at least as fast anyway.
    if (dim < 8) {
        return ret_dist_func;
    }
#ifdef OPT_AVX512F
    if (features.avx512f) {
        if (dim % 16 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 16 * sizeof(float); // handles 16 floats
        return Choose_FP32_IP_implementation_AVX512F(dim);
    }
#endif
#ifdef OPT_AVX
    if (features.avx) {
        if (dim % 8 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 8 * sizeof(float); // handles 8 floats
        return Choose_FP32_IP_implementation_AVX(dim);
    }
#endif
#ifdef OPT_SSE
    if (features.sse) {
        if (dim % 4 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 4 * sizeof(float); // handles 4 floats
        return Choose_FP32_IP_implementation_SSE(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

dist_func_t<double> IP_FP64_GetDistFunc(size_t dim, unsigned char *alignment,
                                        const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<double> ret_dist_func = FP64_InnerProduct;
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_FP64_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_FP64_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd) {
        return Choose_FP64_IP_implementation_NEON(dim);
    }
#endif

#endif

#ifdef CPU_FEATURES_ARCH_X86_64
    // Optimizations assume at least 4 doubles (see the residual handling in the kernels).
    // Below that, the scalar implementation is at least as fast anyway.
    if (dim < 4) {
        return ret_dist_func;
    }
#ifdef OPT_AVX512F
    if (features.avx512f) {
        if (dim % 8 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 8 * sizeof(double); // handles 8 doubles
        return Choose_FP64_IP_implementation_AVX512F(dim);
    }
#endif
#ifdef OPT_AVX
    if (features.avx) {
        if (dim % 4 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 4 * sizeof(double); // handles 4 doubles
        return Choose_FP64_IP_implementation_AVX(dim);
    }
#endif
#ifdef OPT_SSE
    if (features.sse) {
        if (dim % 2 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 2 * sizeof(double); // handles 2 doubles
        return Choose_FP64_IP_implementation_SSE(dim);
    }
#endif
#endif // __x86_64__ */
    return ret_dist_func;
}

dist_func_t<float> IP_BF16_GetDistFunc(size_t dim, unsigned char *alignment, const void *arch_opt) {
    unsigned char dummy_alignment;
    if (!alignment) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = BF16_InnerProduct_LittleEndian;
    if (!is_little_endian()) {
        return BF16_InnerProduct_BigEndian;
    }
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#if defined(CPU_FEATURES_ARCH_AARCH64)
#ifdef OPT_SVE_BF16
    if (features.svebf16) {
        return Choose_BF16_IP_implementation_SVE_BF16(dim);
    }
#endif
#ifdef OPT_NEON_BF16
    if (features.bf16 && dim >= 8) { // Optimization assumes at least 8 BF16s (full chunk)
        return Choose_BF16_IP_implementation_NEON_BF16(dim);
    }
#endif
#endif // AARCH64

#if defined(CPU_FEATURES_ARCH_X86_64)
    // Optimizations assume at least 32 bfloats. If we have less, we use the naive implementation.
    if (dim < 32) {
        return ret_dist_func;
    }

#ifdef OPT_AVX512_BF16_VL
    if (features.avx512_bf16 && features.avx512vl) {
        if (dim % 32 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 32 * sizeof(bfloat16); // align to 512 bits.
        return Choose_BF16_IP_implementation_AVX512BF16_VL(dim);
    }
#endif
#ifdef OPT_AVX512_BW_VBMI2
    if (features.avx512bw && features.avx512vbmi2) {
        if (dim % 32 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 32 * sizeof(bfloat16); // align to 512 bits.
        return Choose_BF16_IP_implementation_AVX512BW_VBMI2(dim);
    }
#endif
#ifdef OPT_AVX2
    if (features.avx2) {
        if (dim % 16 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 16 * sizeof(bfloat16); // align to 256 bits.
        return Choose_BF16_IP_implementation_AVX2(dim);
    }
#endif
#ifdef OPT_SSE3
    if (features.sse3) {
        if (dim % 8 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 8 * sizeof(bfloat16); // align to 128 bits.
        return Choose_BF16_IP_implementation_SSE3(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

dist_func_t<float> IP_FP16_GetDistFunc(size_t dim, unsigned char *alignment, const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }
    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

    dist_func_t<float> ret_dist_func = FP16_InnerProduct;

#if defined(CPU_FEATURES_ARCH_AARCH64)
#ifdef OPT_SVE2
    if (dim <= spaces::FP16_MAX_UNIT_IP_SIMD_DIM && features.sve2) {
        return Choose_FP16_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (dim <= spaces::FP16_MAX_UNIT_IP_SIMD_DIM && features.sve) {
        return Choose_FP16_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_HP
    if (dim <= spaces::FP16_MAX_UNIT_IP_SIMD_DIM && features.asimdhp &&
        dim >= 8) { // Optimization assumes at least 8 16FPs (full chunk)
        return Choose_FP16_IP_implementation_NEON_HP(dim);
    }
#endif
#endif

#if defined(CPU_FEATURES_ARCH_X86_64)
    // Each tier has a minimal dimension implied by its residual handling: the AVX512FP16_VL
    // kernel loads full 512-bit blocks (32 elements), the AVX512F kernel loads full 256-bit
    // blocks (16 elements), and the F16C kernel loads full 128-bit blocks (8 elements).
#ifdef OPT_AVX512_FP16_VL
    // More details about the dimension limitation can be found in this PR's description:
    // https://github.com/RedisAI/VectorSimilarity/pull/477
    if (dim >= 32 && dim <= spaces::FP16_MAX_UNIT_IP_SIMD_DIM && features.avx512_fp16 &&
        features.avx512vl) {
        if (dim % 32 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 32 * sizeof(float16); // handles 32 floats
        return Choose_FP16_IP_implementation_AVX512FP16_VL(dim);
    }
#endif
#ifdef OPT_AVX512F
    if (dim >= 16 && features.avx512f) {
        if (dim % 32 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 32 * sizeof(float16); // handles 32 floats
        return Choose_FP16_IP_implementation_AVX512F(dim);
    }
#endif
#ifdef OPT_F16C
    if (dim >= 8 && features.f16c && features.fma3 && features.avx) {
        if (dim % 16 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 16 * sizeof(float16); // handles 16 floats
        return Choose_FP16_IP_implementation_F16C(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

dist_func_t<float> IP_INT8_GetDistFunc(size_t dim, unsigned char *alignment, const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = INT8_InnerProduct;

    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_INT8_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_INT8_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_DOTPROD // Should be the first check, as it is the most optimized
    if (features.asimddp && dim >= 16) {
        return Choose_INT8_IP_implementation_NEON_DOTPROD(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd && dim >= 16) {
        return Choose_INT8_IP_implementation_NEON(dim);
    }
#endif
#endif
#ifdef CPU_FEATURES_ARCH_X86_64
    // Optimizations assume at least 32 int8. If we have less, we use the naive implementation.
    if (dim < 32) {
        return ret_dist_func;
    }

#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        if (dim % 32 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 32 * sizeof(int8_t); // align to 256 bits.
        return Choose_INT8_IP_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

dist_func_t<float> Cosine_INT8_GetDistFunc(size_t dim, unsigned char *alignment,
                                           const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = INT8_Cosine;

    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_INT8_Cosine_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_INT8_Cosine_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (features.asimddp && dim >= 16) {
        return Choose_INT8_Cosine_implementation_NEON_DOTPROD(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd && dim >= 16) {
        return Choose_INT8_Cosine_implementation_NEON(dim);
    }
#endif
#endif
#ifdef CPU_FEATURES_ARCH_X86_64
    if (dim < 32) {
        return ret_dist_func;
    }
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        // For int8 vectors with cosine distance, the extra float for the norm shifts alignment to
        // `(dim + sizeof(float)) % 32`.
        // Vectors satisfying this have a residual, causing offset loads during calculation.
        // To avoid complexity, we skip alignment here, assuming the performance impact is
        // negligible.
        return Choose_INT8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif

#endif // __x86_64__
    return ret_dist_func;
}

dist_func_t<float> IP_UINT8_GetDistFunc(size_t dim, unsigned char *alignment,
                                        const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = UINT8_InnerProduct;

    // Above this dimension the SIMD kernels' 32-bit total is no longer exact, so hand back the
    // scalar kernel, which accumulates into a 64-bit ret_t. Decided here, once per index, so no
    // distance computation pays for the check. See spaces.h for how the bound is derived.
    if (dim > spaces::UINT8_MAX_EXACT_SIMD_DIM) {
        return ret_dist_func;
    }

    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_UINT8_IP_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_UINT8_IP_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (features.asimddp && dim >= 16) {
        return Choose_UINT8_IP_implementation_NEON_DOTPROD(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd && dim >= 16) {
        return Choose_UINT8_IP_implementation_NEON(dim);
    }
#endif
#endif
#ifdef CPU_FEATURES_ARCH_X86_64
    if (dim < 32) {
        return ret_dist_func;
    }
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        if (dim % 32 == 0) // no point in aligning if we have an offsetting residual
            *alignment = 32 * sizeof(uint8_t); // align to 256 bits.
        return Choose_UINT8_IP_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

dist_func_t<float> Cosine_UINT8_GetDistFunc(size_t dim, unsigned char *alignment,
                                            const void *arch_opt) {
    unsigned char dummy_alignment;
    if (alignment == nullptr) {
        alignment = &dummy_alignment;
    }

    dist_func_t<float> ret_dist_func = UINT8_Cosine;

    // Above this dimension the SIMD kernels' 32-bit total is no longer exact, so hand back the
    // scalar kernel, which accumulates into a 64-bit ret_t. Decided here, once per index, so no
    // distance computation pays for the check. See spaces.h for how the bound is derived.
    if (dim > spaces::UINT8_MAX_EXACT_SIMD_DIM) {
        return ret_dist_func;
    }

    [[maybe_unused]] auto features = getCpuOptimizationFeatures(arch_opt);

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        return Choose_UINT8_Cosine_implementation_SVE2(dim);
    }
#endif
#ifdef OPT_SVE
    if (features.sve) {
        return Choose_UINT8_Cosine_implementation_SVE(dim);
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (features.asimddp && dim >= 16) {
        return Choose_UINT8_Cosine_implementation_NEON_DOTPROD(dim);
    }
#endif
#ifdef OPT_NEON
    if (features.asimd && dim >= 16) {
        return Choose_UINT8_Cosine_implementation_NEON(dim);
    }
#endif
#endif
#ifdef CPU_FEATURES_ARCH_X86_64
    if (dim < 32) {
        return ret_dist_func;
    }
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        // For uint8 vectors with cosine distance, the extra float for the norm shifts alignment to
        // `(dim + sizeof(float)) % 32`.
        // Vectors satisfying this have a residual, causing offset loads during calculation.
        // To avoid complexity, we skip alignment here, assuming the performance impact is
        // negligible.
        return Choose_UINT8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim);
    }
#endif
#endif // __x86_64__
    return ret_dist_func;
}

} // namespace spaces
