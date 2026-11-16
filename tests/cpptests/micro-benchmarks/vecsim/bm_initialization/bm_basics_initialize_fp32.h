/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 * SPDX-FileCopyrightText: Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
/**************************************
  Define and register tests
  NOTE: benchmarks' tests order can affect their results. Please add new benchmarks at the end of
the file.
***************************************/

// Memory BF
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, FLAT), fp32_index_t)
(benchmark::State &st) { Memory(st, INDEX_BF); }
BENCHMARK_REGISTER_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, FLAT))->Iterations(1);

// Memory HNSW
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, HNSW), fp32_index_t)
(benchmark::State &st) { Memory(st, INDEX_HNSW); }
BENCHMARK_REGISTER_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, HNSW))->Iterations(1);

// Memory Tiered
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, Tiered), fp32_index_t)
(benchmark::State &st) { Memory(st, INDEX_TIERED_HNSW); }
BENCHMARK_REGISTER_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, Tiered))->Iterations(1);

// AddLabel
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_ADD_LABEL, fp32_index_t)
(benchmark::State &st) { AddLabel(st); }
REGISTER_AddLabel(BM_ADD_LABEL, INDEX_BF);
REGISTER_AddLabel(BM_ADD_LABEL, INDEX_HNSW);

// DeleteLabel Registration. Definition is placed in the .cpp file.
REGISTER_DeleteLabel(BM_FUNC_NAME(DeleteLabel, BF));
REGISTER_DeleteLabel(BM_FUNC_NAME(DeleteLabel, HNSW));

// TopK BF
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(TopK, BF), fp32_index_t)
(benchmark::State &st) { TopK_BF(st); }
REGISTER_TopK_BF(BM_VecSimCommon, BM_FUNC_NAME(TopK, BF));

// TopK HNSW
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(TopK, HNSW), fp32_index_t)
(benchmark::State &st) { TopK_HNSW(st); }
REGISTER_TopK_HNSW(BM_VecSimCommon, BM_FUNC_NAME(TopK, HNSW));

// TopK Tiered HNSW
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(TopK, Tiered), fp32_index_t)
(benchmark::State &st) { TopK_Tiered(st); }
REGISTER_TopK_Tiered(BM_VecSimCommon, BM_FUNC_NAME(TopK, Tiered));

// Range BF
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_FUNC_NAME(Range, BF), fp32_index_t)
(benchmark::State &st) { Range_BF(st); }
REGISTER_Range_BF(BM_FUNC_NAME(Range, BF), fp32_index_t);

// Range HNSW
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_FUNC_NAME(Range, HNSW), fp32_index_t)
(benchmark::State &st) { Range_HNSW(st); }
REGISTER_Range_HNSW(BM_FUNC_NAME(Range, HNSW), fp32_index_t);

// Tiered HNSW add/delete benchmarks
REGISTER_AddLabel(BM_ADD_LABEL, INDEX_TIERED_HNSW);
REGISTER_DeleteLabel(BM_FUNC_NAME(DeleteLabel, Tiered));

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_ADD_LABEL_ASYNC, fp32_index_t)
(benchmark::State &st) { AddLabel_AsyncIngest(st); }
BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_ADD_LABEL_ASYNC)
    ->UNIT_AND_ITERATIONS->Arg(INDEX_TIERED_HNSW)
    ->ArgName("INDEX_TIERED_HNSW")
    ->UseRealTime();

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_DELETE_LABEL_ASYNC, fp32_index_t)
(benchmark::State &st) { DeleteLabel_AsyncRepair(st); }
BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_DELETE_LABEL_ASYNC)
    ->UNIT_AND_ITERATIONS->Arg(1)
    ->Arg(100)
    ->Arg(BM_VecSimGeneral::block_size)
    ->ArgName("SwapJobsThreshold")
    ->UseRealTime();

// RelabelLabel
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_RELABEL_LABEL, fp32_index_t)
(benchmark::State &st) { RelabelLabel(st); }
REGISTER_RelabelLabel(BM_RELABEL_LABEL, INDEX_BF);
REGISTER_RelabelLabel(BM_RELABEL_LABEL, INDEX_HNSW);
REGISTER_RelabelLabel(BM_RELABEL_LABEL, INDEX_TIERED_HNSW);

// SQ8 cases share one loaded backend, so the read-only cases run before the cases that
// modify it: they measure the loaded snapshot rather than a graph changed by repairs.
// Memory SQ8 Tiered
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, Tiered_SQ8), fp32_index_t)
(benchmark::State &st) { Memory(st, INDEX_TIERED_HNSW_SQ8); }
BENCHMARK_REGISTER_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, Tiered_SQ8))->Iterations(1);

// Memory SQ8 HNSW
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, HNSW_SQ8), fp32_index_t)
(benchmark::State &st) { Memory(st, INDEX_HNSW_SQ8); }
BENCHMARK_REGISTER_F(BM_VecSimCommon, BM_FUNC_NAME(Memory, HNSW_SQ8))->Iterations(1);

// TopK SQ8 Tiered
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(TopK, Tiered_SQ8), fp32_index_t)
(benchmark::State &st) { TopK_Tiered(st, 0, INDEX_TIERED_HNSW_SQ8); }
REGISTER_TopK_Tiered(BM_VecSimCommon, BM_FUNC_NAME(TopK, Tiered_SQ8));

// TopK SQ8 HNSW
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimCommon, BM_FUNC_NAME(TopK, HNSW_SQ8), fp32_index_t)
(benchmark::State &st) { TopK_HNSW(st, 0, INDEX_HNSW_SQ8); }
REGISTER_TopK_HNSW(BM_VecSimCommon, BM_FUNC_NAME(TopK, HNSW_SQ8));

// Range SQ8 Tiered
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_FUNC_NAME(Range, Tiered_SQ8), fp32_index_t)
(benchmark::State &st) { Range_HNSW(st, INDEX_TIERED_HNSW_SQ8); }
REGISTER_Range_HNSW(BM_FUNC_NAME(Range, Tiered_SQ8), fp32_index_t);

// AddLabel SQ8 HNSW
REGISTER_AddLabel(BM_ADD_LABEL, INDEX_HNSW_SQ8);

// SQ8 Tiered add/delete benchmarks
REGISTER_AddLabel(BM_ADD_LABEL, INDEX_TIERED_HNSW_SQ8);
REGISTER_DeleteLabel(BM_FUNC_NAME(DeleteLabel, Tiered_SQ8));

// SQ8 tiered async ingest and repair
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_FUNC_NAME(AddLabel_Async, Tiered_SQ8), fp32_index_t)
(benchmark::State &st) { AddLabel_AsyncIngest(st); }
BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC_NAME(AddLabel_Async, Tiered_SQ8))
    ->UNIT_AND_ITERATIONS->Arg(INDEX_TIERED_HNSW_SQ8)
    ->ArgName("INDEX_TIERED_HNSW_SQ8")
    ->UseRealTime();

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_FUNC_NAME(DeleteLabel_Async, Tiered_SQ8),
                            fp32_index_t)
(benchmark::State &st) { DeleteLabel_AsyncRepair(st, INDEX_TIERED_HNSW_SQ8); }
BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC_NAME(DeleteLabel_Async, Tiered_SQ8))
    ->UNIT_AND_ITERATIONS->Arg(1)
    ->Arg(100)
    ->Arg(BM_VecSimGeneral::block_size)
    ->ArgName("SwapJobsThreshold")
    ->UseRealTime();

// UpdateLabel HNSW and SQ8 HNSW. Re-insertion rebuilds graph links, so the run files register it
// after all other cases.
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_UPDATE_LABEL, fp32_index_t)
(benchmark::State &st) { UpdateLabel(st); }
