/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
#include "VecSim/algorithms/svs/svs_utils.h"
#include "svs/extensions/vamana/scalar.h"
// Tells SQDataset that the concurrent SegmentedBlocked tag is a blocked allocator, so
// that resize()/compact() are not constrained away. Without it a scalar-quantized
// concurrent index fails to compile the moment the index grows.
#include "svs/concurrent/extensions/scalar.h"

#if HAVE_SVS_LVQ
#include SVS_LVQ_HEADER
#include SVS_LEANVEC_HEADER
// The same treatment for the LVQ and LeanVec datasets, each of which keeps its own private
// copy of the blocked-allocator trait. LeanVec additionally picks the allocator for its inner
// datasets through that trait, and its blocked branch hardcodes svs::data::Blocked -- so
// without this the inner storage would quietly lose the grow-stable guarantee.
#include "svs/concurrent/extensions/lvq.h"
#include "svs/concurrent/extensions/leanvec.h"
#endif // HAVE_SVS_LVQ

// Scalar Quantization traits for SVS
template <typename DataType>
struct SVSStorageTraits<DataType, 1, 0, false> {
    using element_type = std::int8_t;
    using allocator_type = svs_details::SVSAllocator<element_type>;
    using blocked_type = svs::concurrent::SegmentedBlocked<svs::AllocatorHandle<element_type>>;
    using index_storage_type =
        svs::quantization::scalar::SQDataset<element_type, svs::Dynamic, blocked_type>;

    static constexpr bool is_compressed() { return true; }

    static auto make_blocked_allocator(size_t block_size, size_t dim,
                                       std::shared_ptr<VecSimAllocator> allocator) {
        // SVS block size is a power of two, so we can use it directly
        auto svs_bs = svs_details::SVSBlockSize(block_size, element_size(dim));
        allocator_type data_allocator{std::move(allocator)};
        return svs_details::make_segmented_blocked_allocator_handle({svs_bs}, data_allocator);
    }

    static constexpr VecSimSvsQuantBits get_compression_mode() { return VecSimSvsQuant_Scalar; }

    template <svs::data::ImmutableMemoryDataset Dataset, svs::threads::ThreadPool Pool>
    static index_storage_type create_storage(const Dataset &data, size_t block_size, Pool &pool,
                                             std::shared_ptr<VecSimAllocator> allocator,
                                             size_t /*leanvec_dim*/) {
        const auto dim = data.dimensions();
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        return index_storage_type::compress(data, pool, blocked_alloc);
    }

    static index_storage_type load(const svs::lib::LoadTable &table, size_t block_size, size_t dim,
                                   std::shared_ptr<VecSimAllocator> allocator) {
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        return index_storage_type::load(table, blocked_alloc);
    }

    static index_storage_type load(const std::string &path, size_t block_size, size_t dim,
                                   std::shared_ptr<VecSimAllocator> allocator) {
        assert(svs::data::detail::is_likely_reload(path)); // TODO implement auto_load for SQDataset
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        // Load the data from disk
        return svs::lib::load_from_disk<index_storage_type>(path, blocked_alloc);
    }

    static constexpr size_t element_size(size_t dims, size_t alignment = 0,
                                         size_t /*leanvec_dim*/ = 0) {
        return dims * sizeof(element_type);
    }

    static size_t storage_capacity(const index_storage_type &storage) {
        // SQDataset does not provide a capacity method
        return storage.size();
    }
};

#if HAVE_SVS_LVQ
namespace svs_details {
template <size_t Primary>
struct LVQSelector {
    using strategy = svs::quantization::lvq::Sequential;
};

template <>
struct LVQSelector<4> {
    using strategy = svs::quantization::lvq::Turbo<16, 8>;
};
} // namespace svs_details

// LVQDataset traits for SVS
template <typename DataType, size_t QuantBits, size_t ResidualBits>
struct SVSStorageTraits<DataType, QuantBits, ResidualBits, false,
                        std::enable_if_t<(QuantBits > 1)>> {
    using allocator_type = svs_details::SVSAllocator<std::byte>;
    using blocked_type = svs::concurrent::SegmentedBlocked<svs::AllocatorHandle<std::byte>>;
    using strategy_type = typename svs_details::LVQSelector<QuantBits>::strategy;
    using index_storage_type =
        svs::quantization::lvq::LVQDataset<QuantBits, ResidualBits, svs::Dynamic, strategy_type,
                                           blocked_type>;

    static constexpr bool is_compressed() { return true; }

    static constexpr VecSimSvsQuantBits get_compression_mode() {
        if constexpr (QuantBits == 4 && ResidualBits == 0) {
            return VecSimSvsQuant_4;
        } else if constexpr (QuantBits == 8 && ResidualBits == 0) {
            return VecSimSvsQuant_8;
        } else if constexpr (QuantBits == 4 && ResidualBits == 4) {
            return VecSimSvsQuant_4x4;
        } else if constexpr (QuantBits == 4 && ResidualBits == 8) {
            return VecSimSvsQuant_4x8;
        } else {
            assert(false && "Unsupported quantization mode");
            return VecSimSvsQuant_NONE; // Unsupported case
        }
    }

    static auto make_blocked_allocator(size_t block_size, size_t dim,
                                       std::shared_ptr<VecSimAllocator> allocator) {
        // SVS block size is a power of two, so we can use it directly
        auto elem_size = std::max(primary_element_size(dim), residual_element_size(dim));
        auto svs_bs = svs_details::SVSBlockSize(block_size, elem_size);
        allocator_type data_allocator{std::move(allocator)};
        return svs_details::make_segmented_blocked_allocator_handle({svs_bs}, data_allocator);
    }

    template <svs::data::ImmutableMemoryDataset Dataset, svs::threads::ThreadPool Pool>
    static index_storage_type create_storage(const Dataset &data, size_t block_size, Pool &pool,
                                             std::shared_ptr<VecSimAllocator> allocator,
                                             size_t /*leanvec_dim*/) {
        const auto dim = data.dimensions();

        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        return index_storage_type::compress(data, pool, 0, blocked_alloc);
    }

    static index_storage_type load(const svs::lib::LoadTable &table, size_t block_size, size_t dim,
                                   std::shared_ptr<VecSimAllocator> allocator) {
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        return index_storage_type::load(table, /*alignment=*/0, blocked_alloc);
    }

    static index_storage_type load(const std::string &path, size_t block_size, size_t dim,
                                   std::shared_ptr<VecSimAllocator> allocator) {
        assert(svs::data::detail::is_likely_reload(path)); // TODO implement auto_load for LVQ
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        // Load the data from disk
        return svs::lib::load_from_disk<index_storage_type>(path, /*alignment=*/0, blocked_alloc);
    }

    static constexpr size_t primary_element_size(size_t dims, size_t alignment = 0,
                                                 size_t /*leanvec_dim*/ = 0) {
        using primary_type = typename index_storage_type::primary_type;
        using layout_type = typename primary_type::helper_type;
        using layout_dims_type = svs::lib::MaybeStatic<index_storage_type::extent>;
        const auto layout_dims = layout_dims_type{dims};
        return primary_type::compute_data_dimensions(layout_type{layout_dims}, alignment);
    }

    static constexpr size_t residual_element_size(size_t dims, size_t alignment = 0) {
        if constexpr (ResidualBits == 0) {
            return 0;
        } else {
            using residual_type = typename index_storage_type::residual_type;
            using dims_type = svs::lib::MaybeStatic<index_storage_type::extent>;
            auto residual_dims = dims_type{dims};
            return residual_type::total_bytes(residual_dims);
        }
    }

    static constexpr size_t element_size(size_t dims, size_t alignment = 0,
                                         size_t leanvec_dim = 0) {
        return primary_element_size(dims, alignment, leanvec_dim) +
               residual_element_size(dims, alignment);
    }

    static size_t storage_capacity(const index_storage_type &storage) {
        // LVQDataset does not provide a capacity method
        return storage.size();
    }
};

// LeanVec dataset traits for SVS
template <typename DataType, size_t QuantBits, size_t ResidualBits>
struct SVSStorageTraits<DataType, QuantBits, ResidualBits, true> {
    using allocator_type = svs_details::SVSAllocator<std::byte>;
    using blocked_type = svs::concurrent::SegmentedBlocked<svs::AllocatorHandle<std::byte>>;
    using index_storage_type = svs::leanvec::LeanDataset<svs::leanvec::UsingLVQ<QuantBits>,
                                                         svs::leanvec::UsingLVQ<ResidualBits>,
                                                         svs::Dynamic, svs::Dynamic, blocked_type>;

    static size_t check_leanvec_dim(size_t dims, size_t leanvec_dim) {
        if (leanvec_dim == 0) {
            return dims / 2; /* default LeanVec dimension */
        }
        return leanvec_dim;
    }

    static constexpr bool is_compressed() { return true; }

    static constexpr auto get_compression_mode() {
        if constexpr (QuantBits == 4 && ResidualBits == 8) {
            return VecSimSvsQuant_4x8_LeanVec;
        } else if constexpr (QuantBits == 8 && ResidualBits == 8) {
            return VecSimSvsQuant_8x8_LeanVec;
        } else {
            assert(false && "Unsupported quantization mode");
            return VecSimSvsQuant_NONE; // Unsupported case
        }
    }

    static auto make_blocked_allocator(size_t block_size, size_t dim,
                                       std::shared_ptr<VecSimAllocator> allocator) {
        // SVS block size is a power of two, so we can use it directly
        auto elem_size = std::max(primary_element_size(dim), secondary_element_size(dim));
        auto svs_bs = svs_details::SVSBlockSize(block_size, elem_size);
        allocator_type data_allocator{std::move(allocator)};
        return svs_details::make_segmented_blocked_allocator_handle({svs_bs}, data_allocator);
    }

    template <svs::data::ImmutableMemoryDataset Dataset, svs::threads::ThreadPool Pool>
    static index_storage_type create_storage(const Dataset &data, size_t block_size, Pool &pool,
                                             std::shared_ptr<VecSimAllocator> allocator,
                                             size_t leanvec_dim) {
        const auto dim = data.dimensions();
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));

        return index_storage_type::reduce(
            data, std::nullopt, pool, 0,
            svs::lib::MaybeStatic<svs::Dynamic>(check_leanvec_dim(dim, leanvec_dim)),
            blocked_alloc);
    }

    static index_storage_type load(const svs::lib::LoadTable &table, size_t block_size, size_t dim,
                                   std::shared_ptr<VecSimAllocator> allocator) {
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        return index_storage_type::load(table, /*alignment=*/0, blocked_alloc);
    }

    static index_storage_type load(const std::string &path, size_t block_size, size_t dim,
                                   std::shared_ptr<VecSimAllocator> allocator) {
        assert(svs::data::detail::is_likely_reload(path)); // TODO implement auto_load for LeanVec
        auto blocked_alloc = make_blocked_allocator(block_size, dim, std::move(allocator));
        // Load the data from disk
        return svs::lib::load_from_disk<index_storage_type>(path, /*alignment=*/0, blocked_alloc);
    }

    static constexpr size_t primary_element_size(size_t dims, size_t alignment = 0,
                                                 size_t leanvec_dim = 0) {
        return SVSStorageTraits<DataType, QuantBits, 0, false>::element_size(
            check_leanvec_dim(dims, leanvec_dim), alignment);
    }

    static constexpr size_t secondary_element_size(size_t dims, size_t alignment = 0) {
        return SVSStorageTraits<DataType, ResidualBits, 0, false>::element_size(dims, alignment);
    }

    static constexpr size_t element_size(size_t dims, size_t alignment = 0,
                                         size_t leanvec_dim = 0) {
        return primary_element_size(dims, alignment, leanvec_dim) +
               secondary_element_size(dims, alignment);
    }

    static size_t storage_capacity(const index_storage_type &storage) {
        // LeanDataset does not provide a capacity method
        return storage.size();
    }
};
#else
#pragma message "SVS LVQ is not available"
#endif // HAVE_SVS_LVQ
