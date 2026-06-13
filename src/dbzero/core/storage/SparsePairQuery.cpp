// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SparsePairQuery.hpp"
#include <cassert>

namespace db0

{

    template <bool read_only>
    SparsePairQuery<read_only>::SparsePairQuery(const StorageOptions &options, std::uint32_t page_size,
        std::uint64_t begin_page_num, std::uint64_t end_page_num,
        SparsePairManager &sparse_pair_manager)
        : m_options(options)
        , m_page_size(page_size)
        , m_page_num(begin_page_num)
        , m_end_page_num(end_page_num)
        , m_sparse_pair_manager(sparse_pair_manager)
        , m_use_bucket_mapping(end_page_num - begin_page_num >= 2 && !!m_options.m_storage_slab_bucket)
    {
        if (m_use_bucket_mapping) {
            if constexpr (read_only) {
                initSparsePair(begin_page_num);
            } else {
                initOrCreateSparsePair(begin_page_num);
            }
        }
    }

    template <bool read_only>
    SparsePairQuery<read_only> &SparsePairQuery<read_only>::operator++()
    {
        assert(hasNext() && "SparsePairQuery page range exhausted");
        ++m_page_num;
        return *this;
    }

    template <bool read_only>
    Allocator::SlotId SparsePairQuery<read_only>::slotId() const
    {
        assert(m_slot_initialized && "SparsePairQuery slot requested before current lookup");
        assert((!m_use_bucket_mapping || m_page_num < m_bucket_end_page_num)
            && "SparsePairQuery slot requested past current bucket");
        return m_slot_id;
    }

    template <bool read_only>
    PlainSparsePair *SparsePairQuery<read_only>::currentSparsePair()
    {
        if (!m_use_bucket_mapping) {
            m_slot_id = getMetaSlotId(m_page_num);
            m_slot_initialized = true;
            return m_sparse_pair_manager.tryGetExisting(m_slot_id);
        }
        if (m_page_num >= m_bucket_end_page_num) {
            initSparsePair(m_page_num);
        }
        return m_sparse_pair;
    }

    template <bool read_only>
    PlainSparsePair &SparsePairQuery<read_only>::currentOrCreateSparsePair()
    {
        if (!m_use_bucket_mapping) {
            m_slot_id = getMetaSlotId(m_page_num);
            m_slot_initialized = true;
            return m_sparse_pair_manager.getOrCreate(m_slot_id);
        }
        if (m_page_num >= m_bucket_end_page_num) {
            initOrCreateSparsePair(m_page_num);
        }
        assert(m_sparse_pair && "SparsePairQuery get-or-create lookup returned null");
        return *m_sparse_pair;
    }

    template <bool read_only>
    Allocator::SlotId SparsePairQuery<read_only>::getMetaSlotId(std::uint64_t page_num) const
    {
        auto address = page_num * static_cast<std::uint64_t>(m_page_size);
        return m_options.m_storage_slab_bucketing(address);
    }
    
    template <bool read_only>
    StorageOptions::StorageSlabBucket SparsePairQuery<read_only>::getBucket(std::uint64_t page_num) const
    {
        auto page_address = page_num * static_cast<std::uint64_t>(m_page_size);
        return m_options.m_storage_slab_bucket(page_address);
    }

    template <bool read_only>
    void SparsePairQuery<read_only>::setBucketEndPageNum(
        const StorageOptions::StorageSlabBucket &bucket, std::uint64_t page_num)
    {
        assert(page_num >= bucket.m_begin_page_num && "SparsePairQuery bucket does not cover begin page");
        assert(page_num < bucket.m_end_page_num && "SparsePairQuery bucket does not cover begin page");
        m_bucket_end_page_num = bucket.m_end_page_num;
    }

    template <bool read_only>
    void SparsePairQuery<read_only>::initSparsePair(std::uint64_t page_num)
    {
        auto bucket = getBucket(page_num);
        setBucketEndPageNum(bucket, page_num);
        m_slot_id = bucket.m_slot_id;
        m_slot_initialized = true;
        m_sparse_pair = m_sparse_pair_manager.tryGetExisting(m_slot_id);
    }

    template <bool read_only>
    void SparsePairQuery<read_only>::initOrCreateSparsePair(std::uint64_t page_num)
    {
        auto bucket = getBucket(page_num);
        setBucketEndPageNum(bucket, page_num);
        m_slot_id = bucket.m_slot_id;
        m_slot_initialized = true;
        m_sparse_pair = &m_sparse_pair_manager.getOrCreate(m_slot_id);
    }

    template class SparsePairQuery<true>;
    template class SparsePairQuery<false>;

}
