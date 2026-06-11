// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "SparsePairManager.hpp"
#include "StorageOptions.hpp"
#include <cstdint>

namespace db0

{

    // Retrieve the managed sparse pairs corresponding to the storage logical page numbers
    template <bool read_only>
    class SparsePairQuery
    {
    public:
        // NOTE: begin_page_num / end_page_num are logical main storage page numbers
        // the slot_num is assigned by bucketing them into slabs (slab ID -> slot num)
        SparsePairQuery(const StorageOptions &options, std::uint32_t page_size,
            std::uint64_t begin_page_num, std::uint64_t end_page_num,
            SparsePairManager &sparse_pair_manager);

        std::uint64_t pageNum() const
        {
            return m_page_num;
        }

        bool hasNext() const
        {
            return m_page_num < m_end_page_num;
        }

        Allocator::SlotId slotId() const;

        SparsePairQuery &operator++();

        PlainSparsePair *currentSparsePair();
        PlainSparsePair &currentOrCreateSparsePair();

    private:
        Allocator::SlotId getMetaSlotId(std::uint64_t page_num) const;
        StorageOptions::StorageSlabBucket getBucket(std::uint64_t page_num) const;
        void setBucketEndPageNum(const StorageOptions::StorageSlabBucket &bucket, std::uint64_t page_num);
        void initSparsePair(std::uint64_t page_num);
        void initOrCreateSparsePair(std::uint64_t page_num);

        const StorageOptions &m_options;
        const std::uint32_t m_page_size;
        std::uint64_t m_page_num;
        const std::uint64_t m_end_page_num;
        SparsePairManager &m_sparse_pair_manager;
        const bool m_use_bucket_mapping;
        Allocator::SlotId m_slot_id = 0;
        bool m_slot_initialized = false;
        std::uint64_t m_bucket_end_page_num = 0;
        PlainSparsePair *m_sparse_pair = nullptr;
    };

    extern template class SparsePairQuery<true>;
    extern template class SparsePairQuery<false>;

}
