// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>

namespace db0

{

    class Diff_IO;
    class SparsePair;

    class MetaPrefix: public DRAM_Prefix
    {
    public:
        MetaPrefix(std::size_t page_size, SparsePair &sparse_pair);

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;

        std::uint64_t commit(ProcessTimer * = nullptr) override;

        StateNumType getStateNum(bool finalized = false) const override;

        std::size_t getDirtySize() const override;

        using DRAM_Prefix::flushDirty;

        std::size_t flushDirty(std::size_t limit) override;

        std::uint64_t getLastUpdated() const override;

        void forAllocatedAddresses(DRAM_Allocator::AddressSinkFunction sink) const;

    protected:
        SparsePair &m_sparse_pair;

    private:
        StateNumType m_state_num = 0;
        std::uint64_t m_last_updated = 0;
        std::unordered_map<std::uint64_t, std::vector<std::byte> > m_previous_pages;

        bool readPage(Diff_IO &page_io, std::uint64_t page_num, StateNumType state_num, void *buffer) const;

        bool flushPage(Diff_IO &page_io, std::uint64_t page_num, const void *buffer, StateNumType state_num);

        std::uint64_t writeFullPage(Diff_IO &page_io, const void *buffer,
            std::uint64_t reusable_storage_page_num = 0);

        void publishCompactedState(StateNumType state_num);

        void capturePreviousPage(std::uint64_t page_num, const MemLock &lock);

        friend void load(MetaPrefix &prefix, Diff_IO &page_io);

        friend bool flush(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer);

        friend bool compact(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer);
    };

    void load(MetaPrefix &prefix, Diff_IO &page_io);

    bool flush(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer = nullptr);

    /**
     * Manually compact MetaSpace page storage.
     *
     * Stages the current head state of all persisted and dirty metadata pages
     * as full DPs at the next state number. Disk writes preserve storage pages
     * needed to read the current head state, prefer reusing stale full-DP pages
     * from the previous state when safe, and do not flush or clear the diff
     * stream. Obsolete diff storage must be reclaimed by a later external step
     * after the compacted head is durably published.
     *
     * @return true if a compacted state was published, false when there are no
     * metadata pages to compact.
     */
    bool compact(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer = nullptr);

}
