// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/storage/SparsePairFwd.hpp>

namespace db0

{
    
    class Diff_IO;

    class MetaPrefix: public DRAM_Prefix
    {
    public:
        using DRAM_Prefix::flushDirty;
        
        /// @brief Create a MetaPrefix instance over the shared sparse mapping.
        /// @param page_size 
        /// @param sparse_pair maintains storage locations of the managed metadata pages
        MetaPrefix(std::size_t page_size, SparsePair &sparse_pair);

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;

        std::uint64_t commit(ProcessTimer * = nullptr) override;

        StateNumType getStateNum(bool finalized = false) const override;
                
        std::size_t flushDirty(std::size_t limit) override;

        void forAllocatedAddresses(std::function<void(std::uint64_t)> sink) const;

        // Get current head state number
        StateNumType getStateNum() const;

    protected:
        SparsePair &m_sparse_pair;

    private:
        std::unordered_map<std::uint64_t, std::vector<std::byte> > m_cow_pages;

        bool flushPage(Diff_IO &page_io, std::uint64_t page_num, const void *buffer, StateNumType state_num);

        std::uint64_t writeFullPage(Diff_IO &page_io, const void *buffer,
            std::uint64_t reusable_storage_page_num = 0);

        void publishCompactedState(StateNumType state_num);

        void captureCoWPage(std::uint64_t page_num, const MemLock &lock);

        friend void load(MetaPrefix &prefix, Diff_IO &page_io);
        friend bool fetchPage(MetaPrefix &prefix, Diff_IO &page_io, std::uint64_t page_num,
            StateNumType state_num, void *buffer);
        friend void load(MetaPrefix &prefix, Diff_IO &page_io, const std::vector<std::uint64_t> &page_nums);

        friend bool flush(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer);

        friend bool compact(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer);
    };

    // Load or refresh all pages from the current head state
    void load(MetaPrefix &, Diff_IO &);

    // Load or refresh specific pages from the current head state
    // this operation is optimized for large page batches
    // @param page_nums sorted page numbers to load
    void load(MetaPrefix &, Diff_IO &, const std::vector<std::uint64_t> &page_nums);

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
