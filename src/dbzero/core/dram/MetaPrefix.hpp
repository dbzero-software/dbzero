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

    private:
        SparsePair &m_sparse_pair;
        StateNumType m_state_num = 0;
        std::uint64_t m_last_updated = 0;
        std::unordered_map<std::uint64_t, std::vector<std::byte> > m_previous_pages;

        bool readPage(Diff_IO &page_io, std::uint64_t page_num, StateNumType state_num, void *buffer) const;

        bool flushPage(Diff_IO &page_io, std::uint64_t page_num, const void *buffer, StateNumType state_num);

        void capturePreviousPage(std::uint64_t page_num, const MemLock &lock);

        friend void load(MetaPrefix &prefix, Diff_IO &page_io);

        friend bool flush(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer);
    };

    void load(MetaPrefix &prefix, Diff_IO &page_io);

    bool flush(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *timer = nullptr);

}
