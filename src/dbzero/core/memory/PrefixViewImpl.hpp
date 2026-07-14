// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <cstdint>
#include "config.hpp"
#include "SnapshotCache.hpp"
#include "utils.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/storage/BaseStorage.hpp>
#include <dbzero/core/memory/Prefix.hpp>

namespace db0

{
    
    class PrefixViewImpl: public Prefix
    {
    public:
        /**
         * @param head_cache the head transaction's cache
         */
        PrefixViewImpl(const std::string &name, std::shared_ptr<BaseStorage> storage, const PrefixCache &head_cache,
            StateNumType state_num);
        
        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;
        
        StateNumType getStateNum(bool finalized) const override;
        
        std::size_t getPageSize() const override;

        std::uint64_t commit(ProcessTimer * = nullptr) override;

        std::uint64_t getLastUpdated() const override;

        void close(ProcessTimer *timer_ptr = nullptr) override;
        
        AccessType getAccessType() const override;

        std::shared_ptr<Prefix> getSnapshot(std::optional<StateNumType> state_num = {}) const override;

        BaseStorage &getStorage() const override;

        std::size_t getDirtySize() const override;

        std::size_t flushDirty(std::size_t limit) override;

    private:
        std::shared_ptr<BaseStorage> m_storage;
        BaseStorage *m_storage_ptr;
        const PrefixCache &m_head_cache;
        // snapshot's private cache instance
        mutable SnapshotCache m_cache;
        // immutable snapshot's state number
        const StateNumType m_state_num;
        const std::size_t m_page_size;
        const std::uint32_t m_shift;
        
        std::shared_ptr<DP_Lock> mapPage(std::uint64_t page_num);
        std::shared_ptr<BoundaryLock> mapBoundaryRange(std::uint64_t page_num, std::uint64_t address, std::size_t size);
        std::shared_ptr<WideLock> mapWideRange(std::uint64_t first_page, std::uint64_t end_page,
            std::uint64_t address, std::size_t size);
        
        inline bool isPageAligned(std::uint64_t addr_or_size) const {
            return (addr_or_size & (m_page_size - 1)) == 0;
        }
    };
    
}
