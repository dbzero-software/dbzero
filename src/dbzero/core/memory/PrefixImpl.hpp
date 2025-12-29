// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <stdexcept>
#include <atomic>
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include "utils.hpp"
#include "config.hpp"
#include "PrefixViewImpl.hpp"
#include "PrefixCache.hpp"
    
namespace db0

{
    
    class CacheRecycler;
    
    /**
     * The default implementation of the Prefix interface
    */
    class PrefixImpl: public Prefix,
        public std::enable_shared_from_this<PrefixImpl>
    {
    public:
        /**
         * Opens an existing prefix from a specific storage
        */        
        PrefixImpl(std::string name, std::atomic<std::size_t> &dirty_meter, CacheRecycler *cache_recycler_ptr,
            std::shared_ptr<BaseStorage> storage);
        PrefixImpl(std::string name, std::atomic<std::size_t> &dirty_meter, CacheRecycler &cache_recycler, 
            std::shared_ptr<BaseStorage> storage);
        
        // NOTE: after calling mapRange for updates (i.e. with AccessOptions::write)
        // we always later need to call setDirty to specificy which micro-area is getting modified
        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;
        
        StateNumType getStateNum(bool finalized = false) const override;
        
        std::size_t getPageSize() const override {
            return m_page_size;
        }

        std::uint64_t commit(ProcessTimer * = nullptr) override;

        std::uint64_t getLastUpdated() const override;

        BaseStorage &getStorage() const override;

        std::size_t getDirtySize() const override;

        void getStats(std::function<void(const std::string &name, std::uint64_t value)>) const override;

        PrefixCache &getCache() const;

        /**
         * Release all owned locks from the cache recycler and clear the cache
         * this method should be called before closing the prefix to clean up used resources
         * Finally close the corresponding storage.
        */
        void close(ProcessTimer *timer_ptr = nullptr) override;
        
        bool beginRefresh() override;
        
        std::uint64_t completeRefresh() override;
        
        AccessType getAccessType() const override {
            return m_storage_ptr->getAccessType();
        }
        
        std::shared_ptr<Prefix> getSnapshot(std::optional<StateNumType> state_num = {}) const override;

        void beginAtomic() override;

        void endAtomic() override;

        void cancelAtomic() override;
        
        MemLock mapRange(std::uint64_t address, std::size_t size, StateNumType state_num,
            FlagSet<AccessOptions>);

        void cleanup() const override;
        
        std::size_t flushDirty(std::size_t limit) override;
                
    protected:
        std::shared_ptr<BaseStorage> m_storage;
        BaseStorage *m_storage_ptr;
        const AccessType m_access_type;
        const std::size_t m_page_size;
        const std::uint32_t m_shift;
        StateNumType m_head_state_num;
        mutable PrefixCache m_cache;
        // flag indicating atomic operation in progress
        bool m_atomic = false;
        
        std::shared_ptr<DP_Lock> mapPage(std::uint64_t page_num, StateNumType state_num, FlagSet<AccessOptions>);
        std::shared_ptr<BoundaryLock> mapBoundaryRange(std::uint64_t page_num, std::uint64_t address,
            std::size_t size, StateNumType state_num, FlagSet<AccessOptions>);
        std::shared_ptr<WideLock> mapWideRange(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t address, 
            std::size_t size, StateNumType state_num, FlagSet<AccessOptions>);

        inline bool isPageAligned(std::uint64_t addr_or_size) const {
            return (addr_or_size & (m_page_size - 1)) == 0;
        }
        
        void adjustAccessMode(FlagSet<AccessOptions> &access_mode, std::uint64_t address, std::size_t size) const;
    };
    
} 