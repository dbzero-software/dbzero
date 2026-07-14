// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <functional>
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/core/memory/DP_Lock.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <dbzero/core/memory/DirtyCache.hpp>
#include <optional>

namespace db0

{

    class BaseStorage;
    
    /**
     * The in-memory only prefix implementation
     * access is limited to page or sub-page ranges
    */
    class DRAM_Prefix: public Prefix, public std::enable_shared_from_this<Prefix>
    {
    public:        
        // A function to consume a single resource (for serialization)
        using SinkFunction = DirtyCache::SinkFunction;
        
        // NOTE: page size for DRAM_Prefix may not be the power of 2
        DRAM_Prefix(std::size_t page_size);
        virtual ~DRAM_Prefix();

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;
        
        StateNumType getStateNum(bool finalized) const override;
        
        std::uint64_t commit(ProcessTimer * = nullptr) override;

        void close(ProcessTimer *timer_ptr = nullptr) override;
        
        std::size_t getDirtySize() const override;

        std::size_t flushDirty(std::size_t) override;

        std::size_t getPageSize() const;
        
        /**
         * Output all modified pages in to a user provided sink function
         * and mark pages as non-dirty
         * The flush order is undefined
        */
        void flushDirty(SinkFunction) const;
        
        /**
         * Set or update a single page
        */
        void *update(std::size_t page_num, bool mark_dirty = true);
        
        bool empty() const;
        
        /**
         * Copy all contents of another prefix to this one, dirty flag not set or updated
         * Existing pages not present in the other prefix will be removed
        */
        void operator=(const DRAM_Prefix &);

        AccessType getAccessType() const override {
            return AccessType::READ_WRITE;
        }

        std::uint64_t getLastUpdated() const override;
        
        std::shared_ptr<Prefix> getSnapshot(std::optional<StateNumType> state_num = {}) const override;
        
        BaseStorage &getStorage() const override;
        
        // Total number of bytes occupied by all pages        
        std::size_t size() const;

    private:        
        const std::size_t m_page_size;
        mutable Storage0 m_dev_null;
        mutable DirtyCache m_dirty_cache;
        StorageContext m_context;
#ifndef NDEBUG
        // cummulated size (in bytes) of all DRAM_Prefix instances
        static std::size_t dp_size;
        // total number of resource locks / data pages
        static std::size_t dp_count;
#endif        

        struct MemoryPage
        {
            mutable std::shared_ptr<DP_Lock> m_lock;
            void *m_buffer;
            
            MemoryPage(const MemoryPage &);
            MemoryPage(MemoryPage &&);
            MemoryPage(StorageContext, std::uint64_t address, std::size_t size);
#ifndef NDEBUG
            ~MemoryPage();
#endif                        

            void resetDirtyFlag();
        };

        mutable std::unordered_map<std::size_t, MemoryPage> m_pages;

    public:
#ifndef NDEBUG
        // get total memory usage across all instances of DRAM_Prefix
        static std::pair<std::size_t, std::size_t> getTotalMemoryUsage();

        const std::unordered_map<std::size_t, MemoryPage> &getPages() const {
            return m_pages;
        }
        
        // Calculate hash from the underlying pages
        std::size_t getContentHash() const;
#endif
    };
    
}