// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <set>
#include "config.hpp"
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/memory/DP_Lock.hpp>
#include <dbzero/core/memory/BoundaryLock.hpp>
#include <dbzero/core/memory/WideLock.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include "PageMap.hpp"
#include "DirtyCache.hpp"

namespace db0

{   
    
    class CacheRecycler;
    class ProcessTimer;
    
    inline bool isBoundaryRange(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t addr_offset) {
        return (end_page == first_page + 2) && (addr_offset != 0);
    }
    
    /**
     * Prefix deficated cache
    */
    class PrefixCache
    {
    public:
        /**
         * @param storage prefix related storage reference         
        */
        PrefixCache(BaseStorage &, CacheRecycler *, std::atomic<std::size_t> *dirty_meter_ptr = nullptr);
                
        /**
         * Attempt retrieving the page / range associated existing resource lock for read or write
         * 
         * @param state_id the state number
         * @param address startig address of the range
         * @param size the range size
         * @param result_state_num if not null, will be set to the closest matching state existing in cache        
         * The "read_state_num" is the actual closest state in the storage (!)
         * @return the resource lock or nullptr if not found
        */
        std::shared_ptr<DP_Lock> findPage(std::uint64_t page_num, StateNumType state_num,
            FlagSet<AccessOptions>, StateNumType &read_state_num) const;
        
        // NOTE: address & size are required to restore lock if required
        // @param res_lock - optional residual lock required to restore the wide lock
        // @return lock exists flag / the actual lock (if lock exists but could not be retrieved then the operation needs 
        // to be repeated with passing the residual lock)
        std::pair<bool, std::shared_ptr<WideLock> > findRange(std::uint64_t first_page, std::uint64_t end_page,
            std::uint64_t address, std::size_t size, StateNumType state_num, FlagSet<AccessOptions>, 
            StateNumType &read_state_num, std::shared_ptr<DP_Lock> res_lock = {}) const;
        
        std::shared_ptr<BoundaryLock> findBoundaryRange(std::uint64_t first_page_num, std::uint64_t address, std::size_t size,
            StateNumType state_num, FlagSet<AccessOptions>, StateNumType &read_state_num, std::shared_ptr<DP_Lock> lhs = {},
            std::shared_ptr<DP_Lock> rhs = {}) const;
        
        /**
         * Create a new page associated resource lock
         * may be a new DP or existing with the storage but not available in cache         
         * @param cow_lock optional copy-on-write lock (previous version)
        */
        std::shared_ptr<DP_Lock> createPage(std::uint64_t page_num, StateNumType read_state_num,
            StateNumType state_num, FlagSet<AccessOptions>, std::shared_ptr<ResourceLock> cow_lock = nullptr);
        
        /**
         * Create a new wide range associated resource lock
         * @param size the lock size (must be > page size but may not be page aligned)
         * @param res_dp the residual lock (may be nullptr if the wide size lock is page aligned)
         */
        std::shared_ptr<WideLock> createRange(std::uint64_t page_num, std::size_t size, StateNumType read_state_num,
            StateNumType state_num, FlagSet<AccessOptions>, std::shared_ptr<DP_Lock> res_dp, 
            std::shared_ptr<ResourceLock> cow_lock = nullptr);
        
        /**
         * Insert copy of a single or wide page lock (not BoundaryLock)
         */
        std::shared_ptr<DP_Lock> insertCopy(std::shared_ptr<DP_Lock>, StateNumType write_state_num,
            FlagSet<AccessOptions> access_mode);
        
        std::shared_ptr<WideLock> insertWideCopy(std::shared_ptr<WideLock>, StateNumType write_state_num,
            FlagSet<AccessOptions> access_mode, std::shared_ptr<DP_Lock> res_lock);
        
        /**
         * Insert a copy of an existing BoundaryLock
         */
        std::shared_ptr<BoundaryLock> insertCopy(std::uint64_t address, std::size_t size, const BoundaryLock &, 
            std::shared_ptr<DP_Lock> lhs, std::shared_ptr<DP_Lock> rhs, StateNumType state_num,
            FlagSet<AccessOptions> access_mode);
        
        /**
         * Mark specific page / range as NOT available in cache (missing)
         * this member is called when a data was mutated in a speciifc state number
        */
        void markAsMissing(std::uint64_t page_num, StateNumType state_num);

        /**
         * Get total size of the managed resources (in bytes)
         * 
         * @return total size in bytes
        */
        std::size_t getSizeOfResources() const;
        
        // Remove cached locks
        void clear();
        
        bool empty() const;
        
        /**
         * Flush all managed locks
        */
        void commit(ProcessTimer * = nullptr);

        // Flush managed boundary locks only
        void flushBoundary();

        /**
         * Relase / rollback all locks stored by the cache
         * this is required for a proper winding down in unit tests
         */
        void release();
        
        // Remove (discard) all volatile locks
        void rollback(StateNumType state_num);

        // Merge atomic operation's data (volatile locks) into an active transaction
        // @param from_state_num must be the atomic operation's assigned (temporary) state number
        // @param to_state_num the actual transaction number
        // @param reused_locks buffer to hold reused volatile locks (this is because we need to update the locks with the CacheRecycler
        // which can only be done AFTER completing the atomic operation since it may trigger the unwanted updates - e.g. via PY_DECREF)
        void merge(StateNumType from_state_num, StateNumType to_state_num, 
            std::vector<std::shared_ptr<ResourceLock> > &reused_locks);
        
        std::size_t getPageSize() const;
        
        CacheRecycler *getCacheRecycler() const;
        
        void clearExpired(StateNumType head_state_num) const;

        std::size_t getDirtySize() const;

        std::size_t flushDirty(std::size_t limit);

        const PageMap<DP_Lock> &getDPMap() const;
        
        const PageMap<BoundaryLock> &getBoundaryMap() const;
        
        const PageMap<WideLock> &getWideMap() const;
        
        // prepare cache for the refresh operation
        void beginRefresh();

        // Scan available dirty locks and return:
        // total number of locks (DP count) / number of locks with CoW data present
        std::pair<std::uint64_t, std::uint64_t> getCoWStats() const;
                
    protected:
        const std::size_t m_page_size;
        const unsigned int m_shift;
        const std::uint64_t m_mask;
        BaseStorage &m_storage;
        // the collection for tracking dirty locks of each type (cleared on flush)
        mutable DirtyCache m_dirty_dp_cache;
        mutable DirtyCache m_dirty_wide_cache;
        StorageContext m_dp_context;
        StorageContext m_wide_context;
        // single data-page resource locks
        mutable PageMap<DP_Lock> m_dp_map;
        // boundary locks
        mutable PageMap<BoundaryLock> m_boundary_map;
        // wide locks
        mutable PageMap<WideLock> m_wide_map;
        // cache recycler keeps track of the accessed locks
        mutable CacheRecycler *m_cache_recycler_ptr = nullptr;
        // marker lock (to mark missing ranges)
        const std::shared_ptr<DP_Lock> m_missing_dp_lock_ptr;
        const std::shared_ptr<WideLock> m_missing_wide_lock_ptr;
        // locks (DP_Lock or WideLock) with no_flush flag (e.g. from an atomic update)
        mutable std::vector<std::shared_ptr<DP_Lock> > m_volatile_locks;
        mutable std::vector<std::shared_ptr<WideLock> > m_volatile_wide_locks;
        mutable std::vector<std::shared_ptr<BoundaryLock> > m_volatile_boundary_locks;

        /**
         * Execute specific function for each stored resource lock, boundary locks processed first
        */
        void forEach(std::function<void(ResourceLock &)>) const;
        
        void eraseRange(std::uint64_t address, std::size_t size, StateNumType state_num);
        void eraseBoundaryRange(std::uint64_t address, std::size_t size, StateNumType state_num);

        // insert new or replace existing range
        std::shared_ptr<DP_Lock> replaceRange(std::uint64_t address, std::size_t size, StateNumType state_num,
            std::shared_ptr<DP_Lock> new_lock);
        bool replaceBoundaryRange(std::uint64_t address, std::size_t size, StateNumType state_num,
            std::shared_ptr<BoundaryLock> new_lock);
        
        inline bool isPageAligned(std::uint64_t addr_or_size) const {
            return (addr_or_size & (m_page_size - 1)) == 0;
        }        
    };
    
    template <typename T> void discardAll(T &volatile_locks)
    {
        for (auto &lock: volatile_locks) {
            lock->discard();
        }
        volatile_locks.clear();
    }

}