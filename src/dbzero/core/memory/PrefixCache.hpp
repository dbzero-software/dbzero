#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <set>
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
        std::shared_ptr<DP_Lock> findPage(std::uint64_t page_num, std::uint64_t state_num,
            FlagSet<AccessOptions>, std::uint64_t &read_state_num) const;
        
        std::shared_ptr<WideLock> findRange(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t state_num,
            FlagSet<AccessOptions>, std::uint64_t &read_state_num) const;
        
        std::shared_ptr<BoundaryLock> findBoundaryRange(std::uint64_t first_page_num, std::uint64_t address, std::size_t size,
            std::uint64_t state_num, FlagSet<AccessOptions>, std::uint64_t &read_state_num) const;
        
        /**
         * Create a new page associated resource lock
         * may be a new DP or existing with the storage but not available in cache         
        */
        std::shared_ptr<DP_Lock> createPage(std::uint64_t page_num, std::uint64_t read_state_num,
            std::uint64_t state_num, FlagSet<AccessOptions>);
        
        /**
         * Create a new wide range associated resource lock
         * @param size the lock size (must be > page size but may not be page aligned)
         * @param res_dp the residual lock (may be nullptr if the wide size lock is page aligned)
         */
        std::shared_ptr<WideLock> createRange(std::uint64_t page_num, std::size_t size, std::uint64_t read_state_num,
            std::uint64_t state_num, FlagSet<AccessOptions>, std::shared_ptr<DP_Lock> res_dp);
        
        /**
         * Insert copy of a single or wide page lock (not BoundaryLock)
         */
        std::shared_ptr<DP_Lock> insertCopy(const DP_Lock &, std::uint64_t write_state_num,
            FlagSet<AccessOptions> access_mode);

        std::shared_ptr<WideLock> insertWideCopy(const WideLock &, std::uint64_t write_state_num,
            FlagSet<AccessOptions> access_mode, std::shared_ptr<DP_Lock> res_lock);
        
        /**
         * Insert a copy of an existing BoundaryLock
         */
        std::shared_ptr<BoundaryLock> insertCopy(std::uint64_t address, std::size_t size, const BoundaryLock &, 
            std::shared_ptr<DP_Lock> lhs, std::shared_ptr<DP_Lock> rhs, std::uint64_t state_num, 
            FlagSet<AccessOptions> access_mode);
        
        /**
         * Mark specific page / range as NOT available in cache (missing)
         * this member is called when a data was mutated in a speciifc state number
        */
        void markAsMissing(std::uint64_t page_num, std::uint64_t state_num);

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
        void flush(ProcessTimer * = nullptr);

        // Flush managed boundary locks only
        void flushBoundary();

        /**
         * Relase / rollback all locks stored by the cache
         * this is required for a proper winding down in unit tests
         */
        void release();

        // Remove (discard) all volatile locks
        void rollback(std::uint64_t state_num);

        // Merge atomic operation's data (volatile locks) into an active transaction
        // @param from_state_num must be the atomic operation's assigned (temporary) state number
        // @param to_state_num the actual transaction number
        void merge(std::uint64_t from_state_num, std::uint64_t to_state_num);
        
        std::size_t getPageSize() const;
        
        CacheRecycler *getCacheRecycler() const;
        
        void clearExpired() const;

        std::size_t getDirtySize() const;

        std::size_t flushDirty(std::size_t limit);
        
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
        mutable std::vector<std::shared_ptr<BoundaryLock> > m_volatile_boundary_locks;

        /**
         * Execute specific function for each stored resource lock, boundary locks processed first
        */
        void forEach(std::function<void(ResourceLock &)>) const;
        
        void eraseRange(std::uint64_t address, std::size_t size, std::uint64_t state_num);
        void eraseBoundaryRange(std::uint64_t address, std::size_t size, std::uint64_t state_num);

        // insert new or replace existing range
        std::shared_ptr<DP_Lock> replaceRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
            std::shared_ptr<DP_Lock> new_lock);
        bool replaceBoundaryRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
            std::shared_ptr<BoundaryLock> new_lock);
    };

}