#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <set>
#include <dbzero/core/memory/BaseLock.hpp>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/memory/BoundaryLock.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include "PageMap.hpp"

namespace db0

{   
    
    class CacheRecycler;
    
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
        PrefixCache(BaseStorage &, CacheRecycler *);
        
        /**
         * Attempt retrieving the range associated existing resource lock for read or write
         * 
         * @param state_id the state number
         * @param address startig address of the range
         * @param size the range size
         * @param result_state_num if not null, will be set to the closest matching state existing in cache        
         * The "read_state_num" is the actual closest state in the storage (!)
         * @return the resource lock or nullptr if not found
        */
        std::shared_ptr<ResourceLock> findRange(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t state_num,
            FlagSet<AccessOptions>, std::uint64_t &read_state_num, int &conflicts) const;
        
        std::shared_ptr<BoundaryLock> findBoundaryRange(std::uint64_t first_page_num, std::uint64_t address, std::size_t size,
            std::uint64_t state_num, FlagSet<AccessOptions>, std::uint64_t &read_state_num) const;
        
        /**
         * Retrieve existing or create a new range associated resource lock         
         *     
         * @param address startig address of the range
         * @param state_num the state number
         * @param size the range size
         * @return the resource lock (either existing or newly created)
        */
        std::shared_ptr<ResourceLock> createRange(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t read_state_num, 
            std::uint64_t state_num, FlagSet<AccessOptions>);
        
        /**
         * Insert copy of a single or wide page lock (not BoundaryLock)
         */
        std::shared_ptr<ResourceLock> insertCopy(const ResourceLock &, std::uint64_t write_state_num,
            FlagSet<AccessOptions> access_mode);

        /**
         * Insert a copy of an existing BoundaryLock
         */
        std::shared_ptr<BoundaryLock> insertCopy(std::uint64_t address, std::size_t size, const BoundaryLock &, 
            std::shared_ptr<ResourceLock> lhs, std::shared_ptr<ResourceLock> rhs, std::uint64_t state_num, 
            FlagSet<AccessOptions> access_mode);
        
        // This operation is required for the conflicting range resolution
        void insertUnique(std::shared_ptr<BoundaryLock>, std::uint64_t state_num);

        /**
         * Mark specific range as NOT available in cache (missing)
         * this member is called when a data was mutated in a speciifc state number
         * @param address startig address of the range
         * @param size the range size
         * @param state_num the state number
        */
        void markRangeAsMissing(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t state_num);

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
        void flush();

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

    protected:
        BaseStorage &m_storage;
        const std::size_t m_page_size;
        const unsigned int m_shift;
        const std::uint64_t m_mask;
        // boundary locks
        mutable PageMap<BoundaryLock> m_boundary_map;
        // regular resource locks (page-wise)
        mutable PageMap<ResourceLock> m_page_map;
        // cache recycler keeps track of the accessed locks
        mutable CacheRecycler *m_cache_recycler_ptr = nullptr;
        // marker lock (to mark missing ranges)
        const std::shared_ptr<ResourceLock> m_missing_lock_ptr;
        // locks with no_flush flag (e.g. from an atomic update)
        mutable std::vector<std::shared_ptr<ResourceLock> > m_volatile_locks;
        mutable std::vector<std::shared_ptr<BoundaryLock> > m_volatile_boundary_locks;
        
        /**
         * Execute specific function for each stored resource lock, boundary locks processed first
        */
        void forEach(std::function<void(BaseLock &)>) const;
        
        void eraseRange(std::uint64_t address, std::size_t size, std::uint64_t state_num);
        void eraseBoundaryRange(std::uint64_t address, std::size_t size, std::uint64_t state_num);

        // insert new or replace existing range
        void replaceRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
            std::shared_ptr<ResourceLock> new_lock);
        void replaceBoundaryRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
            std::shared_ptr<BoundaryLock> new_lock);
    };

}