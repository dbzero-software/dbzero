#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <set>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/memory/BoundaryLock.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/storage/StorageView.hpp>
#include "PageMap.hpp"

namespace db0

{   
    
    class CacheRecycler;
    
    /**
     * Prefix deficated cache
    */
    class PrefixCache
    {
    public:
        /**
         * @param storage prefix related storage reference
        */
        PrefixCache(StorageView &, CacheRecycler *);

        /**
         * Attempt retrieving the range associated resource lock
         * 
         * @param state_id the state number
         * @param address startig address of the range
         * @param size the range size
         * @param result_state_num if not null, will be set to the closest matching state existing in cache
         * @return the resource lock or nullptr if not found
        */
        std::shared_ptr<ResourceLock> findRange(std::uint64_t address, std::uint64_t state_num, std::size_t size,
            std::uint64_t *result_state_num = nullptr) const;

        /**
         * Retrieve existing or create a new range associated resource lock
         * this method should be used for write-only access (data creation)
         *     
         * @param address startig address of the range
         * @param state_num the state number
         * @param size the range size
         * @return the resource lock (either existing or newly created)
        */
        std::shared_ptr<ResourceLock> createRange(std::uint64_t address, std::uint64_t state_num, std::size_t size,
            FlagSet<AccessOptions> access_mode);

        /**
         * Create lock object without adding it to cache
        */
        std::shared_ptr<ResourceLock> createLock(std::uint64_t address, std::size_t size,
            FlagSet<AccessOptions> access_mode) const;
        
        /**
         * Insert a copy of a specifc lock
        */
        std::shared_ptr<ResourceLock> insertCopy(std::uint64_t state_num, const ResourceLock &, std::uint64_t src_state_num,
            FlagSet<AccessOptions> access_mode);

        /**
         * Mark specific range as NOT available in cache (missing)
         * this member is called when a data was mutated in a speciifc state number
         * @param address startig address of the range
         * @param size the range size
         * @param state_num the state number
        */
        void markRangeAsMissing(std::uint64_t address, std::uint64_t state_num, std::size_t size);

        /**
         * Get total size of the managed resources (in bytes)
         * 
         * @return total size in bytes
        */
        std::size_t getSizeOfResources() const;
        
        bool empty() const;
        
        /**
         * Release existing resource lock
        */
        void release(std::uint64_t page_num);

        /**
         * Flush all managed locks
        */
        void flush();

        void clear();

        // Remove (discard) all volatile locks
        void rollback(std::uint64_t state_num);

        // Merge atomic operation's data (volatile locks) into an active transaction
        // @param from_state_num must be the atomic operation's assigned (temporary) state number
        // @param to_state_num the actual transaction number
        void merge(std::uint64_t from_state_num, std::uint64_t to_state_num);
        
    private:
        StorageView &m_storage_view;
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
        
        /**
         * Execute specific function for each stored resource lock, boundary locks processed first
        */
        void forEach(std::function<void(ResourceLock &)>) const;
        
        /**
         * Find or create page in a given state number (must be exact match)
        */
        std::shared_ptr<ResourceLock> findOrCreatePage(std::uint64_t state_num, std::uint64_t page_num,
            FlagSet<AccessOptions> access_mode);

        void eraseRange(std::uint64_t address, std::size_t size, std::uint64_t state_num);
        // insert new or replace existing range
        void replaceRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
            std::shared_ptr<ResourceLock> new_lock);
    };
    
}