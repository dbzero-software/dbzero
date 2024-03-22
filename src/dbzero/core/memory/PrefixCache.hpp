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

        void clear();

        bool empty() const;
        
        /**
         * Release existing resource lock
        */
        void release(std::uint64_t page_num);

        /**
         * Flush all managed locks
        */
        void flush();

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
        
        /**
         * Execute specific function for each stored resource lock, boundary locks processed first
        */
        void forEach(std::function<void(ResourceLock &)>) const;
    };
       
}